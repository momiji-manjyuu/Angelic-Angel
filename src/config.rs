use crate::error::{Result, AngelicAngelError};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};
use uuid::Uuid;

const MAX_CONFIG_BYTES: u64 = 128 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub twitter: TwitterConfig,
    pub registration: Option<Registration>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct TwitterConfig {
    pub auth_token: String,
    pub ct0: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct WebPushKeys {
    pub public_key: Vec<u8>,
    pub private_key: Vec<u8>,
    pub auth_secret: Vec<u8>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct AutoPushSession {
    pub uaid: String,
    pub channel_id: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Registration {
    pub endpoint: String,
    pub autopush: AutoPushSession,
    pub keys: WebPushKeys,
}

// Debug remains usable by containing types without ever formatting credentials,
// push endpoints, session identifiers, or encryption material.
macro_rules! redacted_debug {
    ($($name:ty),+ $(,)?) => {
        $(impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "{} {{ [REDACTED] }}", stringify!($name))
            }
        })+
    };
}
redacted_debug!(Config, TwitterConfig, WebPushKeys, AutoPushSession, Registration);

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        validate_location(path)?;
        let before = validate_existing_file(path)?.ok_or_else(|| {
            config_error("config file does not exist")
        })?;
        let file = File::open(path).map_err(|_| config_error("failed to open config"))?;
        let opened = file.metadata().map_err(|_| config_error("failed to inspect config"))?;
        validate_private_file(&opened)?;
        if !same_file(&before, &opened) {
            return Err(config_error("config changed while opening; retry after checking its path"));
        }
        if opened.len() > MAX_CONFIG_BYTES {
            return Err(config_error("config exceeds the 128 KiB size limit"));
        }
        let mut content = String::new();
        // Recheck bytes actually read: a file may grow after the metadata check.
        file.take(MAX_CONFIG_BYTES + 1).read_to_string(&mut content)
            .map_err(|_| config_error("failed to read config"))?;
        if content.len() as u64 > MAX_CONFIG_BYTES {
            return Err(config_error("config exceeds the 128 KiB size limit"));
        }
        // TOML diagnostics can include the original line and thus the secret value.
        toml::from_str(&content).map_err(|_| config_error("invalid config TOML or field types"))
    }

    /// Atomically replace a private config in a trusted local directory.
    ///
    /// Existing directories and files are never chmodded. Unsafe permissions,
    /// symlinks, hard links, and non-regular files are rejected. Use one config
    /// writer, and a local Unix filesystem supporting rename and directory fsync.
    pub fn save(&self, path: &Path) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|_| config_error("failed to serialize config"))?;
        if content.len() as u64 > MAX_CONFIG_BYTES {
            return Err(config_error("config exceeds the 128 KiB size limit"));
        }
        let parent = validate_location(path)?;
        validate_existing_file(path)?;
        let directory = File::open(&parent)
            .map_err(|_| config_error("failed to open config directory"))?;
        let temp_path = parent.join(format!(".angelic-config-{}.tmp", Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        configure_private_creation(&mut options)?;
        let mut temp_file = options.open(&temp_path)
            .map_err(|_| config_error("failed to create private config temporary file"))?;
        let cleanup = TemporaryConfig(temp_path);
        temp_file.write_all(content.as_bytes())
            .map_err(|_| config_error("failed to write config temporary file"))?;
        temp_file.sync_all().map_err(|_| config_error("failed to sync config temporary file"))?;
        drop(temp_file);
        // Recheck before replacing anything. No existing file is modified in place.
        validate_location(path)?;
        validate_existing_file(path)?;
        fs::rename(&cleanup.0, path).map_err(|_| config_error("failed to atomically replace config"))?;
        directory.sync_all().map_err(|_| {
            config_error("config was replaced but directory sync failed; persistence is uncertain")
        })?;
        Ok(())
    }
}

fn config_error(message: &str) -> AngelicAngelError {
    AngelicAngelError::Config(message.to_string())
}

struct TemporaryConfig(PathBuf);

impl Drop for TemporaryConfig {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn validate_location(path: &Path) -> Result<PathBuf> {
    if path.file_name().is_none() {
        return Err(config_error("config path must name a file"));
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir().map_err(|_| config_error("failed to locate config directory"))?.join(path)
    };
    let parent = absolute.parent().ok_or_else(|| config_error("config path must have a parent"))?;
    let mut checked = PathBuf::new();
    for component in parent.components() {
        match component {
            Component::ParentDir => return Err(config_error("config path must not contain parent traversal")),
            Component::CurDir => continue,
            _ => checked.push(component.as_os_str()),
        }
        let metadata = fs::symlink_metadata(&checked)
            .map_err(|_| config_error("config directory is missing or inaccessible"))?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(config_error("config directory must not contain symlinks"));
        }
        validate_directory(&metadata, checked == parent)?;
    }
    Ok(parent.to_path_buf())
}

fn validate_existing_file(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            validate_private_file(&metadata)?;
            Ok(Some(metadata))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(_) => Err(config_error("failed to inspect config file")),
    }
}

#[cfg(unix)]
fn validate_private_file(metadata: &fs::Metadata) -> Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink()
        || metadata.permissions().mode() & 0o7077 != 0 || metadata.nlink() != 1 {
        return Err(config_error("config must be a regular, single-link owner-only file (mode 0600)"));
    }
    Ok(())
}

#[cfg(unix)]
fn validate_directory(metadata: &fs::Metadata, immediate_parent: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = metadata.permissions().mode();
    // A sticky ancestor such as /tmp is safe for a private child directory, but
    // never write secret config directly into that shared directory.
    if mode & 0o022 != 0 && (immediate_parent || mode & 0o1000 == 0) {
        return Err(config_error("config directory must not be writable by other users"));
    }
    Ok(())
}

#[cfg(unix)]
fn same_file(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(unix)]
fn configure_private_creation(options: &mut OpenOptions) -> Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
    Ok(())
}

// Portable std APIs cannot establish equivalent private ACLs on other platforms.
#[cfg(not(unix))]
fn validate_private_file(_: &fs::Metadata) -> Result<()> {
    Err(config_error("private config storage currently requires Unix"))
}
#[cfg(not(unix))]
fn validate_directory(_: &fs::Metadata, _: bool) -> Result<()> {
    Err(config_error("private config storage currently requires Unix"))
}
#[cfg(not(unix))]
fn same_file(_: &fs::Metadata, _: &fs::Metadata) -> bool { false }
#[cfg(not(unix))]
fn configure_private_creation(_: &mut OpenOptions) -> Result<()> {
    Err(config_error("private config storage currently requires Unix"))
}

/// Reads the webhook endpoint URL from the WEBHOOK_ENDPOINT environment variable.
pub fn get_webhook_endpoint() -> Result<String> {
    std::env::var("WEBHOOK_ENDPOINT").map_err(|_| {
        AngelicAngelError::Config("WEBHOOK_ENDPOINT environment variable is not set".to_string())
    })
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt, symlink};

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let root = fs::canonicalize(std::env::temp_dir()).unwrap();
            let path = root.join(format!("angelic-config-test-{}", Uuid::new_v4()));
            fs::DirBuilder::new().mode(0o700).create(&path).unwrap();
            Self(path)
        }

        fn config_path(&self) -> PathBuf { self.0.join("config.toml") }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
    }

    fn sample_config() -> Config {
        Config {
            twitter: TwitterConfig {
                auth_token: "synthetic-auth-token".to_string(),
                ct0: "synthetic-csrf-token".to_string(),
            },
            registration: Some(Registration {
                endpoint: "https://example.invalid/synthetic-private-endpoint".to_string(),
                autopush: AutoPushSession {
                    uaid: "synthetic-user-agent-id".to_string(),
                    channel_id: "synthetic-channel-id".to_string(),
                },
                keys: WebPushKeys {
                    public_key: vec![41, 42],
                    private_key: vec![91, 92],
                    auth_secret: vec![81, 82],
                },
            }),
        }
    }

    #[test]
    fn debug_never_formats_secret_fields() {
        let config = sample_config();
        let registration = config.registration.as_ref().unwrap();
        for debug in [
            format!("{:?}", config),
            format!("{:?}", config.twitter),
            format!("{:?}", registration),
            format!("{:?}", registration.keys),
            format!("{:?}", registration.autopush),
        ] {
            assert!(debug.contains("[REDACTED]"));
            assert!(!debug.contains("synthetic"));
            assert!(!debug.contains("[91, 92]"));
            assert!(!debug.contains("[81, 82]"));
        }
    }

    #[test]
    fn save_is_private_and_replaces_instead_of_truncating() {
        let directory = TestDirectory::new();
        let path = directory.config_path();
        let mut config = sample_config();
        config.save(&path).unwrap();
        let mut old_file = File::open(&path).unwrap();
        let old_inode = old_file.metadata().unwrap().ino();
        config.twitter.auth_token = "synthetic-replacement-token".to_string();
        config.save(&path).unwrap();
        assert_ne!(old_inode, fs::metadata(&path).unwrap().ino());
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(Config::load(&path).unwrap().twitter.auth_token, "synthetic-replacement-token");
        let mut old_content = String::new();
        old_file.read_to_string(&mut old_content).unwrap();
        assert!(old_content.contains("synthetic-auth-token"));
        assert!(!old_content.contains("synthetic-replacement-token"));
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
    }

    #[test]
    fn malformed_toml_error_does_not_echo_input() {
        let directory = TestDirectory::new();
        let path = directory.config_path();
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path).unwrap();
        file.write_all(b"[twitter]\nauth_token = \"synthetic-secret-must-not-appear\n").unwrap();
        drop(file);
        let error = Config::load(&path).unwrap_err();
        assert!(!error.to_string().contains("synthetic-secret"));
        assert!(!format!("{:?}", error).contains("synthetic-secret"));
    }

    #[test]
    fn listener_config_can_omit_twitter_cookies() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.twitter.auth_token.is_empty());
        assert!(config.twitter.ct0.is_empty());
        assert!(config.registration.is_none());
    }

    #[test]
    fn oversized_config_is_rejected_before_parsing_or_replacement() {
        let directory = TestDirectory::new();
        let path = directory.config_path();
        let file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&path).unwrap();
        file.set_len(MAX_CONFIG_BYTES + 1).unwrap();
        assert!(Config::load(&path).unwrap_err().to_string().contains("size limit"));
        let mut config = sample_config();
        config.twitter.auth_token = "x".repeat(MAX_CONFIG_BYTES as usize);
        assert!(config.save(&path).unwrap_err().to_string().contains("size limit"));
        assert_eq!(fs::metadata(&path).unwrap().len(), MAX_CONFIG_BYTES + 1);
    }

    #[test]
    fn unsafe_existing_permissions_are_rejected_without_chmod() {
        let directory = TestDirectory::new();
        let path = directory.config_path();
        sample_config().save(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        let before = fs::read(&path).unwrap();
        assert!(Config::load(&path).is_err());
        assert!(sample_config().save(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o644);
    }

    #[test]
    fn symlink_and_hardlink_configs_are_rejected() {
        let directory = TestDirectory::new();
        let target = directory.0.join("target.toml");
        sample_config().save(&target).unwrap();
        let before = fs::read(&target).unwrap();
        let link = directory.config_path();
        symlink(&target, &link).unwrap();
        assert!(Config::load(&link).is_err());
        assert!(sample_config().save(&link).is_err());
        assert_eq!(fs::read(&target).unwrap(), before);
        fs::remove_file(&link).unwrap();
        fs::hard_link(&target, &link).unwrap();
        assert!(Config::load(&link).is_err());
        assert!(sample_config().save(&link).is_err());
        assert_eq!(fs::read(&target).unwrap(), before);
    }

    #[test]
    fn unsafe_or_symlink_parent_is_rejected() {
        let directory = TestDirectory::new();
        let child = directory.0.join("child");
        fs::DirBuilder::new().mode(0o700).create(&child).unwrap();
        let link = directory.0.join("linked-child");
        symlink(&child, &link).unwrap();
        assert!(sample_config().save(&link.join("config.toml")).is_err());
        fs::set_permissions(&child, fs::Permissions::from_mode(0o770)).unwrap();
        assert!(sample_config().save(&child.join("config.toml")).is_err());
        assert_eq!(fs::read_dir(&child).unwrap().count(), 0);
        assert_eq!(fs::metadata(&child).unwrap().permissions().mode() & 0o777, 0o770);
    }
}
