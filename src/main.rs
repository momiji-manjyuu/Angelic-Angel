mod autopush;
mod cli;
mod config;
mod delivery;
mod error;
mod filter;
mod listener;
mod outbox;
mod push;
mod twitter;

use std::path::PathBuf;

use clap::Parser;
use cli::{Cli, Commands};
use config::Config;
use console::style;
use dialoguer::Password;
use error::Result;
use indicatif::ProgressBar;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let filter = if cli.verbose {
        EnvFilter::new("off,angelic_angel=debug")
    } else {
        EnvFilter::new("off,angelic_angel=info")
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config_path = cli.config;

    let listener_handles_shutdown = matches!(&cli.command, Commands::Listen { .. });
    let task = async {
        match cli.command {
            Commands::Init => cmd_init(&config_path).await,
            Commands::Register => cmd_register(&config_path).await,
            Commands::Listen { outbox, type_pointer, allow_type } =>
                cmd_listen(&config_path, outbox, type_pointer, allow_type).await,
            Commands::QueueStatus { outbox } => cmd_queue_status(outbox).await,
            Commands::Status => cmd_status(&config_path).await,
            Commands::Unregister => cmd_unregister(&config_path).await,
        }
    };

    let result = if listener_handles_shutdown { task.await } else { tokio::select! {
        result = task => result,
        _ = tokio::signal::ctrl_c() => {
            eprintln!("\n{}", style("Interrupted").red().bold());
            return;
        }
    }};

    if let Err(e) = result {
        eprintln!("{} {}", style("error:").red().bold(), e);
        std::process::exit(1);
    }
}

fn spinner(msg: &str) -> ProgressBar {
    let sp = ProgressBar::new_spinner();
    sp.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    sp.enable_steady_tick(std::time::Duration::from_millis(80));
    sp.set_message(msg.to_string());
    sp
}

async fn spin<F, T>(msg: &str, done_msg: &str, fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>>,
{
    let sp = spinner(msg);
    match fut.await {
        Ok(v) => {
            sp.finish_with_message(format!("{} {}", style("done").green(), done_msg));
            Ok(v)
        }
        Err(e) => {
            sp.finish_with_message(format!("{} {}", style("fail").red(), msg));
            Err(e)
        }
    }
}

async fn cmd_init(config_path: &PathBuf) -> Result<()> {
    eprintln!("{}", style("Initializing configuration").bold());
    eprintln!();

    let auth_token = Password::new()
            .with_prompt("auth_token")
            .interact()
            .map_err(|_| error::AngelicAngelError::Config("credential input failed".into()))?;

    let ct0 = Password::new()
            .with_prompt("ct0")
            .interact()
            .map_err(|_| error::AngelicAngelError::Config("credential input failed".into()))?;

    let config = Config {
        twitter: config::TwitterConfig { auth_token, ct0 },
        registration: None,
    };

    config.save(config_path)?;
    eprintln!(
        "{} Saved to {}",
        style("done").green().bold(),
        style(config_path.display()).underlined()
    );

    Ok(())
}

async fn cmd_register(config_path: &PathBuf) -> Result<()> {
    eprintln!("{}", style("Registering push subscription").bold());
    eprintln!();

    let mut config = Config::load(config_path)?;
    // Check before creating any remote AutoPush registration.
    twitter::validate_credentials(&config.twitter)?;

    let subscription = spin(
        "Registering with AutoPush...",
        "AutoPush registered",
        push::subscribe(),
    )
    .await?;

    spin(
        "Registering with Twitter...",
        "Twitter Push registered",
        twitter::register(&config.twitter, &subscription),
    )
    .await?;

    config.registration = Some(config::Registration {
        endpoint: subscription.endpoint,
        autopush: subscription.autopush,
        keys: subscription.keys,
    });

    config.save(config_path)?;
    eprintln!();
    eprintln!(
        "{} Saved to {}",
        style("done").green().bold(),
        style(config_path.display()).underlined()
    );

    Ok(())
}

async fn cmd_listen(
    config_path: &PathBuf, outbox: PathBuf, type_pointer: String, allow_type: Vec<String>,
) -> Result<()> {
    let filter = filter::NotificationFilter::new(type_pointer, allow_type)?;
    let config = Config::load(config_path)?;
    let registration = config.registration.ok_or_else(|| {
        error::AngelicAngelError::Config(
            "No registration found. Run `register` first.".to_string(),
        )
    })?;

    // Cookies are not needed after explicit registration and are never sent here.
    listener::listen(registration, outbox, filter).await?;

    Ok(())
}

async fn cmd_queue_status(path: PathBuf) -> Result<()> {
    // Fails if an active process owns the queue; live health is logged every 30s.
    if !path.is_dir() {
        return Err(error::AngelicAngelError::Config("outbox directory does not exist".into()));
    }
    let snapshot = tokio::task::spawn_blocking(move || {
        outbox::Outbox::open(path).map(|queue| queue.snapshot())
    }).await.map_err(|_| error::AngelicAngelError::BackgroundTask)??;
    println!("{}", serde_json::to_string_pretty(&snapshot)?);
    Ok(())
}

async fn cmd_status(config_path: &PathBuf) -> Result<()> {
    eprintln!("{}", style("Status").bold());
    eprintln!();

    match Config::load(config_path) {
        Ok(config) => {
            eprintln!("{}  {}", style("Config").cyan().bold(), style(config_path.display()).dim());
            eprintln!(
                "  auth_token  {}",
                style(if config.twitter.auth_token.is_empty() { "unset" } else { "configured (redacted)" }).dim()
            );
            eprintln!(
                "  ct0         {}",
                style(if config.twitter.ct0.is_empty() { "unset" } else { "configured (redacted)" }).dim()
            );

            eprintln!();

            match config.registration {
                Some(_) => {
                    eprintln!("{}  {}", style("Registration").cyan().bold(), style("saved (not checked live)").green());
                    eprintln!("  endpoint / session / keys: configured (redacted)");
                }
                None => {
                    eprintln!(
                        "{}  {}",
                        style("Registration").cyan().bold(),
                        style("not registered").yellow()
                    );
                    eprintln!(
                        "  Run {} to register.",
                        style("angelic-angel register").bold()
                    );
                }
            }
        }
        Err(error) => return Err(error),
    }

    Ok(())
}

async fn cmd_unregister(config_path: &PathBuf) -> Result<()> {
    eprintln!("{}", style("Unregistering").bold());
    eprintln!();

    let mut config = Config::load(config_path)?;
    let reg = config.registration.as_ref().ok_or_else(|| {
        error::AngelicAngelError::Config("No registration found.".to_string())
    })?;

    spin(
        "Unregistering from AutoPush...",
        "AutoPush unregistered",
        autopush::unregister(&reg.autopush),
    )
    .await?;

    config.registration = None;
    config.save(config_path)?;

    eprintln!();
    eprintln!(
        "{} Registration removed from {}",
        style("done").green().bold(),
        style(config_path.display()).underlined()
    );

    Ok(())
}
