use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "angelic-angel")]
#[command(
    about = "Twitter Web Push Receiver",
    long_about = "A server for receiving tweet notifications by emulating browser Web Push."
)]
pub struct Cli {
    /// Configuration file path
    #[arg(short, long, default_value = "angelic-angel.toml")]
    pub config: PathBuf,

    /// Enable verbose output (debug logs)
    #[arg(short, long)]
    pub verbose: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Initialize config interactively with hidden input (never pass cookies in argv)
    Init,
    /// Register AutoPush subscription and Twitter Push endpoint
    Register,
    /// Receive selected notification types into a durable outbox and deliver them
    Listen {
        /// Private local filesystem directory, dedicated to one webhook destination
        #[arg(long, default_value = "state/outbox")]
        outbox: PathBuf,
        /// JSON pointer to the type field, verified using a synthetic fixture
        #[arg(long)]
        type_pointer: String,
        /// Exact allowed type values; unknown or non-string types are discarded
        #[arg(long, required = true, value_delimiter = ',')]
        allow_type: Vec<String>,
    },
    /// Read durable queue health while the listener is stopped (never prints payloads)
    QueueStatus {
        #[arg(long, default_value = "state/outbox")]
        outbox: PathBuf,
    },
    /// Show current config and registration status
    Status,
    /// Unregister AutoPush subscription
    Unregister,
}
