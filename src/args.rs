use super::command::Command;
use ::clap::Parser;

/// Chat over iroh-gossip
///
/// This broadcasts messages over iroh-gossip
#[derive(Debug, Parser)]
pub struct Args {
  /// Disable relay completely
  #[clap(long)]
  pub no_relay: bool,
  /// Set your nickname
  #[clap(
    short, long
  )]
  pub name: Option<String>,
  #[clap(subcommand)]
  pub command: Command,
}
