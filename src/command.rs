use ::clap::Parser;

#[derive(Debug, Parser)]
pub enum Command {
  /// Open a chat room for a topic and print a ticket for others to join
  Open,
  /// Join a chat room from a ticket
  Join {
    /// The ticket, as base32 string
    ticket: String,
  },
}
