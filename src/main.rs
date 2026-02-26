use ::anyhow::Result;
use ::clap::Parser;
use ::data_encoding::BASE32_NOPAD;
use ::futures_lite::stream::StreamExt;
use ::iroh::protocol::Router;
use ::iroh::{Endpoint, NodeAddr, NodeId, PublicKey, RelayMode, SecretKey};
use ::iroh_gossip::net::{Event, Gossip, GossipEvent, GossipReceiver};
use ::iroh_gossip::proto::TopicId;
use ::rand::rngs::OsRng;
use ::serde::{Deserialize, Serialize};
use ::std::collections::HashMap;
use ::std::fmt::{Display, Formatter};
use ::std::net::{Ipv4Addr, SocketAddrV4};
use ::std::str::FromStr;
use tokio::sync::mpsc::Sender;

#[::tokio::main]
async fn main() -> Result<()> {
  let args: Args = Args::parse();

  let (topic_id, peers) = match &args.command {
    Command::Open => {
      let topic_id = TopicId::from_bytes(rand::random());

      println!("> opening chat room for topic {topic_id}");

      (topic_id, vec![])
    },
    Command::Join {
      ticket,
    } => {
      let Ticket {
        topic_id,
        peers,
      } = Ticket::from_str(ticket)?;

      println!("> joining chat room for topic {topic_id}");

      (topic_id, peers)
    },
  };

  let relay_mode: RelayMode = match args.no_relay {
    false => RelayMode::Default,
    true => RelayMode::Disabled,
  };

  let secret_key: SecretKey = SecretKey::generate(OsRng);

  println!("> our secret key: {secret_key}");

  let endpoint: Endpoint = Endpoint::builder()
    .secret_key(secret_key)
    .relay_mode(relay_mode)
    .bind_addr_v4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0))
    // .discovery_n0()
    .bind()
    .await?;

  println!("> our node id: {}", endpoint.node_id());

  let gossip: Gossip = Gossip::builder().spawn(endpoint.clone()).await?;

  let ticket: Ticket = {
    let me = endpoint.node_addr().await?;

    let peers = peers.iter().cloned().chain([me]).collect();

    Ticket {
      peers,
      topic_id,
    }
  };

  println!("> ticket to join us: {ticket}");

  let router: Router = Router::builder(endpoint.clone())
    .accept(iroh_gossip::ALPN, gossip.clone())
    .spawn()
    .await?;

  let peer_ids: Vec<PublicKey> = peers.iter().map(|p| p.node_id).collect();

  if peers.is_empty() {
    println!("> waiting for peers to join us...");
  } else {
    println!("> trying to connect to {} peers...", peers.len());

    for peer in peers.into_iter() {
      endpoint.add_node_addr(peer)?;
    }
  };

  //   let topic_id: TopicId = TopicId::from_bytes(rand::random());

  //   let peer_ids: Vec<PublicKey> = vec![];

  let (sender, receiver) =
    gossip.subscribe_and_join(topic_id, peer_ids).await?.split();

  println!("> connected");

  if let Some(name) = args.name {
    let message: Message = Message::new(MessageBody::AboutMe {
      from: endpoint.node_id(),
      name,
    });

    // let encoded_message = message.to_bytes();

    // sender.broadcast(encoded_message.into()).await?;
    sender.broadcast(message.to_vec().into()).await?;
  }

  ::tokio::spawn(subscribe_loop(receiver));

  let (line_tx, mut line_rx) = ::tokio::sync::mpsc::channel(1);

  ::std::thread::spawn(move || input_loop(line_tx));

  println!("> type a message and hit enter to broadcast...");

  while let Some(text) = line_rx.recv().await {
    let message = Message::new(MessageBody::Message {
      from: endpoint.node_id(),
      text: text.clone(),
    });

    sender.broadcast(message.to_vec().into()).await?;

    println!("> sent: {text}");
  }

  //   let ticket: Ticket = Ticket {
  //     peers: vec![],
  //     topic_id,
  //   };

  //   sender.broadcast("sup".into()).await?;

  router.shutdown().await?;

  Ok(())
}

fn input_loop(line_tx: Sender<String>) -> Result<()> {
  let mut buffer = String::new();

  let stdin = ::std::io::stdin();

  loop {
    stdin.read_line(&mut buffer);

    line_tx.blocking_send(buffer.clone());

    buffer.clear();
  }
}

async fn subscribe_loop(mut receiver: GossipReceiver) -> Result<()> {
  //   while let Some(event) = receiver.try_next().await? {
  //     if let Event::Gossip(gossip_event) = event {
  //       match gossip_event {
  //         GossipEvent::Received(message) => {
  //           println!("got message: {:?}", &message)
  //         },
  //         _ => println!("got event: {:?}", &gossip_event),
  //       }
  //     }
  //   }

  let mut names = HashMap::new();

  while let Some(event) = receiver.try_next().await? {
    if let Event::Gossip(gossip_event) = event {
      match gossip_event {
        GossipEvent::Received(message) => {
          println!("got message: {:?}", &message);

          match Message::from_bytes(&message.content)?.body {
            MessageBody::AboutMe {
              from,
              name,
            } => {
              names.insert(from, name.clone());

              println!("> {} is now known as {}", from.fmt_short(), name);
            },
            MessageBody::Message {
              from,
              text,
            } => {
              let name = names.get(&from).map_or_else(
                || from.fmt_short().to_string(),
                String::to_string,
              );

              println!("{}: {}", name, text);
            },
          }
        },
        _ => println!("got event: {:?}", &gossip_event),
      }
    }
  }

  Ok(())
}

/// Chat over iroh-gossip
///
/// This broadcasts messages over iroh-gossip
#[derive(Debug, Parser)]
struct Args {
  /// Disable relay completely
  #[clap(long)]
  no_relay: bool,
  /// Set your nickname
  #[clap(
    short, long
  )]
  name: Option<String>,
  #[clap(subcommand)]
  command: Command,
}

#[derive(Debug, Parser)]
enum Command {
  /// Open a chat room for a topic and print a ticket for others to join
  Open,
  /// Join a chat room from a ticket
  Join {
    /// The ticket, as base32 string
    ticket: String,
  },
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
  body: MessageBody,
  nonce: [u8; 16],
}

#[derive(Debug, Deserialize, Serialize)]
enum MessageBody {
  AboutMe {
    from: PublicKey,
    name: String,
  },
  Message {
    from: PublicKey,
    text: String,
  },
}

impl Message {
  fn from_bytes(bytes: &[u8]) -> Result<Self> {
    ::serde_json::from_slice(bytes).map_err(Into::into)
  }

  pub fn new(body: MessageBody) -> Self {
    Self {
      body,
      nonce: ::rand::random(),
    }
  }

  pub fn to_vec(&self) -> Vec<u8> {
    ::serde_json::to_vec(self).expect("::serde_json::to_vec is infallible")
  }
}

#[derive(Debug, Deserialize, Serialize)]
struct Ticket {
  peers: Vec<NodeAddr>,
  topic_id: TopicId,
}

impl Ticket {
  fn from_bytes(bytes: &[u8]) -> Result<Self> {
    ::serde_json::from_slice(bytes).map_err(Into::into)
  }

  fn to_bytes(&self) -> Vec<u8> {
    ::serde_json::to_vec(self).expect("::serde_json::to_vec is infallible")
  }
}

impl Display for Ticket {
  fn fmt(
    &self,
    f: &mut Formatter<'_>,
  ) -> std::fmt::Result {
    let mut text = BASE32_NOPAD.encode(&self.to_bytes()[..]);

    text.make_ascii_lowercase();

    write!(f, "{}", text)
  }
}

impl FromStr for Ticket {
  type Err = anyhow::Error;

  fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
    let bytes = BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())?;

    Self::from_bytes(&bytes)
  }
}
