use self::args::Args;
use self::command::Command;
use self::message::Message;
use self::message_body::MessageBody;
use self::ticket::Ticket;
use ::anyhow::Result;
use ::bytes::Bytes;
use ::clap::Parser;
use ::futures_lite::stream::StreamExt;
use ::iroh::protocol::Router;
use ::iroh::{Endpoint, NodeAddr, PublicKey, RelayMode, SecretKey};
use ::iroh_gossip::net::{Event, Gossip, GossipEvent, GossipReceiver};
use ::iroh_gossip::proto::TopicId;
use ::rand::rngs::OsRng;
use ::std::collections::HashMap;
use ::std::net::{Ipv4Addr, SocketAddrV4};
use ::std::str::FromStr;
use ::tokio::sync::mpsc::Sender;

mod args;
mod command;
mod message;
mod message_body;
mod ticket;

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
      let ticket = Ticket::from_str(ticket)?;

      let Ticket {
        topic_id,
        peers,
      } = ticket;

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

  let socket_addr_v4: SocketAddrV4 =
    SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0);

  let endpoint: Endpoint = Endpoint::builder()
    .secret_key(secret_key)
    .relay_mode(relay_mode)
    .bind_addr_v4(socket_addr_v4)
    // .discovery_n0()
    .bind()
    .await?;

  let endpoint_node_id: PublicKey = endpoint.node_id();

  println!("> our node id: {endpoint_node_id}");

  let gossip: Gossip = Gossip::builder().spawn(endpoint.clone()).await?;

  let ticket: Ticket = {
    let me: NodeAddr = endpoint.node_addr().await?;

    let peers: Vec<NodeAddr> = peers.iter().cloned().chain([me]).collect();

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

  let peer_ids: Vec<PublicKey> =
    peers.iter().map(|p: &NodeAddr| p.node_id).collect();

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
    let message_body: MessageBody = MessageBody::AboutMe {
      from: endpoint.node_id(),
      name,
    };

    let message: Message = Message::new(message_body);

    let message_bytes: Bytes = message.into();

    sender.broadcast(message_bytes).await?;
  }

  let subscribe_loop_future = subscribe_loop(receiver);

  ::tokio::spawn(subscribe_loop_future);

  let (line_tx, mut line_rx) = ::tokio::sync::mpsc::channel(1);

  ::std::thread::spawn(move || input_loop(line_tx));

  println!("> type a message and hit enter to broadcast...");

  while let Some(text) = line_rx.recv().await {
    let message_body: MessageBody = MessageBody::Message {
      from: endpoint.node_id(),
      text: text.clone(),
    };

    let message = Message::new(message_body);

    let message_bytes: Bytes = message.into();

    sender.broadcast(message_bytes).await?;

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
    let _ = stdin.read_line(&mut buffer);

    let _ = line_tx.blocking_send(buffer.clone());

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
          //   println!("got message: {:?}", &message);

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
