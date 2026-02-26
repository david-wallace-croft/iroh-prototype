use ::anyhow::Result;
use ::data_encoding::BASE32_NOPAD;
use ::futures_lite::stream::StreamExt;
use ::iroh::protocol::Router;
use ::iroh::{Endpoint, PublicKey, SecretKey};
use ::iroh_gossip::net::{Event, Gossip, GossipEvent, GossipReceiver};
use ::iroh_gossip::proto::TopicId;
use ::rand::rngs::OsRng;
use ::std::str::FromStr;
use iroh::NodeAddr;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[::tokio::main]
async fn main() -> Result<()> {
  let secret_key: SecretKey = SecretKey::generate(OsRng);

  println!("> our secret key: {secret_key}");

  let endpoint: Endpoint = Endpoint::builder()
    .secret_key(secret_key)
    .discovery_n0()
    .bind()
    .await?;

  println!("> our node id: {}", endpoint.node_id());

  let gossip: Gossip = Gossip::builder().spawn(endpoint.clone()).await?;

  let router: Router = Router::builder(endpoint.clone())
    .accept(iroh_gossip::ALPN, gossip.clone())
    .spawn()
    .await?;

  let topic_id: TopicId = TopicId::from_bytes(rand::random());

  let peer_ids: Vec<PublicKey> = vec![];

  let (sender, receiver) = gossip.subscribe(topic_id, peer_ids)?.split();

  ::tokio::spawn(subscribe_loop(receiver));

  let ticket: Ticket = Ticket {
    peers: vec![],
    topic_id,
  };

  println!("> ticket to join us: {ticket}");

  sender.broadcast("sup".into()).await?;

  router.shutdown().await?;

  Ok(())
}

async fn subscribe_loop(mut receiver: GossipReceiver) -> Result<()> {
  while let Some(event) = receiver.try_next().await? {
    if let Event::Gossip(gossip_event) = event {
      match gossip_event {
        GossipEvent::Received(message) => {
          println!("got message: {:?}", &message)
        },
        _ => println!("got event: {:?}", &gossip_event),
      }
    }
  }

  Ok(())
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
