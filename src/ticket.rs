use ::anyhow::Result;
use ::data_encoding::BASE32_NOPAD;
use ::iroh::NodeAddr;
use ::iroh_gossip::proto::TopicId;
use ::serde::{Deserialize, Serialize};
use ::std::fmt::{Display, Formatter};
use ::std::str::FromStr;

#[derive(Debug, Deserialize, Serialize)]
pub struct Ticket {
  pub peers: Vec<NodeAddr>,
  pub topic_id: TopicId,
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
