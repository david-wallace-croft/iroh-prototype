use super::message_body::MessageBody;
use ::anyhow::Result;
use ::bytes::Bytes;
use ::serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Message {
  pub body: MessageBody,
  pub nonce: [u8; 16],
}

impl Message {
  pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
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

impl From<Message> for Bytes {
  fn from(message: Message) -> Self {
    let message_vec: Vec<u8> = message.to_vec();

    message_vec.into()
  }
}
