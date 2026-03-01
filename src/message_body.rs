use ::iroh::NodeId;
use ::serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum MessageBody {
  AboutMe {
    from: NodeId,
    name: String,
  },
  Message {
    from: NodeId,
    text: String,
  },
}
