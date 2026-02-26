use ::anyhow::Result;
use ::iroh::{Endpoint, SecretKey};
use ::rand::rngs::OsRng;

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

  Ok(())
}
