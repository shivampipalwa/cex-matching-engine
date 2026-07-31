//! Runs pending migrations and exits. A one-shot so `api` and `db_writer`
//! never race each other applying the same migration on startup.

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let pool = sqlx::postgres::PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    sqlx::migrate!("./migrations").run(&pool).await?;
    println!("Migrations up to date");
    Ok(())
}
