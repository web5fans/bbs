mod api;
mod atproto;
mod error;
mod lexicon;

#[macro_use]
extern crate tracing as logger;

use std::time::Duration;

use atrium_api::xrpc::http::Method;
use clap::Parser;
use color_eyre::{Result, eyre::eyre};
use common_x::restful::axum::routing::get;
use common_x::restful::axum::{Router, routing::post};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;

use crate::lexicon::post::Post;
use crate::lexicon::section::Section;
use crate::lexicon::status::Status;

#[derive(Clone)]
struct AppView {
    db: Pool<Postgres>,
    pds: String,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version)]
pub struct Args {
    #[clap(long, default_value = "info")]
    log_filter: String,
    #[clap(long)]
    db_url: String,
    #[clap(long)]
    pds: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    common_x::log::init_log_filter(&args.log_filter);
    info!("args: {:?}", args);
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&args.db_url)
        .await?;

    // initialize the database
    Status::init(&db).await?;
    Section::init(&db).await?;
    Post::init(&db).await?;

    let bbs = AppView {
        db,
        pds: args.pds.clone(),
    };

    // api
    let router = Router::new()
        .route("/api/section/list", get(api::section::list))
        .route("/api/record/create", post(api::record::create))
        .route("/api/post/list", post(api::post::list))
        .route("/api/post/detail", get(api::post::detail))
        .layer(CorsLayer::new().allow_methods([
            Method::GET,
            Method::POST,
            Method::DELETE,
            Method::PUT,
        ]))
        .layer((TimeoutLayer::new(Duration::from_secs(10)),))
        .with_state(bbs);
    common_x::restful::http_serve(8080, router)
        .await
        .map_err(|e| eyre!("{e}"))
}
