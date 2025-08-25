mod api;
mod atproto;
mod error;
mod lexicon;

#[macro_use]
extern crate tracing as logger;

use std::time::Duration;

use clap::Parser;
use color_eyre::{Result, eyre::eyre};
use common_x::restful::axum::routing::get;
use common_x::restful::axum::{Router, routing::post};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;

use crate::lexicon::post::Post;
use crate::lexicon::reply::Reply;
use crate::lexicon::section::Section;
use crate::lexicon::status::Status;

#[derive(Clone)]
struct AppView {
    db: Pool<Postgres>,
    pds: String,
    whitelist: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version)]
pub struct Args {
    #[clap(short, long, default_value = "info")]
    log_filter: String,
    #[clap(short, long)]
    db_url: String,
    #[clap(short, long)]
    pds: String,
    #[clap(short, long)]
    whitelist: String,
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
    Reply::init(&db).await?;

    let bbs = AppView {
        db,
        pds: args.pds.clone(),
        whitelist: args.whitelist.split(',').map(|s| s.to_string()).collect(),
    };

    // api
    let router = Router::new()
        .route("/api/record/create", post(api::record::create))
        .route("/api/section/list", get(api::section::list))
        .route("/api/section/detail", get(api::section::detail))
        .route("/api/post/list", post(api::post::list))
        .route("/api/post/top", post(api::post::top))
        .route("/api/post/detail", get(api::post::detail))
        .route("/api/post/replied", post(api::post::replied))
        .route("/api/reply/list", post(api::reply::list))
        .route("/api/repo/profile", get(api::repo::profile))
        .layer((TimeoutLayer::new(Duration::from_secs(10)),))
        .layer(CorsLayer::permissive())
        .with_state(bbs);
    common_x::restful::http_serve(8080, router)
        .await
        .map_err(|e| eyre!("{e}"))
}
