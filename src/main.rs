mod api;
mod atproto;
mod ckb;
mod error;
mod indexer;
mod lexicon;
mod micro_pay;

#[macro_use]
extern crate tracing as logger;

use std::time::Duration;

use ckb_sdk::CkbRpcAsyncClient;
use clap::Parser;
use color_eyre::{Result, eyre::eyre};
use common_x::restful::axum::routing::get;
use common_x::restful::axum::{Router, routing::post};
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable};

use crate::api::ApiDoc;
use crate::lexicon::comment::Comment;
use crate::lexicon::like::Like;
use crate::lexicon::post::Post;
use crate::lexicon::reply::Reply;
use crate::lexicon::section::Section;
use crate::lexicon::status::Status;

#[derive(Clone)]
struct AppView {
    db: Pool<Postgres>,
    pds: String,
    ckb_client: CkbRpcAsyncClient,
    indexer: String,
    pay_url: String,
    bbs_ckb_addr: String,
    whitelist: Vec<String>,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version)]
pub struct Args {
    #[clap(short, long, default_value = "info")]
    log_filter: String,
    #[clap(long, default_value = "8080")]
    port: u16,
    #[clap(short, long)]
    db_url: String,
    #[clap(short, long)]
    pds: String,
    #[clap(short, long)]
    ckb_url: String,
    #[clap(short, long)]
    bbs_ckb_addr: String,
    #[clap(short, long)]
    pay_url: String,
    #[clap(short, long)]
    indexer: String,
    #[clap(short, long, default_value = "")]
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
    Comment::init(&db).await?;
    Reply::init(&db).await?;
    Like::init(&db).await?;

    let bbs = AppView {
        db,
        pds: args.pds.clone(),
        ckb_client: CkbRpcAsyncClient::new(&args.ckb_url),
        bbs_ckb_addr: args.bbs_ckb_addr.clone(),
        indexer: args.indexer.clone(),
        pay_url: args.pay_url.clone(),
        whitelist: args
            .whitelist
            .split(',')
            .filter_map(|s| {
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_owned())
                }
            })
            .collect(),
    };

    // api
    let router = Router::new()
        .merge(Scalar::with_url("/apidoc", ApiDoc::openapi()))
        .route("/api/admin/update_tag", post(api::admin::update_tag))
        .route("/api/record/create", post(api::record::create))
        .route("/api/record/update", post(api::record::update))
        .route("/api/section/list", get(api::section::list))
        .route("/api/section/detail", get(api::section::detail))
        .route("/api/post/list", post(api::post::list))
        .route("/api/post/top", post(api::post::top))
        .route("/api/post/detail", get(api::post::detail))
        .route("/api/post/commented", post(api::post::commented))
        .route("/api/comment/list", post(api::comment::list))
        .route("/api/reply/list", post(api::reply::list))
        .route("/api/repo/profile", get(api::repo::profile))
        .route("/api/repo/login_info", get(api::repo::login_info))
        .route("/api/like/list", post(api::like::list))
        .route("/api/tip/prepare", post(api::tip::prepare))
        .route("/api/tip/transfer", post(api::tip::transfer))
        .route("/api/tip/list", post(api::tip::list_by_for))
        .route("/api/tip/expense_details", post(api::tip::expense_details))
        .route("/api/tip/income_details", post(api::tip::income_details))
        .route("/api/donate/prepare", post(api::donate::prepare))
        .route("/api/donate/transfer", post(api::donate::transfer))
        .layer((TimeoutLayer::new(Duration::from_secs(10)),))
        .layer(CorsLayer::permissive())
        .with_state(bbs);
    common_x::restful::http_serve(args.port, router)
        .await
        .map_err(|e| eyre!("{e}"))
}
