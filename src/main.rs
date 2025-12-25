mod api;
mod atproto;
mod ckb;
mod config;
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
use crate::config::AppConfig;
use crate::lexicon::comment::Comment;
use crate::lexicon::like::Like;
use crate::lexicon::notify::Notify;
use crate::lexicon::post::Post;
use crate::lexicon::reply::Reply;
use crate::lexicon::section::Section;
use crate::lexicon::status::Status;
use crate::lexicon::whitelist::Whitelist;

#[derive(Clone)]
struct AppView {
    db: Pool<Postgres>,
    pds: String,
    ckb_client: CkbRpcAsyncClient,
    indexer: String,
    pay_url: String,
    bbs_ckb_addr: String,
    ckb_net: ckb_sdk::NetworkType,
}

#[derive(Parser, Debug, Clone)]
#[command(author, version)]
pub struct Args {
    #[clap(short('c'), long = "config", default_value = "config.toml")]
    config_path: String,
    #[clap(short, long, default_value = "false")]
    apidoc: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config: AppConfig = common_x::configure::file_config(&args.config_path)?;

    common_x::log::init_log(config.log_config.clone());
    info!("config: {:?}", config);
    let db = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.db_url)
        .await?;

    // initialize the database
    Status::init(&db).await?;
    Section::init(&db).await?;
    Post::init(&db).await?;
    Comment::init(&db).await?;
    Reply::init(&db).await?;
    Like::init(&db).await?;
    Whitelist::init(&db).await?;
    Notify::init(&db).await?;

    let bbs = AppView {
        db,
        pds: config.pds.clone(),
        ckb_client: CkbRpcAsyncClient::new(&config.ckb_url),
        bbs_ckb_addr: config.bbs_ckb_addr.clone(),
        indexer: config.indexer.clone(),
        pay_url: config.pay_url.clone(),
        ckb_net: config.ckb_net,
    };

    let router = if args.apidoc {
        Router::new().merge(Scalar::with_url("/apidoc", ApiDoc::openapi()))
    } else {
        Router::new()
    };
    let router = router
        .route("/api/admin/update_tag", post(api::admin::update_tag))
        .route("/api/record/create", post(api::record::create))
        .route("/api/record/update", post(api::record::update))
        .route("/api/record/delete", post(api::record::delete))
        .route("/api/section/list", get(api::section::list))
        .route("/api/section/detail", get(api::section::detail))
        .route("/api/post/list", post(api::post::list))
        .route("/api/post/top", post(api::post::top))
        .route("/api/post/detail", get(api::post::detail))
        .route("/api/post/commented", post(api::post::commented))
        .route("/api/post/list_draft", post(api::post::list_draft))
        .route("/api/post/detail_draft", get(api::post::detail_draft))
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
        .route("/api/tip/stats", get(api::tip::stats))
        .route("/api/donate/prepare", post(api::donate::prepare))
        .route("/api/donate/transfer", post(api::donate::transfer))
        .route("/api/notify/list", post(api::notify::list))
        .route("/api/notify/read", post(api::notify::read))
        .route("/api/notify/unread_num", get(api::notify::unread_num))
        .layer((TimeoutLayer::with_status_code(
            reqwest::StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ),))
        .layer(CorsLayer::permissive())
        .with_state(bbs);
    common_x::restful::http_serve(config.port, router)
        .await
        .map_err(|e| eyre!("{e}"))
}
