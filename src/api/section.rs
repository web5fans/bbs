use color_eyre::eyre::{OptionExt, eyre};
use common_x::restful::{
    axum::{
        extract::{Query, State},
        response::IntoResponse,
    },
    ok,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde_json::{Value, json};
use sqlx::query_as_with;

use crate::{
    AppView,
    atproto::{NSID_PROFILE, get_record},
    error::AppError,
    lexicon::{
        post::Post,
        section::{Section, SectionRowSample, SectionView},
    },
};

pub(crate) async fn list(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let (sql, values) = sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Description,
            Section::Owner,
            Section::Administrators,
        ])
        .expr(Expr::cust("(select sum(\"post\".\"visited_count\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as visited_count"))
        .expr(Expr::cust("(select count(\"post\".\"uri\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as post_count"))
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"section_id\" = \"section\".\"id\") as reply_count"))
        .from(Section::Table)
        .and_where(
            if let Some(Some(repo)) = query.get("repo").map(|r| r.as_str()) {
                Expr::col((Section::Table, Section::Permission))
                    .eq(0)
                    .or(Expr::col((Section::Table, Section::Owner)).eq(repo))
                    .or(Expr::Custom(format!(
                        "'{repo}' = ANY(coalesce(section.administrators, array[]::text[]))"
                    )))
            } else {
                Expr::col((Section::Table, Section::Permission)).eq(0)
            },
        )
        .order_by(Section::Id, Order::Asc)
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let rows: Vec<SectionRowSample> = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    let mut views = vec![];
    for row in rows {
        let owner_author = if let Some(owner) = row.owner {
            // select post count
            let (sql, values) = sea_query::Query::select()
                .expr(Expr::col((Post::Table, Post::Uri)).count())
                .from(Post::Table)
                .and_where(Expr::col(Post::Repo).eq(&owner))
                .build_sqlx(PostgresQueryBuilder);
            debug!("post count exec sql: {sql}");
            let count_row: (i64,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            let mut identity = get_record(&state.pds, &owner, NSID_PROFILE, "self")
                .await
                .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
                .unwrap_or(json!({
                    "did": owner
                }));
            identity["did"] = Value::String(owner.clone());
            identity["post_count"] = Value::String(count_row.0.to_string());
            identity
        } else {
            json!({})
        };

        let mut administrators = vec![];

        if let Some(admins) = row.administrators {
            for admin in admins {
                // select post count
                let (sql, values) = sea_query::Query::select()
                    .expr(Expr::col((Post::Table, Post::Uri)).count())
                    .from(Post::Table)
                    .and_where(Expr::col(Post::Repo).eq(&admin))
                    .build_sqlx(PostgresQueryBuilder);
                debug!("post count exec sql: {sql}");
                let count_row: (i64,) = query_as_with(&sql, values.clone())
                    .fetch_one(&state.db)
                    .await
                    .map_err(|e| {
                        debug!("exec sql failed: {e}");
                        AppError::NotFound
                    })?;
                administrators.push({
                    let mut identity = get_record(&state.pds, &admin, NSID_PROFILE, "self")
                        .await
                        .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
                        .unwrap_or(json!({
                            "did": admin
                        }));
                    identity["did"] = Value::String(admin.clone());
                    identity["post_count"] = Value::String(count_row.0.to_string());
                    identity
                });
            }
        }
        views.push(SectionView {
            id: row.id.to_string(),
            name: row.name,
            description: row.description,
            owner: owner_author,
            administrators: Value::Array(administrators),
            visited_count: row.visited_count.to_string(),
            post_count: row.post_count.to_string(),
            reply_count: row.reply_count.to_string(),
        });
    }

    Ok(ok(views))
}

pub(crate) async fn detail(
    State(state): State<AppView>,
    Query(query): Query<Value>,
) -> Result<impl IntoResponse, AppError> {
    let id: i32 = query
        .get("id")
        .and_then(|id| id.as_str())
        .ok_or_eyre("id not be null")?
        .parse()?;

    let (sql, values) = sea_query::Query::select()
        .columns([
            Section::Id,
            Section::Name,
            Section::Description,
            Section::Owner,
            Section::Administrators,
        ])
        .expr(Expr::cust("(select sum(\"post\".\"visited_count\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as visited_count"))
        .expr(Expr::cust("(select count(\"post\".\"uri\") from \"post\" where \"post\".\"section_id\" = \"section\".\"id\") as post_count"))
        .expr(Expr::cust("(select count(\"reply\".\"uri\") from \"reply\" where \"reply\".\"section_id\" = \"section\".\"id\") as reply_count"))
        .from(Section::Table)
        .and_where(Expr::col(Section::Id).eq(id))
        .build_sqlx(PostgresQueryBuilder);

    debug!("sql: {sql}");

    let row: SectionRowSample = query_as_with::<_, SectionRowSample, _>(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    let owner_author = if let Some(owner) = row.owner {
        // select post count
        let (sql, values) = sea_query::Query::select()
            .expr(Expr::col((Post::Table, Post::Uri)).count())
            .from(Post::Table)
            .and_where(Expr::col(Post::Repo).eq(&owner))
            .build_sqlx(PostgresQueryBuilder);
        debug!("post count exec sql: {sql}");
        let count_row: (i64,) = query_as_with(&sql, values.clone())
            .fetch_one(&state.db)
            .await
            .map_err(|e| {
                debug!("exec sql failed: {e}");
                AppError::NotFound
            })?;
        let mut identity = get_record(&state.pds, &owner, NSID_PROFILE, "self")
            .await
            .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
            .unwrap_or(json!({
                "did": owner
            }));
        identity["did"] = Value::String(owner.clone());
        identity["post_count"] = Value::String(count_row.0.to_string());
        identity
    } else {
        json!({})
    };

    let mut administrators = vec![];

    if let Some(admins) = row.administrators {
        for admin in admins {
            administrators.push({
                // select post count
                let (sql, values) = sea_query::Query::select()
                    .expr(Expr::col((Post::Table, Post::Uri)).count())
                    .from(Post::Table)
                    .and_where(Expr::col(Post::Repo).eq(&admin))
                    .build_sqlx(PostgresQueryBuilder);
                debug!("post count exec sql: {sql}");
                let count_row: (i64,) = query_as_with(&sql, values.clone())
                    .fetch_one(&state.db)
                    .await
                    .map_err(|e| {
                        debug!("exec sql failed: {e}");
                        AppError::NotFound
                    })?;
                let mut identity = get_record(&state.pds, &admin, NSID_PROFILE, "self")
                    .await
                    .and_then(|row| row.get("value").cloned().ok_or_eyre("NOT_FOUND"))
                    .unwrap_or(json!({
                        "did": admin
                    }));
                identity["did"] = Value::String(admin.clone());
                identity["post_count"] = Value::String(count_row.0.to_string());
                identity
            });
        }
    }

    Ok(ok(SectionView {
        id: row.id.to_string(),
        name: row.name,
        description: row.description,
        owner: owner_author,
        administrators: Value::Array(administrators),
        visited_count: row.visited_count.to_string(),
        post_count: row.post_count.to_string(),
        reply_count: row.reply_count.to_string(),
    }))
}
