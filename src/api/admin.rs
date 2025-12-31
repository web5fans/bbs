use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{Json, extract::State, response::IntoResponse},
    ok_simple,
};
use sea_query::{Expr, ExprTrait, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use sqlx::query_as_with;
use utoipa::ToSchema;
use validator::Validate;

use crate::{
    AppView,
    api::{SignedBody, SignedParam},
    atproto::{NSID_COMMENT, NSID_POST, NSID_REPLY},
    error::AppError,
    lexicon::{
        administrator::Administrator,
        comment::Comment,
        notify::{Notify, NotifyRow, NotifyType},
        post::Post,
        reply::Reply,
        section::Section,
    },
};

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateTagParams {
    pub nsid: String,
    pub uri: String,
    pub is_top: Option<bool>,
    pub is_announcement: Option<bool>,
    pub is_disabled: Option<bool>,
    pub reasons_for_disabled: Option<String>,
    pub timestamp: i64,
}

impl SignedParam for UpdateTagParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/admin/update_tag")]
pub(crate) async fn update_tag(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<UpdateTagParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let section_id = match body.params.nsid.as_str() {
        NSID_POST => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Post::Table, Post::SectionId)])
                .from(Post::Table)
                .and_where(Expr::col(Post::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        NSID_REPLY => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Reply::Table, Reply::SectionId)])
                .from(Reply::Table)
                .and_where(Expr::col(Reply::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        NSID_COMMENT => {
            let (sql, values) = sea_query::Query::select()
                .columns([(Comment::Table, Comment::SectionId)])
                .from(Comment::Table)
                .and_where(Expr::col(Comment::Uri).eq(body.params.uri.clone()))
                .build_sqlx(PostgresQueryBuilder);
            let row: (i32,) = query_as_with(&sql, values.clone())
                .fetch_one(&state.db)
                .await
                .map_err(|e| {
                    debug!("exec sql failed: {e}");
                    AppError::NotFound
                })?;
            row.0
        }
        _ => return Err(eyre!("nsid is not allowed!").into()),
    };

    let section_row = Section::select_by_id(&state.db, section_id)
        .await
        .map_err(|e| {
            debug!("exec sql failed: {e}");
            AppError::NotFound
        })?;

    let admins = Administrator::all_did(&state.db).await;

    if section_row.owner == Some(body.did.clone()) || admins.contains(&body.did) {
        body.verify_signature(&state.indexer)
            .await
            .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
        match body.params.nsid.as_str() {
            NSID_POST => {
                Post::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_top,
                    body.params.is_announcement,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            NSID_REPLY => {
                Reply::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            NSID_COMMENT => {
                Comment::update_tag(
                    &state.db,
                    &body.params.uri,
                    body.params.is_disabled,
                    body.params.reasons_for_disabled,
                )
                .await?;
            }
            _ => return Err(eyre!("nsid is not allowed!").into()),
        }

        // notify
        if let Some(true) = body.params.is_disabled {
            let (receiver, _nsid, _rkey) = crate::lexicon::resolve_uri(&body.params.uri)?;
            Notify::insert(
                &state.db,
                &NotifyRow {
                    id: 0,
                    title: "Be Hidden".to_string(),
                    sender: body.did.to_string(),
                    receiver: receiver.to_string(),
                    n_type: NotifyType::BeHidden as i32,
                    target_uri: body.params.uri.to_string(),
                    amount: 0,
                    readed: None,
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }
        if let Some(false) = body.params.is_disabled {
            let (receiver, _nsid, _rkey) = crate::lexicon::resolve_uri(&body.params.uri)?;
            Notify::insert(
                &state.db,
                &NotifyRow {
                    id: 0,
                    title: "Be Displayed".to_string(),
                    sender: body.did.to_string(),
                    receiver: receiver.to_string(),
                    n_type: NotifyType::BeDisplayed as i32,
                    target_uri: body.params.uri.to_string(),
                    amount: 0,
                    readed: None,
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }
    } else {
        return Err(AppError::ValidateFailed(
            "only section administrator can update post tag".to_string(),
        ));
    }

    Ok(ok_simple())
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateOwnerParams {
    pub section: String,
    pub owner: Option<String>,
    pub timestamp: i64,
}

impl SignedParam for UpdateOwnerParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/admin/update_owner")]
pub(crate) async fn update_owner(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<UpdateOwnerParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let admins = Administrator::all_did(&state.db).await;
    if !admins.contains(&body.did) {
        return Err(AppError::ValidateFailed(
            "only administrator can update section owner".to_string(),
        ));
    }
    body.verify_signature(&state.indexer)
        .await
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let (sql, values) = sea_query::Query::update()
        .table(Section::Table)
        .value(Section::Owner, body.params.owner.clone())
        .and_where(Expr::col(Section::Id).eq(body.params.section.parse::<i32>()?))
        .build_sqlx(PostgresQueryBuilder);
    sqlx::query_with(&sql, values.clone())
        .execute(&state.db)
        .await?;

    Ok(ok_simple())
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateSectionParams {
    pub section: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub image: Option<String>,
    pub is_disabled: Option<bool>,
    pub timestamp: i64,
}

impl SignedParam for UpdateSectionParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/admin/update_section")]
pub(crate) async fn update_section(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<UpdateSectionParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let admins = Administrator::all_did(&state.db).await;
    if !admins.contains(&body.did) {
        return Err(AppError::ValidateFailed(
            "only administrator can update section owner".to_string(),
        ));
    }
    body.verify_signature(&state.indexer)
        .await
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    if let Some(is_disabled) = body.params.is_disabled {
        let (sql, values) = sea_query::Query::update()
            .table(Section::Table)
            .value(Section::IsDisabled, is_disabled)
            .and_where(Expr::col(Section::Id).eq(body.params.section.parse::<i32>()?))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values.clone())
            .execute(&state.db)
            .await?;
    }
    if let Some(name) = &body.params.name {
        let (sql, values) = sea_query::Query::update()
            .table(Section::Table)
            .value(Section::Name, name.clone())
            .and_where(Expr::col(Section::Id).eq(body.params.section.parse::<i32>()?))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values.clone())
            .execute(&state.db)
            .await?;
    }
    if let Some(description) = &body.params.description {
        let (sql, values) = sea_query::Query::update()
            .table(Section::Table)
            .value(Section::Description, description.clone())
            .and_where(Expr::col(Section::Id).eq(body.params.section.parse::<i32>()?))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values.clone())
            .execute(&state.db)
            .await?;
    }
    if let Some(image) = &body.params.image {
        let (sql, values) = sea_query::Query::update()
            .table(Section::Table)
            .value(Section::Image, image.clone())
            .and_where(Expr::col(Section::Id).eq(body.params.section.parse::<i32>()?))
            .build_sqlx(PostgresQueryBuilder);
        sqlx::query_with(&sql, values.clone())
            .execute(&state.db)
            .await?;
    }

    Ok(ok_simple())
}
