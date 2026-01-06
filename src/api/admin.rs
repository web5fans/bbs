use color_eyre::eyre::eyre;
use common_x::restful::{
    axum::{
        Json,
        extract::{Query, State},
        response::IntoResponse,
    },
    ok, ok_simple,
};
use sea_query::{Expr, ExprTrait, Order, PostgresQueryBuilder};
use sea_query_sqlx::SqlxBinder;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::query_as_with;
use utoipa::{IntoParams, ToSchema};
use validator::Validate;

use crate::{
    AppView,
    api::{SignedBody, SignedParam, build_author},
    atproto::{NSID_COMMENT, NSID_POST, NSID_REPLY},
    error::AppError,
    lexicon::{
        administrator::{Administrator, AdministratorView},
        comment::Comment,
        notify::{Notify, NotifyRow, NotifyType},
        operation::{Operation, OperationRow, OperationView},
        post::Post,
        reply::Reply,
        section::Section,
        whitelist::Whitelist,
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

            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "隐藏帖子".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
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

            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "取消隐藏".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }

        if let Some(true) = body.params.is_announcement {
            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "设置公告".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }
        if let Some(false) = body.params.is_announcement {
            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "下架公告".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }

        if let Some(true) = body.params.is_top {
            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "置顶帖子".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
                    created: chrono::Local::now(),
                },
            )
            .await
            .ok();
        }
        if let Some(false) = body.params.is_top {
            Operation::insert(
                &state.db,
                OperationRow {
                    id: 0,
                    operator: body.did.to_string(),
                    action: "取消置顶".to_string(),
                    message: body.params.uri.to_string(),
                    target: body.params.uri.to_string(),
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
    pub did: Option<String>,
    pub name: Option<String>,
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

    if let Some(did) = &body.params.did {
        let author = build_author(&state, did).await;
        if let Some(display_name) = author
            .get("displayName")
            .and_then(|n| n.as_str())
            .map(|n| n.to_lowercase())
        {
            if let Some(name) = body.params.name {
                if name.to_lowercase() != display_name {
                    return Err(AppError::ValidateFailed(
                        "display name not match".to_string(),
                    ));
                }
            } else {
                return Err(AppError::ValidateFailed("name is null".to_string()));
            }
        } else {
            return Err(AppError::ValidateFailed("did not found".to_string()));
        }
    }

    let (sql, values) = sea_query::Query::update()
        .table(Section::Table)
        .values([
            (Section::Owner, body.params.did.into()),
            (Section::OwnerSetTime, Expr::current_timestamp()),
        ])
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

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct WhitelistParams {
    pub whitelist: Vec<String>,
    pub timestamp: i64,
}

impl SignedParam for WhitelistParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/admin/add_whitelist")]
pub(crate) async fn add_whitelist(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<WhitelistParams>>,
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

    for did in &body.params.whitelist {
        Whitelist::insert(&state.db, did).await.ok();
    }

    Operation::insert(
        &state.db,
        OperationRow {
            id: 0,
            operator: body.did,
            action: "添加白名单".to_string(),
            message: json!(body.params.whitelist).to_string(),
            target: String::default(),
            created: chrono::Local::now(),
        },
    )
    .await
    .ok();

    Ok(ok_simple())
}

#[utoipa::path(post, path = "/api/admin/delete_whitelist")]
pub(crate) async fn delete_whitelist(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<WhitelistParams>>,
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

    for did in &body.params.whitelist {
        Whitelist::delete(&state.db, did).await.ok();
    }

    Operation::insert(
        &state.db,
        OperationRow {
            id: 0,
            operator: body.did,
            action: "删除白名单".to_string(),
            message: json!(body.params.whitelist).to_string(),
            target: String::default(),
            created: chrono::Local::now(),
        },
    )
    .await
    .ok();

    Ok(ok_simple())
}

#[utoipa::path(get, path = "/api/admin")]
pub(crate) async fn list(State(state): State<AppView>) -> Result<impl IntoResponse, AppError> {
    let rows = Administrator::all(&state.db).await;
    let mut views: Vec<AdministratorView> = vec![];

    for row in rows {
        let author = build_author(&state, &row.did).await;
        views.push(AdministratorView {
            did: author,
            permission: row.permission.to_string(),
            updated: row.updated,
            created: row.created,
        });
    }

    Ok(ok(views))
}

#[derive(Debug, Default, Validate, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub(crate) struct UpdateAdminParams {
    pub did: String,
    pub name: String,
    pub timestamp: i64,
}

impl SignedParam for UpdateAdminParams {
    fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[utoipa::path(post, path = "/api/admin/add")]
pub(crate) async fn add(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<UpdateAdminParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let (sql, values) = sea_query::Query::select()
        .column(Administrator::Did)
        .from(Administrator::Table)
        .and_where(Expr::col(Administrator::Permission).eq(0))
        .build_sqlx(PostgresQueryBuilder);
    let rows: Vec<(String,)> = sqlx::query_as_with(&sql, values)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let super_admins: Vec<String> = rows.into_iter().map(|r| r.0).collect();
    if !super_admins.contains(&body.did) {
        return Err(AppError::ValidateFailed(
            "only super administrator can add administrator".to_string(),
        ));
    }
    body.verify_signature(&state.indexer)
        .await
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    let author = build_author(&state, &body.params.did).await;
    if let Some(display_name) = author
        .get("displayName")
        .and_then(|n| n.as_str())
        .map(|n| n.to_lowercase())
    {
        if body.params.name.to_lowercase() != display_name {
            return Err(AppError::ValidateFailed(
                "display name not match".to_string(),
            ));
        }
    } else {
        return Err(AppError::ValidateFailed("did not found".to_string()));
    }

    Administrator::insert(&state.db, &body.params.did, 1).await?;

    Operation::insert(
        &state.db,
        OperationRow {
            id: 0,
            operator: body.did,
            action: "添加管理员".to_string(),
            message: author.to_string(),
            target: String::default(),
            created: chrono::Local::now(),
        },
    )
    .await
    .ok();

    Ok(ok_simple())
}

#[utoipa::path(post, path = "/api/admin/delete")]
pub(crate) async fn delete(
    State(state): State<AppView>,
    Json(body): Json<SignedBody<UpdateAdminParams>>,
) -> Result<impl IntoResponse, AppError> {
    body.validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let (sql, values) = sea_query::Query::select()
        .column(Administrator::Did)
        .from(Administrator::Table)
        .and_where(Expr::col(Administrator::Permission).eq(0))
        .build_sqlx(PostgresQueryBuilder);
    let rows: Vec<(String,)> = sqlx::query_as_with(&sql, values)
        .fetch_all(&state.db)
        .await
        .unwrap_or_default();
    let super_admins: Vec<String> = rows.into_iter().map(|r| r.0).collect();
    if !super_admins.contains(&body.did) {
        return Err(AppError::ValidateFailed(
            "only super administrator can add administrator".to_string(),
        ));
    }
    body.verify_signature(&state.indexer)
        .await
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;

    Administrator::delete(&state.db, &body.params.did).await?;

    let author = build_author(&state, &body.params.did).await;
    Operation::insert(
        &state.db,
        OperationRow {
            id: 0,
            operator: body.did,
            action: "删除管理员".to_string(),
            message: author.to_string(),
            target: String::default(),
            created: chrono::Local::now(),
        },
    )
    .await
    .ok();

    Ok(ok_simple())
}

#[derive(Debug, Validate, Deserialize, IntoParams)]
#[serde(default)]
pub struct OperationQuery {
    #[validate(range(min = 1))]
    pub page: u64,
    #[validate(range(min = 1))]
    pub per_page: u64,
}

impl Default for OperationQuery {
    fn default() -> Self {
        Self {
            page: 1,
            per_page: 20,
        }
    }
}

#[utoipa::path(get, path = "/api/admin/operations", params(OperationQuery))]
pub(crate) async fn operations(
    State(state): State<AppView>,
    Query(query): Query<OperationQuery>,
) -> Result<impl IntoResponse, AppError> {
    query
        .validate()
        .map_err(|e| AppError::ValidateFailed(e.to_string()))?;
    let offset = query.per_page * (query.page - 1);
    let (sql, values) = Operation::build_select()
        .order_by(Operation::Created, Order::Desc)
        .offset(offset)
        .limit(query.per_page)
        .build_sqlx(PostgresQueryBuilder);

    let rows: Vec<OperationRow> = query_as_with(&sql, values.clone())
        .fetch_all(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;
    let mut views: Vec<OperationView> = vec![];

    for row in rows {
        let operator = build_author(&state, &row.operator).await;
        views.push(OperationView {
            id: row.id.to_string(),
            operator,
            action: row.action.clone(),
            message: row.message.clone(),
            // TODO: target build
            target: row.target.clone().into(),
            created: row.created,
        });
    }

    let (sql, values) = sea_query::Query::select()
        .expr(Expr::col((Operation::Table, Operation::Id)).count_distinct())
        .from(Operation::Table)
        .build_sqlx(PostgresQueryBuilder);

    let total: (i64,) = query_as_with(&sql, values.clone())
        .fetch_one(&state.db)
        .await
        .map_err(|e| eyre!("exec sql failed: {e}"))?;

    Ok(ok(json!({
        "comments": views,
        "page": query.page,
        "per_page": query.per_page,
        "total":  total.0
    })))
}
