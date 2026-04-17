use axum::{
    extract::{Json, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    net::SocketAddr,
    path::{Component, Path as StdPath, PathBuf},
    sync::Arc,
};
use tokio::net::TcpListener;
use tower_http::services::{ServeDir, ServeFile};

#[derive(Clone)]
struct AppState {
    src_root: Arc<PathBuf>,
    dst_root: Arc<PathBuf>,
    manifest_path: Arc<PathBuf>,
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = Json(serde_json::json!({
            "error": self.message,
        }));
        (self.status, body).into_response()
    }
}

impl From<std::io::Error> for ApiError {
    fn from(err: std::io::Error) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    }
}

#[derive(Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Serialize)]
struct StateResponse {
    src_root: String,
    dst_root: String,
    total_source_files: usize,
    total_assigned_files: usize,
    bundles: Vec<BundleResponse>,
}

#[derive(Serialize)]
struct BundleResponse {
    rel_path: String,
    file_count: usize,
    unassigned_count: usize,
    files: Vec<FileResponse>,
    guess_options: GuessResponse,
}

#[derive(Serialize)]
struct FileResponse {
    index: usize,
    rel_path: String,
    name: String,
    assigned: bool,
    assigned_to: Option<String>,
}

#[derive(Deserialize)]
struct GuessRequest {
    bundle_rel: String,
    sample_index: Option<usize>,
    series: Option<String>,
}

#[derive(Serialize, Clone)]
struct GuessResponse {
    author: Vec<String>,
    series: Vec<String>,
    volume: Vec<String>,
    book: Vec<String>,
}

#[derive(Deserialize)]
struct AssignRequest {
    bundle_rel: String,
    author: String,
    series: Option<String>,
    volume: Option<String>,
    book: String,
    narrator: Option<String>,
    file_indexes: Vec<usize>,
}

#[derive(Serialize)]
struct AssignResponse {
    destination_dir: String,
    linked_files: Vec<String>,
}

#[derive(Deserialize)]
struct UnassignRequest {
    source_files: Vec<String>,
}

#[derive(Serialize)]
struct UnassignResponse {
    removed: usize,
}

#[derive(Deserialize)]
struct RewriteRequest {
    source_files: Vec<String>,
    author: String,
    series: Option<String>,
    volume: Option<String>,
    book: String,
    narrator: Option<String>,
}

#[derive(Serialize)]
struct RewriteResponse {
    destination_dir: String,
    linked_files: Vec<String>,
}

#[derive(Serialize)]
struct VerifyResponse {
    ok: bool,
    total_source_files: usize,
    total_assigned_files: usize,
    missing_files: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ManifestFile {
    version: u32,
    books: Vec<ManifestBook>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestBook {
    id: String,
    bundle_rel: String,
    author: String,
    series: Option<String>,
    volume: Option<String>,
    book: String,
    narrator: Option<String>,
    source_files: Vec<String>,
}

#[derive(Debug, Clone)]
struct RenderedAssignment {
    source_rel: String,
    dest_rel: String,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("server error: {}", err.message);
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ApiError> {
    let testing = env::var("TESTING").unwrap_or_else(|_| "0".into()) == "1";

    let src_default = if testing {
        "./simulated-abb"
    } else {
        "/mnt/decypharr/completed/abb"
    };
    let dst_default = if testing {
        "./simulated-abb-sorted"
    } else {
        "/mnt/decypharr/completed/abb-sorted"
    };
    let src_root = normalize_abs_path(env::var("ABB_SRC").unwrap_or_else(|_| src_default.into()))?;
    let dst_root = normalize_abs_path(env::var("ABB_DST").unwrap_or_else(|_| dst_default.into()))?;
    let manifest_path = match env::var("ABB_MANIFEST") {
        Ok(value) => normalize_abs_path(value)?,
        Err(_) => dst_root.join("donotdelete.abb2abs.manifest.json"),
    };
    let bind = env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:3000".into());

    fs::create_dir_all(&dst_root)?;
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let state = AppState {
        src_root: Arc::new(src_root),
        dst_root: Arc::new(dst_root),
        manifest_path: Arc::new(manifest_path),
    };

    let app = Router::new()
        .route("/api/health", get(health))
        .route("/api/state", get(get_state))
        .route("/api/guess", post(post_guess))
        .route("/api/assign", post(post_assign))
        .route("/api/unassign", post(post_unassign))
        .route("/api/rewrite", post(post_rewrite))
        .route("/api/verify", get(get_verify))
        .fallback_service(
            ServeDir::new("static").not_found_service(ServeFile::new("static/index.html")),
        )
        .with_state(state.clone());

    let addr: SocketAddr = bind
        .parse()
        .map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, format!("invalid BIND_ADDR: {e}")))?;

    let listener = TcpListener::bind(addr).await.map_err(|e| {
        ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("bind failed: {e}"),
        )
    })?;

    println!("listening on http://{}", addr);
    println!("source={}", state.src_root.display());
    println!("dest={}", state.dst_root.display());
    println!("manifest={}", state.manifest_path.display());

    axum::serve(listener, app)
        .await
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn get_state(State(state): State<AppState>) -> Result<Json<StateResponse>, ApiError> {
    let snapshot = build_snapshot_real(&state)?;
    Ok(Json(snapshot))
}

async fn post_guess(
    State(state): State<AppState>,
    Json(req): Json<GuessRequest>,
) -> Result<Json<GuessResponse>, ApiError> {
    guess_real_fs(&state, req)
}

async fn post_assign(
    State(state): State<AppState>,
    Json(req): Json<AssignRequest>,
) -> Result<Json<AssignResponse>, ApiError> {
    assign_real_fs(&state, req)
}

async fn post_unassign(
    State(state): State<AppState>,
    Json(req): Json<UnassignRequest>,
) -> Result<Json<UnassignResponse>, ApiError> {
    unassign_real_fs(&state, req)
}

async fn post_rewrite(
    State(state): State<AppState>,
    Json(req): Json<RewriteRequest>,
) -> Result<Json<RewriteResponse>, ApiError> {
    rewrite_real_fs(&state, req)
}

async fn get_verify(State(state): State<AppState>) -> Result<Json<VerifyResponse>, ApiError> {
    verify_real_fs(&state)
}

fn build_snapshot_real(state: &AppState) -> Result<StateResponse, ApiError> {
    let source_files = collect_all_source_files(&state.src_root)?;
    let manifest = load_manifest(state.manifest_path.as_ref())?;
    let rendered = manifest_assignments(&manifest)?;
    let assignment_map: HashMap<String, String> = rendered
        .into_iter()
        .map(|a| (a.source_rel, a.dest_rel))
        .collect();
    let bundles = collect_top_level_items(&state.src_root)?;

    let mut bundle_responses = Vec::new();

    for bundle in bundles {
        let files = collect_bundle_files(&bundle)?;
        if files.is_empty() {
            continue;
        }

        let mut file_rows = Vec::new();
        let mut unassigned_count = 0usize;

        for (i, file) in files.iter().enumerate() {
            let file_rel = path_rel(&state.src_root, file);
            let assigned_to = assignment_map.get(&file_rel).cloned();
            let assigned = assigned_to.is_some();
            if !assigned {
                unassigned_count += 1;
            }
            file_rows.push(FileResponse {
                index: i + 1,
                rel_path: path_rel(&state.src_root, file),
                name: file
                    .file_name()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default()
                    .to_string(),
                assigned,
                assigned_to,
            });
        }

        let guess_options = GuessResponse {
            author: Vec::new(),
            series: Vec::new(),
            volume: Vec::new(),
            book: Vec::new(),
        };

        bundle_responses.push(BundleResponse {
            rel_path: path_rel(&state.src_root, &bundle),
            file_count: files.len(),
            unassigned_count,
            files: file_rows,
            guess_options,
        });
    }

    Ok(StateResponse {
        src_root: state.src_root.display().to_string(),
        dst_root: state.dst_root.display().to_string(),
        total_source_files: source_files.len(),
        total_assigned_files: assignment_map.len(),
        bundles: bundle_responses,
    })
}

fn guess_real_fs(state: &AppState, req: GuessRequest) -> Result<Json<GuessResponse>, ApiError> {
    let bundle_path = safe_join_under_root(&state.src_root, &req.bundle_rel)?;
    let bundle_files = collect_bundle_files(&bundle_path)?;
    if bundle_files.is_empty() {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "bundle has no files"));
    }

    let sample_index = req.sample_index.unwrap_or(1);
    if sample_index == 0 || sample_index > bundle_files.len() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("sample_index out of range: {}", sample_index),
        ));
    }

    let guesses = infer_guesses(
        state,
        &bundle_path,
        &bundle_files,
        &bundle_files[sample_index - 1],
        req.series.as_deref(),
    )?;
    Ok(Json(guesses))
}

fn assign_real_fs(state: &AppState, req: AssignRequest) -> Result<Json<AssignResponse>, ApiError> {
    let author = sanitize_component(&req.author);
    let series = req
        .series
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());
    let volume = req
        .volume
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());
    if volume.is_some() && series.is_none() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "volume requires a series",
        ));
    }
    let book = sanitize_component(&req.book);
    let narrator = req
        .narrator
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());

    if author.is_empty() || book.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "author and book are required",
        ));
    }

    if req.file_indexes.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "file_indexes must not be empty",
        ));
    }

    let bundle_path = safe_join_under_root(&state.src_root, &req.bundle_rel)?;
    let bundle_files = collect_bundle_files(&bundle_path)?;
    if bundle_files.is_empty() {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "bundle has no files"));
    }

    let mut source_files = Vec::new();
    let mut seen = HashSet::new();
    for raw_index in &req.file_indexes {
        if *raw_index == 0 || *raw_index > bundle_files.len() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("file index out of range: {}", raw_index),
            ));
        }
        let idx = *raw_index - 1;
        if !seen.insert(idx) {
            continue;
        }
        source_files.push(path_rel(state.src_root.as_ref(), &bundle_files[idx]));
    }

    if source_files.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "file_indexes must resolve to at least one file",
        ));
    }

    let mut manifest = load_manifest(state.manifest_path.as_ref())?;

    let assigned_sources: HashSet<String> = manifest
        .books
        .iter()
        .flat_map(|b| b.source_files.iter().cloned())
        .collect();

    for source_rel in &source_files {
        if assigned_sources.contains(source_rel) {
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                format!("source file already assigned: {}", source_rel),
            ));
        }
    }

    let new_book = ManifestBook {
        id: format!("book_{:08}", manifest.books.len() + 1),
        bundle_rel: req.bundle_rel.clone(),
        author: author.clone(),
        series: series.clone(),
        volume: volume.clone(),
        book: book.clone(),
        narrator: narrator.clone(),
        source_files: source_files.clone(),
    };

    let destination_dir = render_book_dest_dir(&new_book);
    let linked_files: Vec<String> = new_book
        .source_files
        .iter()
        .map(|source_rel| {
            let file_name = StdPath::new(source_rel)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            format!("{}/{}", destination_dir, file_name)
        })
        .collect();

    manifest.books.push(new_book);
    render_manifest_to_dst(state, &manifest)?;
    save_manifest(state.manifest_path.as_ref(), &manifest)?;

    Ok(Json(AssignResponse {
        destination_dir,
        linked_files,
    }))
}

fn unassign_real_fs(
    state: &AppState,
    req: UnassignRequest,
) -> Result<Json<UnassignResponse>, ApiError> {
    if req.source_files.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "source_files must not be empty",
        ));
    }

    let target_set: HashSet<String> = req.source_files.iter().cloned().collect();
    let mut manifest = load_manifest(state.manifest_path.as_ref())?;

    let before = manifest.books.len();
    manifest.books.retain(|book| {
        let current: HashSet<String> = book.source_files.iter().cloned().collect();
        current != target_set
    });

    let removed_books = before.saturating_sub(manifest.books.len());
    if removed_books == 0 {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            "book not found in manifest",
        ));
    }

    render_manifest_to_dst(state, &manifest)?;
    save_manifest(state.manifest_path.as_ref(), &manifest)?;

    Ok(Json(UnassignResponse {
        removed: removed_books,
    }))
}

fn rewrite_real_fs(
    state: &AppState,
    req: RewriteRequest,
) -> Result<Json<RewriteResponse>, ApiError> {
    let author = sanitize_component(&req.author);
    let series = req
        .series
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());
    let volume = req
        .volume
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());
    if volume.is_some() && series.is_none() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "volume requires a series",
        ));
    }
    let book_name = sanitize_component(&req.book);
    let narrator = req
        .narrator
        .as_deref()
        .map(sanitize_component)
        .filter(|s| !s.is_empty());

    if author.is_empty() || book_name.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "author and book are required",
        ));
    }

    if req.source_files.is_empty() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "source_files must not be empty",
        ));
    }

    let known_source_files: HashSet<String> = collect_all_source_files(state.src_root.as_ref())?
        .into_iter()
        .map(|p| path_rel(state.src_root.as_ref(), &p))
        .collect();

    for source_rel in &req.source_files {
        if !known_source_files.contains(source_rel) {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!("unknown source file: {}", source_rel),
            ));
        }
    }

    let mut manifest = load_manifest(state.manifest_path.as_ref())?;

    let target_set: HashSet<String> = req.source_files.iter().cloned().collect();

    let book = manifest
        .books
        .iter_mut()
        .find(|book| {
            let current: HashSet<String> = book.source_files.iter().cloned().collect();
            current == target_set
        })
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "book not found in manifest"))?;

    book.author = author;
    book.series = series;
    book.volume = volume;
    book.book = book_name;
    book.narrator = narrator;

    let destination_dir = render_book_dest_dir(book);
    let linked_files = book
        .source_files
        .iter()
        .map(|source_rel| {
            let file_name = StdPath::new(source_rel)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            format!("{}/{}", destination_dir, file_name)
        })
        .collect();

    render_manifest_to_dst(state, &manifest)?;
    save_manifest(state.manifest_path.as_ref(), &manifest)?;

    Ok(Json(RewriteResponse {
        destination_dir,
        linked_files,
    }))
}

fn verify_real_fs(state: &AppState) -> Result<Json<VerifyResponse>, ApiError> {
    let source_files = collect_all_source_files(&state.src_root)?;
    let source_set: HashSet<String> = source_files
        .iter()
        .map(|p| path_rel(state.src_root.as_ref(), p))
        .collect();
    let manifest = load_manifest(state.manifest_path.as_ref())?;
    let rendered = manifest_assignments(&manifest)?;
    let assigned_set: HashSet<String> = rendered.iter().map(|a| a.source_rel.clone()).collect();

    let actual = load_destination_assignments(
        state.src_root.as_ref(),
        state.dst_root.as_ref(),
        state.manifest_path.as_ref(),
    )?;
    let actual_map: HashMap<String, String> = actual
        .into_iter()
        .map(|(src, dst)| {
            (
                path_rel(state.src_root.as_ref(), &src),
                path_rel(state.dst_root.as_ref(), &dst),
            )
        })
        .collect();

    let desired_map: HashMap<String, String> = rendered
        .into_iter()
        .map(|a| (a.source_rel, a.dest_rel))
        .collect();

    let mut missing_files = Vec::new();
    for rel in &source_set {
        if !assigned_set.contains(rel) {
            missing_files.push(rel.clone());
        }
    }

    let render_ok = desired_map == actual_map;

    Ok(Json(VerifyResponse {
        ok: assigned_set.len() == source_set.len() && missing_files.is_empty() && render_ok,
        total_source_files: source_set.len(),
        total_assigned_files: assigned_set.len(),
        missing_files,
    }))
}

fn load_manifest(path: &StdPath) -> Result<ManifestFile, ApiError> {
    if !path.exists() {
        return Ok(ManifestFile {
            version: 1,
            books: Vec::new(),
        });
    }

    let text = fs::read_to_string(path)?;
    let manifest: ManifestFile = serde_json::from_str(&text)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(manifest)
}

fn save_manifest(path: &StdPath, manifest: &ManifestFile) -> Result<(), ApiError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let text = serde_json::to_string_pretty(manifest)
        .map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    fs::write(path, text)?;
    Ok(())
}

fn render_book_dest_dir(book: &ManifestBook) -> String {
    make_dest_dir_rel(
        &sanitize_component(&book.author),
        book.series.as_deref().map(sanitize_component).as_deref(),
        book.volume.as_deref().map(sanitize_component).as_deref(),
        &sanitize_component(&book.book),
        book.narrator.as_deref().map(sanitize_component).as_deref(),
    )
}

fn manifest_assignments(manifest: &ManifestFile) -> Result<Vec<RenderedAssignment>, ApiError> {
    let mut out = Vec::new();
    let mut seen_sources = HashSet::new();

    for book in &manifest.books {
        let dest_dir = render_book_dest_dir(book);

        for source_rel in &book.source_files {
            if !seen_sources.insert(source_rel.clone()) {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "source file assigned more than once in manifest: {}",
                        source_rel
                    ),
                ));
            }

            let file_name = StdPath::new(source_rel)
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "invalid source filename"))?;

            let dest_rel = format!("{}/{}", dest_dir, file_name);

            out.push(RenderedAssignment {
                source_rel: source_rel.clone(),
                dest_rel,
            });
        }
    }

    Ok(out)
}

fn render_manifest_to_dst(state: &AppState, manifest: &ManifestFile) -> Result<(), ApiError> {
    fs::create_dir_all(state.dst_root.as_ref())?;

    let desired = manifest_assignments(manifest)?;
    let desired_map: HashMap<String, String> = desired
        .iter()
        .map(|a| (a.source_rel.clone(), a.dest_rel.clone()))
        .collect();

    let actual = load_destination_assignments(
        state.src_root.as_ref(),
        state.dst_root.as_ref(),
        state.manifest_path.as_ref(),
    )?;
    let actual_map: HashMap<String, String> = actual
        .into_iter()
        .map(|(src, dst)| {
            (
                path_rel(state.src_root.as_ref(), &src),
                path_rel(state.dst_root.as_ref(), &dst),
            )
        })
        .collect();

    for (source_rel, actual_dest_rel) in &actual_map {
        match desired_map.get(source_rel) {
            Some(desired_dest_rel) if desired_dest_rel == actual_dest_rel => {}
            _ => {
                let full = state.dst_root.join(actual_dest_rel);
                if symlink_exists(&full) {
                    fs::remove_file(&full)?;
                }
            }
        }
    }

    for assignment in &desired {
        let src_full = state.src_root.join(&assignment.source_rel);
        let dst_full = state.dst_root.join(&assignment.dest_rel);

        if symlink_exists(&dst_full) {
            continue;
        }

        if let Some(parent) = dst_full.parent() {
            fs::create_dir_all(parent)?;
        }

        if !symlink_exists(&src_full) && !src_full.exists() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                format!(
                    "manifest references missing source file: {}",
                    src_full.display()
                ),
            ));
        }

        create_symlink(&src_full, &dst_full)?;
    }

    prune_empty_dirs(state.dst_root.as_ref())?;
    Ok(())
}

fn prune_empty_dirs(root: &StdPath) -> Result<(), ApiError> {
    let mut dirs = Vec::new();

    let mut stack = vec![root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_dir() && !meta.file_type().is_symlink() {
            dirs.push(path.clone());
            for entry in fs::read_dir(&path)? {
                stack.push(entry?.path());
            }
        }
    }

    dirs.sort_by_key(|p| std::cmp::Reverse(p.components().count()));

    for dir in dirs {
        if dir == root {
            continue;
        }
        if fs::read_dir(&dir)?.next().is_none() {
            fs::remove_dir(&dir)?;
        }
    }

    Ok(())
}

fn collect_top_level_items(root: &StdPath) -> Result<Vec<PathBuf>, ApiError> {
    let mut items = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        items.push(normalize_lexical_path(&entry.path()));
    }
    items.sort();
    Ok(items)
}

fn collect_all_source_files(root: &StdPath) -> Result<Vec<PathBuf>, ApiError> {
    collect_files_recursive(root)
}

fn collect_bundle_files(bundle: &StdPath) -> Result<Vec<PathBuf>, ApiError> {
    let meta = fs::symlink_metadata(bundle)?;
    if meta.file_type().is_dir() && !meta.file_type().is_symlink() {
        collect_files_recursive(bundle)
    } else {
        Ok(vec![bundle.to_path_buf()])
    }
}

fn collect_files_recursive(root: &StdPath) -> Result<Vec<PathBuf>, ApiError> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let meta = fs::symlink_metadata(&path)?;
        let ft = meta.file_type();

        if ft.is_dir() && !ft.is_symlink() {
            let mut entries = Vec::new();
            for entry in fs::read_dir(&path)? {
                entries.push(entry?.path());
            }
            entries.sort();
            entries.reverse();
            for child in entries {
                stack.push(child);
            }
        } else if ft.is_file() || ft.is_symlink() {
            if should_include_source_file(&path) {
                out.push(normalize_lexical_path(&path));
            }
        }
    }

    out.sort();
    Ok(out)
}

fn should_include_source_file(path: &StdPath) -> bool {
    let ext = path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.to_ascii_lowercase());

    !matches!(ext.as_deref(), Some("rar"))
}

fn load_destination_assignments(
    src_root: &StdPath,
    dst_root: &StdPath,
    manifest_path: &StdPath,
) -> Result<HashMap<PathBuf, PathBuf>, ApiError> {
    let source_files = collect_all_source_files(src_root)?;
    let source_set: HashSet<PathBuf> = source_files.into_iter().collect();
    let mut out = HashMap::new();

    let mut stack = vec![dst_root.to_path_buf()];
    while let Some(path) = stack.pop() {
        let meta = fs::symlink_metadata(&path)?;
        let ft = meta.file_type();

        if ft.is_dir() && !ft.is_symlink() {
            let mut entries = Vec::new();
            for entry in fs::read_dir(&path)? {
                entries.push(entry?.path());
            }
            entries.sort();
            entries.reverse();
            for child in entries {
                stack.push(child);
            }
            continue;
        }

        if ft.is_symlink() {
            let normalized_path = normalize_lexical_path(&path);
            let target = normalize_lexical_path(&fs::read_link(&path)?);
            if !target.is_absolute() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "destination symlink is relative: {} -> {}",
                        path.display(),
                        target.display()
                    ),
                ));
            }
            if !target.starts_with(src_root) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "destination symlink points outside source: {} -> {}",
                        path.display(),
                        target.display()
                    ),
                ));
            }
            if !source_set.contains(&target) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    format!(
                        "destination symlink points to unknown source file: {} -> {}",
                        path.display(),
                        target.display()
                    ),
                ));
            }
            if out.insert(target.clone(), normalized_path).is_some() {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    format!(
                        "duplicate source assignment detected for {}",
                        target.display()
                    ),
                ));
            }
            continue;
        }

        let normalized_path = normalize_lexical_path(&path);
        let normalized_manifest = normalize_lexical_path(manifest_path);

        if normalized_path == normalized_manifest {
            continue;
        }

        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!(
                "destination contains a non-symlink file: {}",
                path.display()
            ),
        ));
    }

    Ok(out)
}

fn safe_join_under_root(root: &StdPath, rel: &str) -> Result<PathBuf, ApiError> {
    let rel_path = PathBuf::from(rel);
    if rel_path.is_absolute() {
        return Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            "relative path expected",
        ));
    }

    for component in rel_path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "path contains invalid components",
                ))
            }
        }
    }

    Ok(root.join(rel_path))
}

fn normalize_abs_path(input: String) -> Result<PathBuf, ApiError> {
    let p = PathBuf::from(input);
    let abs = if p.is_absolute() {
        p
    } else {
        env::current_dir()?.join(p)
    };
    Ok(normalize_lexical_path(&abs))
}

fn normalize_lexical_path(path: &StdPath) -> PathBuf {
    let mut out = PathBuf::new();

    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }

    out
}

fn path_rel(root: &StdPath, path: &StdPath) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .to_string()
}

fn sanitize_component(value: &str) -> String {
    let trimmed = value.trim().replace('\n', " ").replace('/', " - ");
    collapse_spaces(&trimmed)
}

fn normalize_guess(value: &str) -> String {
    let value = sanitize_component(value).replace('_', " ");
    collapse_spaces(&value)
}

fn collapse_spaces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !prev_space {
                out.push(' ');
                prev_space = true;
            }
        } else {
            out.push(ch);
            prev_space = false;
        }
    }
    out.trim().to_string()
}

fn make_dest_dir_rel(
    author: &str,
    series: Option<&str>,
    volume: Option<&str>,
    book: &str,
    narrator: Option<&str>,
) -> String {
    let mut leaf = String::new();

    match volume.filter(|s| !s.is_empty()) {
        Some(volume) => {
            leaf.push_str(volume);
            leaf.push_str(". ");
            leaf.push_str(book);
        }
        None => {
            leaf.push_str(book);
        }
    }

    if let Some(n) = narrator.filter(|s| !s.is_empty()) {
        leaf.push_str(" {");
        leaf.push_str(n);
        leaf.push('}');
    }

    match series.filter(|s| !s.is_empty()) {
        Some(series) => format!("{}/{}/{}", author, series, leaf),
        None => format!("{}/{}", author, leaf),
    }
}

fn create_symlink(src: &StdPath, dst: &StdPath) -> Result<(), ApiError> {
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(src, dst)?;
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = src;
        let _ = dst;
        Err(ApiError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            "this example currently supports unix-style symlinks only",
        ))
    }
}

fn symlink_exists(path: &StdPath) -> bool {
    fs::symlink_metadata(path).is_ok()
}

fn infer_guesses(
    state: &AppState,
    bundle: &StdPath,
    bundle_files: &[PathBuf],
    sample_file: &StdPath,
    series_override: Option<&str>,
) -> Result<GuessResponse, ApiError> {
    let src_root = state.src_root.as_ref();
    let bundle_rel = path_rel(src_root, bundle);
    if bundle_files.is_empty() {
        return Ok(GuessResponse {
            author: Vec::new(),
            series: Vec::new(),
            volume: Vec::new(),
            book: Vec::new(),
        });
    }

    let manifest = load_manifest(state.manifest_path.as_ref())?;
    let history = load_bucket_history(&manifest, &bundle_rel);

    let bucket_name = normalize_guess(
        StdPath::new(&bundle_rel)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(&bundle_rel),
    );

    let bucket_pieces = bucket_piece_candidates(&bucket_name);

    let author = build_author_guesses(&bucket_pieces, &history);
    let series = build_series_guesses(&bucket_pieces, &history);

    let chosen_series = series_override
        .map(normalize_guess)
        .filter(|s| !s.is_empty())
        .or_else(|| series.first().cloned())
        .unwrap_or_default();

    let volume = build_volume_guesses(&history, &chosen_series);
    let book = build_book_guesses(&bucket_pieces, &bundle_files, sample_file);

    Ok(GuessResponse {
        author,
        series,
        volume,
        book,
    })
}

#[derive(Debug, Default)]
struct BucketHistory {
    author_counts: HashMap<String, usize>,
    series_counts: HashMap<String, usize>,
    used_volumes_by_series: HashMap<String, HashSet<u32>>,
}

#[derive(Debug)]
struct RankedGuess {
    value: String,
    score: i32,
}

fn load_bucket_history(manifest: &ManifestFile, bundle_rel: &str) -> BucketHistory {
    let mut out = BucketHistory::default();

    for book in manifest.books.iter().filter(|book| book.bundle_rel == bundle_rel) {
        let author = normalize_guess(&book.author);
        if !author.is_empty() {
            *out.author_counts.entry(author).or_insert(0) += 1;
        }

        let series = book
            .series
            .as_deref()
            .map(normalize_guess)
            .filter(|s| !s.is_empty());

        if let Some(series) = series {
            *out.series_counts.entry(series.clone()).or_insert(0) += 1;

            if let Some(volume_num) = book
                .volume
                .as_deref()
                .map(normalize_guess)
                .and_then(|v| v.parse::<u32>().ok())
                .filter(|n| *n >= 1)
            {
                out.used_volumes_by_series
                    .entry(series.to_lowercase())
                    .or_default()
                    .insert(volume_num);
            }
        }
    }

    out
}

fn build_author_guesses(bucket_pieces: &[String], history: &BucketHistory) -> Vec<String> {
    let mut scored = Vec::new();

    let mut historical: Vec<(String, usize)> = history
        .author_counts
        .iter()
        .map(|(value, count)| (value.clone(), *count))
        .collect();

    historical.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    for (author, count) in historical {
        push_guess(&mut scored, author, 1000 + (count as i32 * 100));
    }

    for piece in bucket_pieces {
        if looks_like_author_name(piece) {
            push_guess(&mut scored, piece.clone(), 300 + piece.len() as i32);
        }
    }

    finalize_ranked_guesses(scored)
}

fn build_series_guesses(bucket_pieces: &[String], history: &BucketHistory) -> Vec<String> {
    let mut scored = Vec::new();

    let mut historical: Vec<(String, usize)> = history
        .series_counts
        .iter()
        .map(|(value, count)| (value.clone(), *count))
        .collect();

    historical.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    for (series, count) in historical {
        push_guess(&mut scored, series, 1000 + (count as i32 * 100));
    }

    for piece in bucket_pieces {
        if looks_like_author_name(piece) && !looks_like_series_name(piece) {
            continue;
        }
        push_guess(&mut scored, piece.clone(), 250 + piece.len() as i32);
    }

    finalize_ranked_guesses(scored)
}

fn build_volume_guesses(history: &BucketHistory, series: &str) -> Vec<String> {
    let series_key = normalize_guess(series).to_lowercase();
    if series_key.is_empty() {
        return Vec::new();
    }

    let used = history
        .used_volumes_by_series
        .get(&series_key)
        .cloned()
        .unwrap_or_default();

    let max_used = used.iter().copied().max().unwrap_or(0);
    let search_until = std::cmp::max(max_used + 10, 10);

    let mut out = Vec::new();
    for n in 1..=search_until {
        if !used.contains(&n) {
            out.push(n.to_string());
        }
    }

    out
}

fn build_book_guesses(
    bucket_pieces: &[String],
    bundle_files: &[PathBuf],
    sample_file: &StdPath,
) -> Vec<String> {
    let mut scored = Vec::new();

    let cleaned_file_titles: Vec<String> = bundle_files
        .iter()
        .map(|path| {
            let file_name = path
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            clean_file_title_for_book_guess(file_name)
        })
        .collect();

    let mut file_candidate_counts: HashMap<String, (String, usize)> = HashMap::new();

    for path in bundle_files {
        let file_name = path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default();

        let mut seen_in_this_file = HashSet::new();
        for candidate in book_piece_candidates_from_file_name(file_name) {
            let key = candidate.to_lowercase();
            if !seen_in_this_file.insert(key.clone()) {
                continue;
            }

            let entry = file_candidate_counts
                .entry(key)
                .or_insert((candidate.clone(), 0));
            entry.1 += 1;
        }
    }

    let sample_file_name = sample_file
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();

    let sample_candidate_keys: HashSet<String> =
        book_piece_candidates_from_file_name(sample_file_name)
            .into_iter()
            .map(|s| s.to_lowercase())
            .collect();

    let mut file_candidates: Vec<(String, usize)> = file_candidate_counts.into_values().collect();

    file_candidates.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| b.0.len().cmp(&a.0.len()))
            .then_with(|| a.0.cmp(&b.0))
    });

    for (candidate, count) in file_candidates {
        let mut score = 600 + (count as i32 * 80) + candidate.len() as i32;
        if sample_candidate_keys.contains(&candidate.to_lowercase()) {
            score += 40;
        }
        push_guess(&mut scored, candidate, score);
    }

    for piece in bucket_pieces {
        if looks_like_author_name(piece) {
            continue;
        }

        let piece_lower = piece.to_lowercase();
        let freq = cleaned_file_titles
            .iter()
            .filter(|title| title.to_lowercase().contains(&piece_lower))
            .count();

        let score = 250 + (freq as i32 * 60) + piece.len() as i32;
        push_guess(&mut scored, piece.clone(), score);
    }

    finalize_ranked_guesses(scored)
}

fn bucket_piece_candidates(bucket_name: &str) -> Vec<String> {
    let mut out = Vec::new();

    let mut cleaned = normalize_guess(bucket_name);

    let read_by_re = Regex::new(r"(?i)\s*-\s*read by\s+.+$").unwrap();
    cleaned = read_by_re.replace(&cleaned, "").to_string();

    let version_re = Regex::new(r"(?i)\s*-\s*v\d+$").unwrap();
    cleaned = version_re.replace(&cleaned, "").to_string();

    let cleaned = collapse_spaces(&cleaned);

    push_phrase(&mut out, remove_common_metadata(&cleaned));

    if let Some((left, right)) = cleaned.rsplit_once(" by ") {
        push_phrase(&mut out, remove_common_metadata(left));
        push_phrase(&mut out, remove_common_metadata(right));
    }

    for part in cleaned.split(" - ") {
        push_phrase(&mut out, remove_common_metadata(part));
    }

    let paren_re = Regex::new(r"^(?P<left>.+?)\s*\((?P<inner>[^()]+)\)$").unwrap();
    if let Some(caps) = paren_re.captures(&cleaned) {
        push_phrase(
            &mut out,
            remove_common_metadata(caps.name("left").map(|m| m.as_str()).unwrap_or_default()),
        );
        push_phrase(
            &mut out,
            remove_common_metadata(caps.name("inner").map(|m| m.as_str()).unwrap_or_default()),
        );
    }

    for suffix in [
        " Books",
        " Series",
        " Saga",
        " Trilogy",
        " Chronicles",
        " Cycle",
        " Novels",
    ] {
        if let Some(stripped) = cleaned.strip_suffix(suffix) {
            push_phrase(&mut out, remove_common_metadata(stripped));
        }
    }

    dedupe_preserve_order(out)
}

fn book_piece_candidates_from_file_name(file_name: &str) -> Vec<String> {
    let cleaned = clean_file_title_for_book_guess(file_name);
    let mut out = Vec::new();

    push_phrase(&mut out, cleaned.clone());

    for part in cleaned.split(" - ") {
        push_phrase(&mut out, part);
    }

    for part in cleaned.split(':') {
        push_phrase(&mut out, part);
    }

    for part in cleaned.split(" / ") {
        push_phrase(&mut out, part);
    }

    dedupe_preserve_order(out)
}

fn clean_file_title_for_book_guess(file_name: &str) -> String {
    let mut s = normalize_guess(&strip_extension(file_name));

    if let Some((left, _)) = s.rsplit_once(" by ") {
        s = left.to_string();
    } else if let Some((left, right)) = s.split_once(" - ") {
        if looks_like_author_name(left) {
            s = right.to_string();
        } else if left.chars().all(|c| c.is_ascii_digit() || c == ' ') {
            s = right.to_string();
        }
    }

    s = strip_trailing_track_noise(&s);

    let bracket_re = Regex::new(r"\[[^\[\]]*\]").unwrap();
    s = bracket_re.replace_all(&s, " ").to_string();

    let paren_re = Regex::new(r"\([^)]*\)").unwrap();
    s = paren_re.replace_all(&s, " ").to_string();

    let book_num_re = Regex::new(r",?\s+[Bb]ook\s+\d+$").unwrap();
    s = book_num_re.replace(&s, "").to_string();

    let chapter_re = Regex::new(r"(?i)\bchapter\s+\d+\b").unwrap();
    s = chapter_re.replace_all(&s, " ").to_string();

    let part_re = Regex::new(r"(?i)\bpart\s+\d+\b").unwrap();
    s = part_re.replace_all(&s, " ").to_string();

    remove_common_metadata(&s)
}

fn strip_trailing_track_noise(input: &str) -> String {
    let mut s = normalize_guess(input);

    let start_track_re = Regex::new(r"^\d{1,3}\s*-\s*").unwrap();
    s = start_track_re.replace(&s, "").to_string();

    let end_track_re = Regex::new(r"\s*-\s*\d{1,3}$").unwrap();
    s = end_track_re.replace(&s, "").to_string();

    let bracket_track_re = Regex::new(r"\s*\[\d{1,3}(?:-\d{1,3})?\]$").unwrap();
    s = bracket_track_re.replace(&s, "").to_string();

    collapse_spaces(&s)
}

fn push_phrase(out: &mut Vec<String>, value: impl Into<String>) {
    let value = normalize_guess(&value.into());
    if !looks_useful_guess_phrase(&value) {
        return;
    }
    out.push(value);
}

fn looks_useful_guess_phrase(value: &str) -> bool {
    let value = normalize_guess(value);
    if value.len() < 2 {
        return false;
    }

    if value
        .chars()
        .all(|c| c.is_ascii_digit() || c.is_whitespace() || c == '.' || c == '-')
    {
        return false;
    }

    true
}

fn looks_like_author_name(value: &str) -> bool {
    let value = normalize_guess(value);
    if value.is_empty() {
        return false;
    }

    let lower = value.to_lowercase();
    if lower.contains(" trilogy")
        || lower.contains(" saga")
        || lower.contains(" series")
        || lower.contains(" novels")
        || lower.contains(" chronicles")
        || lower.contains(" cycle")
        || lower.contains(" books")
        || lower.contains(" book ")
    {
        return false;
    }

    let words: Vec<&str> = value.split_whitespace().collect();
    if words.is_empty() || words.len() > 5 {
        return false;
    }

    words.iter().all(|word| {
        word.chars()
            .next()
            .map(|ch| ch.is_ascii_uppercase())
            .unwrap_or(false)
    })
}

fn looks_like_series_name(value: &str) -> bool {
    let lower = normalize_guess(value).to_lowercase();
    if lower.is_empty() {
        return false;
    }

    lower.contains(" trilogy")
        || lower.contains(" saga")
        || lower.contains(" series")
        || lower.contains(" novels")
        || lower.contains(" chronicles")
        || lower.contains(" cycle")
        || lower.contains(" books")
}

fn dedupe_preserve_order(values: Vec<String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();

    for value in values {
        let normalized = normalize_guess(&value);
        if normalized.is_empty() {
            continue;
        }

        let key = normalized.to_lowercase();
        if seen.insert(key) {
            out.push(normalized);
        }
    }

    out
}

fn push_guess(scored: &mut Vec<RankedGuess>, value: impl Into<String>, score: i32) {
    let value = normalize_guess(&value.into());
    if value.is_empty() {
        return;
    }
    scored.push(RankedGuess { value, score });
}

fn finalize_ranked_guesses(scored: Vec<RankedGuess>) -> Vec<String> {
    let mut best: HashMap<String, (String, i32)> = HashMap::new();

    for item in scored {
        let key = normalize_guess(&item.value).to_lowercase();
        if key.is_empty() {
            continue;
        }

        match best.get_mut(&key) {
            Some((value, score)) => {
                if item.score > *score || (item.score == *score && item.value < *value) {
                    *value = item.value;
                    *score = item.score;
                }
            }
            None => {
                best.insert(key, (item.value, item.score));
            }
        }
    }

    let mut items: Vec<(String, i32)> = best.into_values().collect();
    items.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    items.into_iter().map(|(value, _)| value).collect()
}

fn strip_extension(file_name: &str) -> String {
    StdPath::new(file_name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(file_name)
        .to_string()
}

fn remove_common_metadata(input: &str) -> String {
    let mut s = normalize_guess(input);

    let bracket_re = Regex::new(r"\[[^\[\]]*\]").unwrap();
    s = bracket_re.replace_all(&s, " ").to_string();

    let year_re = Regex::new(r"\((?:19|20)\d{2}(?:[–-](?:19|20)\d{2})?\)").unwrap();
    s = year_re.replace_all(&s, " ").to_string();

    let unabridged_re = Regex::new(r"\((?:U|u)nabridged\)").unwrap();
    s = unabridged_re.replace_all(&s, " ").to_string();

    let book_re = Regex::new(r",?\s+[Bb]ook\s+\d+").unwrap();
    s = book_re.replace_all(&s, " ").to_string();

    collapse_spaces(&s)
}
