//! Bounded reverse reading of configured JSON tracing files for the console.

use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, NaiveDate, Utc};
use ring::{aead, rand::SecureRandom};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    auth::{KeySource, SecretRing},
    logging::{LogLevel, LogSink, LoggingConf, Rotation},
};

use super::query::LogQuery;

const BLOCK_BYTES: usize = 64 * 1024;
const SCAN_BYTES: usize = 8 * 1024 * 1024;
const MAX_FILES: usize = 64;
const MAX_LINE_BYTES: usize = 256 * 1024;
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const NONCE_BYTES: usize = 12;
const FIELD_LIMIT: usize = 8;
const SPAN_LIMIT: usize = 4;
const VALUE_LIMIT: usize = 128;
const KEY_CONTEXT: &[u8] = b"console-log-cursor-v1";

/// One safe console representation of a structured tracing event.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogEntry {
    pub(crate) id: String,
    pub(crate) timestamp: String,
    pub(crate) level: String,
    pub(crate) target: String,
    pub(crate) message: String,
    pub(crate) rule: String,
    pub(crate) fields: BTreeMap<String, String>,
    pub(crate) spans: Vec<LogSpan>,
}

/// A bounded span projection for one console log event.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogSpan {
    pub(crate) name: String,
    pub(crate) fields: BTreeMap<String, String>,
}

/// One cursor-paginated console log response.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct LogPage {
    pub(crate) items: Vec<LogEntry>,
    pub(crate) rules: Vec<String>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) truncated: bool,
    pub(crate) configured: bool,
}

/// Private runtime state for the console file-log viewer.
pub(crate) struct LogRuntime {
    sources: Vec<LogSource>,
    cursors: CursorCodec,
}

#[derive(Clone)]
struct LogSource {
    name: String,
    dir: PathBuf,
    _rotation: Rotation,
}

#[derive(Clone, Serialize, Deserialize)]
struct PageCursor {
    version: u8,
    filter: String,
    files: Vec<FileCursor>,
}

#[derive(Clone, Serialize, Deserialize)]
struct FileCursor {
    rule: String,
    file: String,
    offset: u64,
    modified: u64,
}

#[derive(Clone, Serialize, Deserialize)]
struct EntryCursor {
    version: u8,
    filter: String,
    rule: String,
    file: String,
    start: u64,
    digest: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", content = "value")]
enum CursorValue {
    Page(PageCursor),
    Entry(EntryCursor),
}

struct CursorCodec {
    active: Vec<u8>,
    verification: Vec<Vec<u8>>,
}

#[derive(Debug, Error)]
pub(crate) enum LogError {
    #[error("the log query is invalid")]
    InvalidQuery,
    #[error("the requested log cursor is invalid")]
    InvalidCursor,
    #[error("the selected log entry is unavailable")]
    EntryUnavailable,
    #[error("configured log files are unavailable")]
    Unavailable,
}

struct ScanState {
    remaining: usize,
    truncated: bool,
}

struct FileReader {
    source: String,
    name: String,
    file: File,
    offset: u64,
}

struct FoundEntry {
    entry: LogEntry,
    start: u64,
    end: u64,
    digest: String,
    rule: String,
    file: String,
}

enum LineRead {
    Data(u64, Vec<u8>),
    Skip,
    End,
    Budget,
}

impl LogRuntime {
    /// Builds validated per-site source descriptors without opening log files.
    pub(crate) fn new(
        conf: &LoggingConf,
        project_dir: &Path,
        secrets: &SecretRing,
    ) -> Result<Self, String> {
        Ok(Self {
            sources: file_sources(conf, project_dir),
            cursors: CursorCodec::new(secrets)?,
        })
    }

    /// Reads one bounded page of newest matching log events off the Tokio runtime.
    pub(crate) async fn page(&self, query: &LogQuery, limit: usize) -> Result<LogPage, LogError> {
        validate_query(query)?;
        let sources = self.sources.clone();
        let codec = self.cursors.clone();
        let query = query.normalized();
        tokio::task::spawn_blocking(move || read_page(&sources, &codec, &query, limit))
            .await
            .map_err(|_| LogError::Unavailable)?
    }

    /// Re-reads one signed entry pointer without retaining prior page contents.
    pub(crate) async fn selected(&self, query: &LogQuery) -> Result<Option<LogEntry>, LogError> {
        let Some(token) = query.selected.as_deref() else {
            return Ok(None);
        };
        let sources = self.sources.clone();
        let codec = self.cursors.clone();
        let token = token.to_owned();
        tokio::task::spawn_blocking(move || read_selected(&sources, &codec, &token))
            .await
            .map_err(|_| LogError::Unavailable)?
    }
}

/// Resolves one bounded console result size from the existing page configuration.
pub(crate) fn limit(query: &LogQuery, default: usize, max: usize) -> usize {
    query.limit.unwrap_or(default).clamp(1, max.min(100))
}

impl Clone for CursorCodec {
    fn clone(&self) -> Self {
        Self {
            active: self.active.clone(),
            verification: self.verification.clone(),
        }
    }
}

impl CursorCodec {
    fn new(secrets: &SecretRing) -> Result<Self, String> {
        let active = secrets
            .derived_active(&KeySource::site_secret(), KEY_CONTEXT, 32)
            .map_err(|_| "console log cursor key is invalid".to_string())?;
        let verification = secrets
            .derived_verification(&KeySource::site_secret(), KEY_CONTEXT, 32)
            .map_err(|_| "console log cursor key is invalid".to_string())?;
        Ok(Self {
            active,
            verification,
        })
    }

    fn seal(&self, value: CursorValue) -> Result<String, LogError> {
        let mut payload = serde_json::to_vec(&value).map_err(|_| LogError::InvalidCursor)?;
        let nonce = cursor_nonce()?;
        let bytes = nonce.as_ref().to_vec();
        cursor_key(&self.active)?
            .seal_in_place_append_tag(nonce, aead::Aad::empty(), &mut payload)
            .map_err(|_| LogError::InvalidCursor)?;
        let mut encoded = bytes;
        encoded.extend(payload);
        Ok(URL_SAFE_NO_PAD.encode(encoded))
    }

    fn open(&self, token: &str) -> Result<CursorValue, LogError> {
        if token.len() > MAX_CURSOR_BYTES {
            return Err(LogError::InvalidCursor);
        }
        let encoded = URL_SAFE_NO_PAD
            .decode(token)
            .map_err(|_| LogError::InvalidCursor)?;
        let (nonce, ciphertext) = split_cursor(&encoded)?;
        for material in &self.verification {
            let mut payload = ciphertext.to_vec();
            let nonce = aead::Nonce::assume_unique_for_key(nonce);
            if let Ok(value) =
                cursor_key(material)?.open_in_place(nonce, aead::Aad::empty(), &mut payload)
            {
                return serde_json::from_slice(value).map_err(|_| LogError::InvalidCursor);
            }
        }
        Err(LogError::InvalidCursor)
    }
}

fn cursor_nonce() -> Result<aead::Nonce, LogError> {
    let mut bytes = [0_u8; NONCE_BYTES];
    ring::rand::SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| LogError::Unavailable)?;
    Ok(aead::Nonce::assume_unique_for_key(bytes))
}

fn cursor_key(value: &[u8]) -> Result<aead::LessSafeKey, LogError> {
    aead::UnboundKey::new(&aead::AES_256_GCM, value)
        .map(aead::LessSafeKey::new)
        .map_err(|_| LogError::Unavailable)
}

fn split_cursor(value: &[u8]) -> Result<([u8; NONCE_BYTES], &[u8]), LogError> {
    if value.len() <= NONCE_BYTES + aead::AES_256_GCM.tag_len() {
        return Err(LogError::InvalidCursor);
    }
    let (nonce, payload) = value.split_at(NONCE_BYTES);
    let bytes: [u8; NONCE_BYTES] = nonce.try_into().map_err(|_| LogError::InvalidCursor)?;
    Ok((bytes, payload))
}

fn file_sources(conf: &LoggingConf, project_dir: &Path) -> Vec<LogSource> {
    conf.rules
        .iter()
        .filter_map(|rule| match &rule.sink {
            LogSink::File { dir, rotation } => Some(LogSource {
                name: rule.name.clone(),
                dir: crate::logging::resolve_log_dir(project_dir, dir),
                _rotation: *rotation,
            }),
            LogSink::Stdout { .. } | LogSink::Stderr { .. } => None,
        })
        .collect()
}

fn read_page(
    sources: &[LogSource],
    codec: &CursorCodec,
    query: &LogQuery,
    limit: usize,
) -> Result<LogPage, LogError> {
    let rules = sources.iter().map(|source| source.name.clone()).collect();
    if sources.is_empty() {
        return Ok(empty_page(rules));
    }
    let files = cursor_files(sources, codec, query)?;
    let mut state = ScanState {
        remaining: SCAN_BYTES,
        truncated: false,
    };
    let mut readers = open_readers(sources, &files)?;
    let mut next = next_entries(&mut readers, query, &mut state)?;
    let mut entries = Vec::with_capacity(limit);
    let mut seen = HashSet::with_capacity(limit);
    fill_page(
        codec,
        &mut readers,
        &mut next,
        query,
        &mut state,
        limit,
        &mut entries,
        &mut seen,
    )?;
    let cursor = next_cursor(codec, query, &readers, &next, state.truncated)?;
    Ok(LogPage {
        items: entries,
        rules,
        next_cursor: cursor,
        truncated: state.truncated,
        configured: true,
    })
}

fn empty_page(rules: Vec<String>) -> LogPage {
    LogPage {
        items: Vec::new(),
        rules,
        next_cursor: None,
        truncated: false,
        configured: false,
    }
}

fn cursor_files(
    sources: &[LogSource],
    codec: &CursorCodec,
    query: &LogQuery,
) -> Result<Vec<FileCursor>, LogError> {
    match query.cursor.as_deref() {
        Some(token) => cursor_page(codec, token, query),
        None => discover_files(sources, query),
    }
}

fn cursor_page(
    codec: &CursorCodec,
    token: &str,
    query: &LogQuery,
) -> Result<Vec<FileCursor>, LogError> {
    let CursorValue::Page(value) = codec.open(token)? else {
        return Err(LogError::InvalidCursor);
    };
    if value.version != 1 || value.filter != query.filter_key() {
        return Err(LogError::InvalidCursor);
    }
    Ok(value.files)
}

fn discover_files(sources: &[LogSource], query: &LogQuery) -> Result<Vec<FileCursor>, LogError> {
    let mut files = Vec::new();
    for source in selected_sources(sources, query)? {
        files.extend(source_files(source)?);
    }
    files.sort_by(|left, right| right.modified.cmp(&left.modified));
    files.truncate(MAX_FILES);
    Ok(files)
}

fn selected_sources<'a>(
    sources: &'a [LogSource],
    query: &LogQuery,
) -> Result<Vec<&'a LogSource>, LogError> {
    match query.rule.as_deref() {
        Some(name) => sources
            .iter()
            .find(|source| source.name == name)
            .map(|source| vec![source])
            .ok_or(LogError::InvalidQuery),
        None => Ok(sources.iter().collect()),
    }
}

fn source_files(source: &LogSource) -> Result<Vec<FileCursor>, LogError> {
    let entries = match fs::read_dir(&source.dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(_) => return Err(LogError::Unavailable),
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|_| LogError::Unavailable)?;
        if let Some(file) = source_file(source, entry.path())? {
            files.push(file);
        }
    }
    Ok(files)
}

fn source_file(source: &LogSource, path: PathBuf) -> Result<Option<FileCursor>, LogError> {
    let name = match path.file_name().and_then(|name| name.to_str()) {
        Some(name) => name,
        None => return Ok(None),
    };
    if !file_matches(source, name) {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(&path).map_err(|_| LogError::Unavailable)?;
    if !metadata.file_type().is_file() {
        return Ok(None);
    }
    Ok(Some(FileCursor {
        rule: source.name.clone(),
        file: name.to_owned(),
        offset: metadata.len(),
        modified: modified_at(&metadata),
    }))
}

fn modified_at(metadata: &fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn file_matches(source: &LogSource, file: &str) -> bool {
    let prefix = format!("{}.", source.name);
    file == source.name || file.starts_with(&prefix)
}

fn open_readers(sources: &[LogSource], files: &[FileCursor]) -> Result<Vec<FileReader>, LogError> {
    files
        .iter()
        .map(|cursor| open_reader(sources, cursor))
        .collect()
}

fn open_reader(sources: &[LogSource], cursor: &FileCursor) -> Result<FileReader, LogError> {
    let source = sources
        .iter()
        .find(|source| source.name == cursor.rule)
        .ok_or(LogError::InvalidCursor)?;
    let path = source.dir.join(&cursor.file);
    let metadata = fs::symlink_metadata(&path).map_err(|_| LogError::InvalidCursor)?;
    if !metadata.file_type().is_file()
        || !file_matches(source, &cursor.file)
        || cursor.offset > metadata.len()
    {
        return Err(LogError::InvalidCursor);
    }
    let file = File::open(path).map_err(|_| LogError::EntryUnavailable)?;
    Ok(FileReader {
        source: cursor.rule.clone(),
        name: cursor.file.clone(),
        file,
        offset: cursor.offset,
    })
}

fn next_entries(
    readers: &mut [FileReader],
    query: &LogQuery,
    state: &mut ScanState,
) -> Result<Vec<Option<FoundEntry>>, LogError> {
    readers
        .iter_mut()
        .map(|reader| next_entry(reader, query, state))
        .collect()
}

fn fill_page(
    codec: &CursorCodec,
    readers: &mut [FileReader],
    next: &mut [Option<FoundEntry>],
    query: &LogQuery,
    state: &mut ScanState,
    limit: usize,
    entries: &mut Vec<LogEntry>,
    seen: &mut HashSet<String>,
) -> Result<(), LogError> {
    while entries.len() < limit {
        let Some(index) = newest_index(next) else {
            break;
        };
        let Some(found) = next.get_mut(index).and_then(Option::take) else {
            break;
        };
        if seen.insert(found.digest.clone()) {
            let id = entry_cursor(codec, &found)?;
            let mut entry = found.entry;
            entry.id = id;
            entries.push(entry);
        }
        let reader = readers.get_mut(index).ok_or(LogError::Unavailable)?;
        let slot = next.get_mut(index).ok_or(LogError::Unavailable)?;
        *slot = next_entry(reader, query, state)?;
        if state.truncated {
            break;
        }
    }
    Ok(())
}

fn newest_index(entries: &[Option<FoundEntry>]) -> Option<usize> {
    entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| entry.as_ref().map(|entry| (index, &entry.entry.timestamp)))
        .max_by(|left, right| left.1.cmp(right.1))
        .map(|(index, _)| index)
}

fn next_cursor(
    codec: &CursorCodec,
    query: &LogQuery,
    readers: &[FileReader],
    next: &[Option<FoundEntry>],
    truncated: bool,
) -> Result<Option<String>, LogError> {
    if readers.is_empty() || (!truncated && next.iter().all(Option::is_none)) {
        return Ok(None);
    }
    let files = readers
        .iter()
        .enumerate()
        .map(|(index, reader)| cursor_file(reader, next.get(index)))
        .collect();
    codec
        .seal(CursorValue::Page(PageCursor {
            version: 1,
            filter: query.filter_key(),
            files,
        }))
        .map(Some)
}

fn cursor_file(reader: &FileReader, next: Option<&Option<FoundEntry>>) -> FileCursor {
    let offset = next
        .and_then(Option::as_ref)
        .map(|entry| entry.end)
        .unwrap_or(reader.offset);
    FileCursor {
        rule: reader.source.clone(),
        file: reader.name.clone(),
        offset,
        modified: 0,
    }
}

fn next_entry(
    reader: &mut FileReader,
    query: &LogQuery,
    state: &mut ScanState,
) -> Result<Option<FoundEntry>, LogError> {
    loop {
        let end = reader.offset;
        let (start, line) = match previous_line(reader, state)? {
            LineRead::Data(start, line) => (start, line),
            LineRead::Skip => continue,
            LineRead::End | LineRead::Budget => return Ok(None),
        };
        let digest = blake3::hash(&line).to_hex().to_string();
        let Some(mut entry) = parse_entry(&reader.source, &line, query)? else {
            continue;
        };
        entry.id = String::new();
        return Ok(Some(FoundEntry {
            entry,
            start,
            end,
            digest,
            rule: reader.source.clone(),
            file: reader.name.clone(),
        }));
    }
}

fn previous_line(reader: &mut FileReader, state: &mut ScanState) -> Result<LineRead, LogError> {
    let mut end = reader.offset;
    let mut parts = Vec::new();
    let mut length = 0_usize;
    while end > 0 {
        let start = end.saturating_sub(BLOCK_BYTES as u64);
        let Some(mut block) = read_block(reader, start, end, state)? else {
            return Ok(LineRead::Budget);
        };
        if parts.is_empty() && block.last() == Some(&b'\n') {
            block.pop();
        }
        if let Some(index) = block.iter().rposition(|byte| *byte == b'\n') {
            let line_start = start + index as u64 + 1;
            parts.push(block.split_off(index + 1));
            reader.offset = line_start;
            return join_line(line_start, parts, length.saturating_add(block.len()));
        }
        length = length.saturating_add(block.len());
        if length > MAX_LINE_BYTES {
            reader.offset = start;
            return Ok(LineRead::Skip);
        }
        parts.push(block);
        end = start;
    }
    reader.offset = 0;
    if parts.is_empty() {
        return Ok(LineRead::End);
    }
    join_line(0, parts, length)
}

fn read_block(
    reader: &mut FileReader,
    start: u64,
    end: u64,
    state: &mut ScanState,
) -> Result<Option<Vec<u8>>, LogError> {
    let length = usize::try_from(end.saturating_sub(start)).map_err(|_| LogError::Unavailable)?;
    if length > state.remaining {
        state.truncated = true;
        return Ok(None);
    }
    let mut block = vec![0_u8; length];
    reader
        .file
        .seek(SeekFrom::Start(start))
        .map_err(|_| LogError::Unavailable)?;
    reader
        .file
        .read_exact(&mut block)
        .map_err(|_| LogError::Unavailable)?;
    state.remaining -= length;
    Ok(Some(block))
}

fn join_line(start: u64, mut parts: Vec<Vec<u8>>, length: usize) -> Result<LineRead, LogError> {
    if parts.is_empty() {
        return Ok(LineRead::End);
    }
    if length > MAX_LINE_BYTES {
        return Ok(LineRead::Skip);
    }
    let mut line = Vec::with_capacity(length);
    while let Some(part) = parts.pop() {
        line.extend(part);
    }
    Ok(LineRead::Data(start, line))
}

fn parse_entry(rule: &str, line: &[u8], query: &LogQuery) -> Result<Option<LogEntry>, LogError> {
    let value: serde_json::Value = match serde_json::from_slice(line) {
        Ok(value) => value,
        Err(_) => return Ok(None),
    };
    let Some(timestamp) = value.get("timestamp").and_then(serde_json::Value::as_str) else {
        return Ok(None);
    };
    let Ok(time) = DateTime::parse_from_rfc3339(timestamp) else {
        return Ok(None);
    };
    let time = time.with_timezone(&Utc);
    let level = value
        .get("level")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("INFO");
    let target = value
        .get("target")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if !matches_query(&value, level, target, time, query) {
        return Ok(None);
    }
    Ok(Some(normalize_entry(
        rule, timestamp, level, target, &value,
    )))
}

fn matches_query(
    value: &serde_json::Value,
    level: &str,
    target: &str,
    time: DateTime<Utc>,
    query: &LogQuery,
) -> bool {
    query
        .level
        .as_ref()
        .is_none_or(|expected| expected.eq_ignore_ascii_case(level))
        && query
            .target
            .as_ref()
            .is_none_or(|prefix| target.starts_with(prefix))
        && query
            .from_date()
            .is_none_or(|start| time.date_naive() >= start)
        && query.to_date().is_none_or(|end| time.date_naive() <= end)
        && query
            .q
            .as_ref()
            .is_none_or(|text| value_contains(value, text))
}

fn value_contains(value: &serde_json::Value, query: &str) -> bool {
    match value {
        serde_json::Value::String(value) => value.to_lowercase().contains(query),
        serde_json::Value::Number(value) => value.to_string().contains(query),
        serde_json::Value::Bool(value) => value.to_string().contains(query),
        serde_json::Value::Array(values) => values.iter().any(|value| value_contains(value, query)),
        serde_json::Value::Object(values) => values
            .iter()
            .any(|(key, value)| key.to_lowercase().contains(query) || value_contains(value, query)),
        serde_json::Value::Null => false,
    }
}

fn normalize_entry(
    rule: &str,
    timestamp: &str,
    level: &str,
    target: &str,
    value: &serde_json::Value,
) -> LogEntry {
    let fields = value
        .get("fields")
        .and_then(serde_json::Value::as_object)
        .map(fields_out)
        .unwrap_or_default();
    let message = fields.get("message").cloned().unwrap_or_default();
    let spans = value
        .get("spans")
        .and_then(serde_json::Value::as_array)
        .map(|spans| spans_out(spans))
        .unwrap_or_default();
    LogEntry {
        id: String::new(),
        timestamp: timestamp.to_owned(),
        level: level.to_ascii_uppercase(),
        target: target.to_owned(),
        message,
        rule: rule.to_owned(),
        fields,
        spans,
    }
}

fn fields_out(value: &serde_json::Map<String, serde_json::Value>) -> BTreeMap<String, String> {
    value
        .iter()
        .take(FIELD_LIMIT)
        .filter_map(|(key, value)| scalar(value).map(|value| (key.clone(), value)))
        .collect()
}

fn spans_out(values: &[serde_json::Value]) -> Vec<LogSpan> {
    values
        .iter()
        .take(SPAN_LIMIT)
        .filter_map(span_out)
        .collect()
}

fn span_out(value: &serde_json::Value) -> Option<LogSpan> {
    let value = value.as_object()?;
    let name = value.get("name")?.as_str()?.to_owned();
    let fields = value
        .get("fields")
        .and_then(serde_json::Value::as_object)
        .map(fields_out)
        .unwrap_or_default();
    Some(LogSpan { name, fields })
}

fn scalar(value: &serde_json::Value) -> Option<String> {
    let value = match value {
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Number(value) => value.to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        _ => return None,
    };
    Some(truncate(value, VALUE_LIMIT))
}

fn truncate(mut value: String, max: usize) -> String {
    if value.len() > max {
        value.truncate(max);
        value.push('…');
    }
    value
}

fn read_selected(
    sources: &[LogSource],
    codec: &CursorCodec,
    token: &str,
) -> Result<Option<LogEntry>, LogError> {
    let CursorValue::Entry(pointer) = codec.open(token)? else {
        return Err(LogError::InvalidCursor);
    };
    if pointer.version != 1 {
        return Err(LogError::InvalidCursor);
    }
    let cursor = FileCursor {
        rule: pointer.rule,
        file: pointer.file,
        offset: 0,
        modified: 0,
    };
    let mut reader = open_reader(sources, &cursor)?;
    let line = selected_line(&mut reader.file, pointer.start)?;
    if blake3::hash(&line).to_hex().to_string() != pointer.digest {
        return Err(LogError::EntryUnavailable);
    }
    let mut entry = match parse_entry(&reader.source, &line, &LogQuery::default())? {
        Some(entry) => entry,
        None => return Ok(None),
    };
    entry.id = token.to_owned();
    Ok(Some(entry))
}

fn selected_line(file: &mut File, start: u64) -> Result<Vec<u8>, LogError> {
    file.seek(SeekFrom::Start(start))
        .map_err(|_| LogError::EntryUnavailable)?;
    let mut bytes = Vec::with_capacity(BLOCK_BYTES);
    file.by_ref()
        .take((MAX_LINE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| LogError::EntryUnavailable)?;
    let end = bytes
        .iter()
        .position(|byte| *byte == b'\n')
        .unwrap_or(bytes.len());
    if end >= MAX_LINE_BYTES {
        return Err(LogError::EntryUnavailable);
    }
    bytes.truncate(end);
    Ok(bytes)
}

fn entry_cursor(codec: &CursorCodec, found: &FoundEntry) -> Result<String, LogError> {
    codec.seal(CursorValue::Entry(EntryCursor {
        version: 1,
        filter: String::new(),
        rule: found.rule.clone(),
        file: found.file.clone(),
        start: found.start,
        digest: found.digest.clone(),
    }))
}

impl LogQuery {
    fn normalized(&self) -> Self {
        let mut query = self.clone();
        query.level = query.level.map(|value| value.to_ascii_uppercase());
        query.q = query.q.map(|value| value.to_lowercase());
        query
    }

    pub(crate) fn filter_key(&self) -> String {
        let value = serde_json::json!({ "rule": self.rule, "level": self.level, "target": self.target, "from": self.from, "to": self.to, "q": self.q });
        blake3::hash(value.to_string().as_bytes())
            .to_hex()
            .to_string()
    }

    fn from_date(&self) -> Option<NaiveDate> {
        parse_date(self.from.as_deref())
    }
    fn to_date(&self) -> Option<NaiveDate> {
        parse_date(self.to.as_deref())
    }
}

fn validate_query(query: &LogQuery) -> Result<(), LogError> {
    if query
        .level
        .as_deref()
        .is_some_and(|value| LogLevel::from_str(value).is_none())
    {
        return Err(LogError::InvalidQuery);
    }
    if query
        .from
        .as_deref()
        .is_some_and(|value| parse_date(Some(value)).is_none())
    {
        return Err(LogError::InvalidQuery);
    }
    if query
        .to
        .as_deref()
        .is_some_and(|value| parse_date(Some(value)).is_none())
    {
        return Err(LogError::InvalidQuery);
    }
    Ok(())
}

fn parse_date(value: Option<&str>) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(value?, "%Y-%m-%d").ok()
}

impl Default for LogQuery {
    fn default() -> Self {
        Self {
            rule: None,
            level: None,
            target: None,
            from: None,
            to: None,
            q: None,
            limit: None,
            cursor: None,
            selected: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        auth::SecretRing,
        logging::{LogRule, LogSink},
    };

    use super::*;

    /// Reads structured events newest first without loading the complete log file.
    #[tokio::test]
    async fn reads_filtered_entries_in_reverse_order() -> Result<(), LogError> {
        let directory = test_directory("reverse")?;
        write_log(
            &directory,
            "APP.2026-08-04",
            &[
                event("12:00:00", "INFO", "one"),
                event("12:01:00", "ERROR", "two"),
            ],
        )?;
        let runtime = test_runtime(&directory)?;
        let query = LogQuery {
            level: Some("ERROR".into()),
            ..LogQuery::default()
        };
        let page = runtime.page(&query, 10).await?;
        assert_eq!(page.items.len(), 1);
        assert_eq!(
            page.items.first().map(|entry| entry.message.as_str()),
            Some("two")
        );
        remove_directory(&directory);
        Ok(())
    }

    /// Preserves older entries behind an authenticated pagination cursor.
    #[tokio::test]
    async fn cursor_continues_to_older_entries() -> Result<(), LogError> {
        let directory = test_directory("cursor")?;
        write_log(
            &directory,
            "APP.2026-08-04",
            &[
                event("12:00:00", "INFO", "one"),
                event("12:01:00", "INFO", "two"),
            ],
        )?;
        let runtime = test_runtime(&directory)?;
        let first = runtime.page(&LogQuery::default(), 1).await?;
        let cursor = first.next_cursor.clone().ok_or(LogError::InvalidCursor)?;
        let second = runtime
            .page(
                &LogQuery {
                    cursor: Some(cursor),
                    ..LogQuery::default()
                },
                1,
            )
            .await?;
        assert_eq!(
            first.items.first().map(|entry| entry.message.as_str()),
            Some("two")
        );
        assert_eq!(
            second.items.first().map(|entry| entry.message.as_str()),
            Some("one")
        );
        remove_directory(&directory);
        Ok(())
    }

    /// Rejects a modified cursor instead of accepting an arbitrary file position.
    #[tokio::test]
    async fn rejects_modified_cursor() -> Result<(), LogError> {
        let directory = test_directory("cursor-tamper")?;
        write_log(
            &directory,
            "APP.2026-08-04",
            &[event("12:00:00", "INFO", "one")],
        )?;
        let runtime = test_runtime(&directory)?;
        let page = runtime.page(&LogQuery::default(), 1).await?;
        let cursor = match page.next_cursor {
            Some(cursor) => cursor,
            None => "invalid".to_owned(),
        };
        let query = LogQuery {
            cursor: Some(format!("{cursor}x")),
            ..LogQuery::default()
        };
        assert!(matches!(
            runtime.page(&query, 1).await,
            Err(LogError::InvalidCursor)
        ));
        remove_directory(&directory);
        Ok(())
    }

    /// Reopens exactly the signed entry location for the console inspector.
    #[tokio::test]
    async fn selected_entry_is_reread_from_disk() -> Result<(), LogError> {
        let directory = test_directory("selected")?;
        write_log(
            &directory,
            "APP.2026-08-04",
            &[event("12:00:00", "INFO", "one")],
        )?;
        let runtime = test_runtime(&directory)?;
        let page = runtime.page(&LogQuery::default(), 1).await?;
        let selected = page
            .items
            .first()
            .map(|entry| entry.id.clone())
            .ok_or(LogError::EntryUnavailable)?;
        let entry = runtime
            .selected(&LogQuery {
                selected: Some(selected),
                ..LogQuery::default()
            })
            .await?;
        assert_eq!(
            entry.as_ref().map(|entry| entry.message.as_str()),
            Some("one")
        );
        remove_directory(&directory);
        Ok(())
    }

    fn test_runtime(directory: &Path) -> Result<LogRuntime, LogError> {
        let conf = LoggingConf {
            env_prefix: None,
            rules: vec![LogRule {
                name: "APP".into(),
                sink: LogSink::File {
                    dir: directory.to_string_lossy().into_owned(),
                    rotation: Rotation::Daily,
                },
                default_filter: "info".into(),
            }],
        };
        let secrets = SecretRing::new("a sufficiently long test secret", &[], directory, 16)
            .map_err(|_| LogError::Unavailable)?;
        LogRuntime::new(&conf, directory, &secrets).map_err(|_| LogError::Unavailable)
    }

    fn test_directory(name: &str) -> Result<PathBuf, LogError> {
        let directory =
            std::env::temp_dir().join(format!("vyuh-console-logs-{name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&directory).map_err(|_| LogError::Unavailable)?;
        Ok(directory)
    }

    fn write_log(directory: &Path, name: &str, events: &[String]) -> Result<(), LogError> {
        let path = directory.join(name);
        fs::write(path, format!("{}\n", events.join("\n"))).map_err(|_| LogError::Unavailable)
    }

    fn event(time: &str, level: &str, message: &str) -> String {
        format!(
            r#"{{"timestamp":"2026-08-04T{time}Z","level":"{level}","target":"app::worker","fields":{{"message":"{message}","request_id":"abc"}}}}"#
        )
    }

    fn remove_directory(directory: &Path) {
        let _ = fs::remove_dir_all(directory);
    }
}
