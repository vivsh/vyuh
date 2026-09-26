-- Read-only report: repair these rows before applying 0003_result_preflight.
-- PostgreSQL 16+ validates JSON without throwing, so malformed rows are reported too.
SELECT id::text AS task_id FROM vyuh_tasks WHERE CASE WHEN (resume_input IS NULL OR (octet_length(resume_input) <= 32768 AND (CASE WHEN resume_input IS JSON OBJECT THEN
 (SELECT count(*) FROM json_object_keys(resume_input::json)) = 1 AND
 ((resume_input::json->'Ok' IS NOT NULL) OR
 (json_typeof(resume_input::json->'Err') = 'object'
 AND json_typeof(resume_input::json->'Err'->'message') = 'string'
 AND (resume_input::json->'Err'->'task_id' IS NULL
 OR json_typeof(resume_input::json->'Err'->'task_id') = 'null'
 OR (resume_input::json->'Err'->>'task_id') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')))
 ELSE false END))) AND (last_error IS NULL OR octet_length(json_build_object('Err', json_build_object('task_id', id::text, 'message', last_error))::text) <= 32768) THEN 1 ELSE 0 END = 0;
