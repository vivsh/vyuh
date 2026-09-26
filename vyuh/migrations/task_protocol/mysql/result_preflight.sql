-- Read-only report: repair these rows before applying 0003_result_preflight.
-- PostgreSQL reports malformed JSON as a cast error; repair it before retrying.
SELECT LOWER(CONCAT(SUBSTR(HEX(id), 1, 8), '-', SUBSTR(HEX(id), 9, 4), '-', SUBSTR(HEX(id), 13, 4), '-', SUBSTR(HEX(id), 17, 4), '-', SUBSTR(HEX(id), 21, 12))) AS task_id FROM vyuh_tasks WHERE CASE WHEN (resume_input IS NULL OR (OCTET_LENGTH(resume_input) <= 32768 AND (CASE WHEN JSON_VALID(resume_input) THEN
 CASE WHEN JSON_TYPE(resume_input) = 'OBJECT' THEN
 JSON_LENGTH(resume_input) = 1 AND
 (JSON_CONTAINS_PATH(resume_input, 'one', '$.Ok') = 1 OR
 (JSON_TYPE(JSON_EXTRACT(resume_input, '$.Err')) = 'OBJECT'
 AND JSON_TYPE(JSON_EXTRACT(resume_input, '$.Err.message')) = 'STRING'
 AND (JSON_CONTAINS_PATH(resume_input, 'one', '$.Err.task_id') = 0
 OR JSON_TYPE(JSON_EXTRACT(resume_input, '$.Err.task_id')) = 'NULL'
 OR (JSON_TYPE(JSON_EXTRACT(resume_input, '$.Err.task_id')) = 'STRING'
 AND JSON_UNQUOTE(JSON_EXTRACT(resume_input, '$.Err.task_id')) REGEXP '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'))))
 ELSE 0 END ELSE 0 END))) AND (last_error IS NULL OR OCTET_LENGTH(JSON_OBJECT('Err', JSON_OBJECT('task_id', LOWER(CONCAT(SUBSTR(HEX(id), 1, 8), '-', SUBSTR(HEX(id), 9, 4), '-', SUBSTR(HEX(id), 13, 4), '-', SUBSTR(HEX(id), 17, 4), '-', SUBSTR(HEX(id), 21, 12))), 'message', last_error))) <= 32768) THEN 1 ELSE 0 END = 0;
