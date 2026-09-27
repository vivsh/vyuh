-- Replace this entire example roster with all application registrations.
-- Run with workers/writers stopped, after explicit reclassification and before
-- advancing the protocol marker. Every returned row requires manual resolution.
-- Decode Work and Flow checkpoint/resume types in application-specific preflight as well.
WITH registered(name, kind) AS (
    SELECT 'checkout', 1
    UNION ALL SELECT 'charge', 0
)
SELECT task.id, task.name, task.kind, task.status
FROM vyuh_tasks AS task
LEFT JOIN registered ON registered.name = task.name
WHERE task.status IN (0, 1, 2)
  AND (registered.name IS NULL
       OR registered.kind <> task.kind);
