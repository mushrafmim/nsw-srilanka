-- Created at: 2026-11-03T17:41:09Z
-- Need Postgres- or SQLite-only SQL? Nest -- @postgres / -- @sqlite blocks
-- inside @UP or @DOWN -- see docs/migrations.md.

-- @UP
-- SLPA's Cargo Management System reports a service order's approvals and its payment by
-- webhook, naming the order by the slug it issued, or by its order number. Each order is
-- recorded here when the CMS raises it, with the task that raised it, the consignment's case
-- and the flow's workflow, so a webhook finds the task waiting on the order within that flow
-- instead of searching every task's data (see external-integration/slpa/serviceorder).
CREATE TABLE IF NOT EXISTS slpa_service_orders (
	slug TEXT NOT NULL PRIMARY KEY,
	service_order_no TEXT NOT NULL DEFAULT '',
	task_id TEXT NOT NULL,
	case_id TEXT NOT NULL,
	workflow_id TEXT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_slpa_service_orders_service_order_no ON slpa_service_orders (service_order_no);
CREATE INDEX IF NOT EXISTS idx_slpa_service_orders_case_workflow ON slpa_service_orders (case_id, workflow_id, created_at);

-- Orders raised before this table, from the tasks that hold them. The task that raised an
-- order recorded it under its "so" namespace, and the tasks waiting on it were given the same
-- slug, so of the tasks holding a slug the one started first raised it. An order whose task has
-- no case or workflow cannot be scoped, and is left out.
INSERT INTO slpa_service_orders (slug, service_order_no, task_id, case_id, workflow_id, created_at)
SELECT t.data -> 'so' ->> 'slug',
	COALESCE(t.data -> 'so' ->> 'service_order_no', ''),
	t.task_id,
	t.case_id,
	t.parent_workflow_id,
	t.created_at
FROM taskflow_tasks t
WHERE COALESCE(t.data -> 'so' ->> 'slug', '') <> ''
	AND COALESCE(t.case_id, '') <> ''
	AND COALESCE(t.parent_workflow_id, '') <> ''
	AND NOT EXISTS (
		SELECT 1 FROM taskflow_tasks e
		WHERE e.data -> 'so' ->> 'slug' = t.data -> 'so' ->> 'slug'
			AND (e.created_at < t.created_at OR (e.created_at = t.created_at AND e.task_id < t.task_id))
	)
ON CONFLICT (slug) DO NOTHING;

-- The webhook no longer reads the task table by these, so the indexes added to core's table
-- for it (000013, 000014) go.
DROP INDEX IF EXISTS idx_taskflow_tasks_slpa_order_slug;
DROP INDEX IF EXISTS idx_taskflow_tasks_slpa_service_order_no;

-- @DOWN
CREATE INDEX IF NOT EXISTS idx_taskflow_tasks_slpa_service_order_no
	ON taskflow_tasks ((data -> 'so' ->> 'service_order_no'));
CREATE INDEX IF NOT EXISTS idx_taskflow_tasks_slpa_order_slug
	ON taskflow_tasks ((data -> 'so' ->> 'slug'));

DROP TABLE IF EXISTS slpa_service_orders;
