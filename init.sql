-- Auto-run by the official postgres image on first container startup
-- (mounted into /docker-entrypoint-initdb.d/). Creates the schema the
-- consumer service relies on, so the database is ready before the
-- app_service container connects to it.

CREATE TABLE IF NOT EXISTS user_profiles (
    user_id           UUID PRIMARY KEY,
    last_activity_at  TIMESTAMPTZ NOT NULL,
    total_events      INTEGER NOT NULL DEFAULT 0,
    last_event_type   VARCHAR(255) NOT NULL,
    event_counts      JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- Backbone of consumer idempotency: every successfully processed
-- event_id is recorded here so redelivered messages are detected and
-- skipped instead of double-counted.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      UUID PRIMARY KEY,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_user_profiles_last_activity_at
    ON user_profiles (last_activity_at);
