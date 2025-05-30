-- Enable WAL mode for better concurrent access
-- PRAGMA journal_mode=WAL;

-- Reduce sync overhead
-- PRAGMA synchronous=NORMAL;

create table if not exists train_position (
    created_at integer,

    lat real,
    lon real,

    delay integer,
    relation text,
    train_number text,
    menetvonal text,
    elvira_id text,
    line text
);

CREATE INDEX if not exists idx_timestamp ON train_position(created_at);
