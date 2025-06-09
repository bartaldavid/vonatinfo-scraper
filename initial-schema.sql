-- Reduce sync overhead
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys = on;

create table if not exists relation (
    id integer primary key, 
    name text not null unique
);

create table if not exists train_number (
    id integer primary key, 
    train_number text not null unique
);

create table if not exists menetvonal (
    id integer primary key, 
    name text not null unique
);

create table if not exists elvira_id (
    id integer primary key, 
    elvira_id text not null unique
);

create table if not exists line_id (
    id integer primary key,
    line text not null unique
);

create table if not exists train_position (
    created_at integer,

    --lat real,
    --lon real,
    lat_micro integer,
    lon_micro integer,

    delay integer,
    relation_id integer references relation(id),
    train_number_id integer  references  train_number(id),
    menetvonal_id integer references menetvonal(id),
    elvira_id_id integer  references elvira_id(id),
    line_id integer references line_id(id)
);

CREATE INDEX if not exists idx_train_position_created_at ON train_position(created_at);
CREATE INDEX IF NOT EXISTS idx_train_position_relation_id ON train_position(relation_id);
CREATE INDEX IF NOT EXISTS idx_train_position_train_number_id ON train_position(train_number_id);
CREATE INDEX IF NOT EXISTS idx_train_position_menetvonal_id ON train_position(menetvonal_id);
CREATE INDEX IF NOT EXISTS idx_train_position_elvira_id_id ON train_position(elvira_id_id);
CREATE INDEX IF NOT EXISTS idx_train_position_line_id ON train_position(line_id);
create index if not exists idx_train_position_delay on train_position(delay);
