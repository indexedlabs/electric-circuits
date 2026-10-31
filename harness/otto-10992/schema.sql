CREATE TABLE IF NOT EXISTS record (id bigserial PRIMARY KEY, group_id integer NOT NULL, body text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS record_items (id bigserial PRIMARY KEY, record_id bigint NOT NULL REFERENCES record(id), body text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS record_meta (id bigserial PRIMARY KEY, record_id bigint NOT NULL REFERENCES record(id), body text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS plain_events (id bigserial PRIMARY KEY, group_id integer NOT NULL, body text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS sparse_events (id bigserial PRIMARY KEY, group_id integer NOT NULL, body text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now());
