-- Create interactions table if not exists
create table if not exists interactions (
  id bigserial primary key,
  user_id text not null,
  item_id text not null,
  event_type text not null, -- click, play, like, rate, etc.
  value double precision,   -- e.g., rating, watch ratio
  ts timestamptz not null default now()
);

-- basic indexes
create index if not exists idx_interactions_user on interactions(user_id);
create index if not exists idx_interactions_item on interactions(item_id);
create index if not exists idx_interactions_ts on interactions(ts);
