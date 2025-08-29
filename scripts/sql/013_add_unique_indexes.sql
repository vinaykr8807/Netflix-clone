-- Ensures upsert targets exist; safe to run multiple times
-- This does NOT drop or alter data. It only creates indexes if missing.

-- raw_links: make sure movie_id is unique (needed for upsert)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'raw_links_movie_id_key'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX raw_links_movie_id_key ON public.raw_links (movie_id)';
    EXCEPTION WHEN duplicate_table THEN
      -- ignore race
      NULL;
    END;
  END IF;
END$$;

-- processed_interactions: unique composite key for safe upserts on re-run
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'processed_interactions_user_movie_idx'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX processed_interactions_user_movie_idx ON public.processed_interactions (user_id, movie_id)';
    EXCEPTION WHEN duplicate_table THEN
      -- ignore race
      NULL;
    END;
  END IF;
END$$;

-- Optional: raw_ratings composite unique to avoid duplicates (not required if you don't upsert it)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_indexes
    WHERE schemaname = 'public'
      AND indexname = 'raw_ratings_user_movie_idx'
  ) THEN
    BEGIN
      EXECUTE 'CREATE UNIQUE INDEX raw_ratings_user_movie_idx ON public.raw_ratings (user_id, movie_id)';
    EXCEPTION WHEN duplicate_table THEN
      NULL;
    END;
  END IF;
END$$;
