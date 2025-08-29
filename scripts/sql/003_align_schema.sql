-- Drop legacy index that referenced a non-existent recommendations.score column and ensure needed uniques exist
DROP INDEX IF EXISTS public.idx_recommendations_score;

-- Ensure processed_interactions unique index for REST upserts
CREATE UNIQUE INDEX IF NOT EXISTS ux_processed_interactions_user_movie
ON public.processed_interactions (user_id, movie_id);
