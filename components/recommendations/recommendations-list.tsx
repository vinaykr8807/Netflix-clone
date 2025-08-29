"use client"

import useSWR from "swr"
import { useState } from "react"

/*
  Client component to fetch and display recommendations for a userId.
*/

type RecItem = {
  movieId: number
  title: string | null
  tmdbId: number | null
  score: number
}
type RecResponse = {
  items: RecItem[]
  updated_at: string | null
}

const fetcher = (url: string) =>
  fetch(url).then((r) => {
    if (!r.ok) throw new Error(`HTTP ${r.status}`)
    return r.json()
  })

function Poster({ tmdbId, title }: { tmdbId: number | null; title: string | null }) {
  const { data } = useSWR(tmdbId ? `/api/tmdb/details/${tmdbId}` : null, fetcher)
  const imgPath: string | null = data?.poster_path ?? null
  const src = imgPath ? `https://image.tmdb.org/t/p/w342${imgPath}` : null

  return (
    <div className="w-40 h-60 bg-muted rounded-md overflow-hidden flex items-center justify-center">
      {src ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img src={src || "/placeholder.svg"} alt={title ?? "Poster"} className="w-full h-full object-cover" />
      ) : (
        <span className="text-xs text-muted-foreground text-center p-2">{title ?? "No title"}</span>
      )}
    </div>
  )
}

export function RecommendationsList() {
  const [userId, setUserId] = useState<string>("610")
  const { data, error, isLoading, mutate } = useSWR<RecResponse>(
    userId ? `/api/recommendations/${userId}` : null,
    fetcher,
  )

  return (
    <section className="space-y-4">
      <div className="flex items-center gap-2">
        <input
          className="px-3 py-2 rounded-md bg-background border border-border text-foreground"
          placeholder="Enter user id"
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          inputMode="numeric"
        />
        <button className="px-3 py-2 rounded-md bg-primary text-primary-foreground" onClick={() => mutate()}>
          Load
        </button>
      </div>

      {isLoading && <p className="text-sm text-muted-foreground">Loading...</p>}
      {error && <p className="text-sm text-destructive">Failed to load recommendations. Try another user id.</p>}

      {data && data.items?.length > 0 ? (
        <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-5 gap-4">
          {data.items.map((it) => (
            <div key={it.movieId} className="space-y-2">
              <Poster tmdbId={it.tmdbId} title={it.title} />
              <div className="space-y-1">
                <p className="text-sm font-medium text-pretty">{it.title ?? `Movie ${it.movieId}`}</p>
                <p className="text-xs text-muted-foreground">Score: {it.score.toFixed(2)}</p>
              </div>
            </div>
          ))}
        </div>
      ) : (
        !isLoading && <p className="text-sm text-muted-foreground">No recommendations found for user {userId}.</p>
      )}
    </section>
  )
}
