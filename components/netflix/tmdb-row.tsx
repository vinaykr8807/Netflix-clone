"use client"

import useSWR from "swr"
import Image from "next/image"
import type { TmdbItem, TmdbResponse } from "@/lib/tmdb-types"
import { cn } from "@/lib/utils"

const fetcher = async (url: string) => {
  const r = await fetch(url)
  if (!r.ok) {
    const text = await r.text().catch(() => "")
    throw new Error(text || `Request failed with ${r.status}`)
  }
  return r.json()
}

export function TmdbRow({
  title,
  endpoint,
  compact = false,
}: {
  title: string
  endpoint: string
  compact?: boolean
}) {
  const { data, error, isLoading } = useSWR<TmdbResponse>(endpoint, fetcher)

  return (
    <section className="px-4 md:px-8 py-6">
      <h2 className="text-lg md:text-xl font-semibold text-foreground mb-3">{title}</h2>
      {isLoading && <p className="text-sm text-neutral-400">Loading...</p>}
      {error && <p className="text-sm text-red-400">Failed to load.</p>}
      <div
        className={cn(
          "flex gap-4 overflow-x-auto scrollbar-thin scrollbar-thumb-neutral-700 scrollbar-track-transparent",
          compact ? "py-2" : "py-3",
        )}
      >
        {(data?.results || []).map((item) => (
          <Card key={item.id} item={item} compact={compact} />
        ))}
      </div>
    </section>
  )
}

function Card({ item, compact }: { item: TmdbItem; compact?: boolean }) {
  const title = item.title || item.name || "Untitled"
  const poster = item.poster_path ? `https://image.tmdb.org/t/p/w500/${item.poster_path}` : "/abstract-movie-poster.png"

  return (
    <div className="min-w-[140px] md:min-w-[180px] lg:min-w-[200px] shrink-0">
      <div className="relative w-[140px] h-[210px] md:w-[180px] md:h-[270px] lg:w-[200px] lg:h-[300px] rounded-md overflow-hidden bg-neutral-900">
        <Image
          src={poster || "/placeholder.svg"}
          alt={`${title} poster`}
          fill
          className="object-cover transition-transform duration-300 hover:scale-105"
          sizes="200px"
        />
      </div>
      {!compact && <p className="mt-2 text-xs md:text-sm text-neutral-200 line-clamp-2">{title}</p>}
    </div>
  )
}
