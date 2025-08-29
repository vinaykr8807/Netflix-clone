import { type NextRequest, NextResponse } from "next/server"

/*
  Fetch TMDB movie details via Bearer token (v4) or fallback to v3 key.
  Requires TMDB_BEARER or TMDB_API_KEY (server-only).
*/

async function fetchDetails(tmdbId: string) {
  const bearer = process.env.TMDB_BEARER
  const apiKey = process.env.TMDB_API_KEY
  const url = `https://api.themoviedb.org/3/movie/${tmdbId}?language=en-US`

  const headers: Record<string, string> = {}

  if (bearer) {
    headers["Authorization"] = `Bearer ${bearer}`
  } else if (apiKey) {
    // append api_key if no bearer
    return fetch(`${url}&api_key=${apiKey}`, { cache: "no-store" }).then((r) => r.json())
  } else {
    throw new Error("TMDB credentials missing")
  }

  const res = await fetch(url, { headers, cache: "no-store" })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(`TMDB error ${res.status}: ${text}`)
  }
  return res.json()
}

export async function GET(req: NextRequest, { params }: { params: { tmdbId: string } }) {
  try {
    const data = await fetchDetails(params.tmdbId)
    return NextResponse.json(data)
  } catch (e: any) {
    return NextResponse.json({ error: e.message }, { status: 500 })
  }
}
