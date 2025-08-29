const TMDB_BASE = "https://api.themoviedb.org/3"

function makeUrl(path: string) {
  // ensures we always add a leading slash
  const p = path.startsWith("/") ? path : `/${path}`
  return `${TMDB_BASE}${p}`
}

export async function tmdbFetch(path: string, searchParams?: Record<string, string | number>) {
  const bearer = process.env.TMDB_BEARER
  const apiKey = process.env.TMDB_API_KEY

  const url = new URL(makeUrl(path))
  url.searchParams.set("language", "en-US")
  if (searchParams) {
    for (const [k, v] of Object.entries(searchParams)) url.searchParams.set(k, String(v))
  }
  // prefer v4 bearer; fallback to v3 api key
  const headers: Record<string, string> = {}
  if (bearer) headers["Authorization"] = `Bearer ${bearer}`
  if (!bearer && apiKey) url.searchParams.set("api_key", apiKey)

  const res = await fetch(url.toString(), { headers, cache: "no-store" })
  if (!res.ok) {
    const body = await res.text().catch(() => "")
    throw new Error(`TMDB error ${res.status}: ${body || res.statusText}`)
  }
  return res.json()
}
