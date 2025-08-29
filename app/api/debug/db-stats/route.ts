import { NextResponse } from "next/server"

export async function GET() {
  try {
    const url = process.env.SUPABASE_URL
    const key = process.env.SUPABASE_SERVICE_ROLE_KEY
    if (!url || !key) {
      return NextResponse.json(
        { ok: false, error: "Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY" },
        { status: 500 },
      )
    }

    async function count(table: string) {
      const res = await fetch(`${url}/rest/v1/${table}?select=*&limit=1`, {
        headers: {
          apikey: key,
          Authorization: `Bearer ${key}`,
          Prefer: "count=exact",
        },
        cache: "no-store",
      })
      const total = Number.parseInt(res.headers.get("content-range")?.split("/")?.[1] || "0", 10)
      return Number.isFinite(total) ? total : 0
    }

    const [movies, links, rawRatings, processed, recs] = await Promise.all([
      count("raw_movies"),
      count("raw_links"),
      count("raw_ratings"),
      count("processed_interactions"),
      count("recommendations"),
    ])

    const serverTime = new Date().toISOString()
    const baseUrlEnv = process.env.NEXT_PUBLIC_BASE_URL || null

    return NextResponse.json({
      ok: true,
      tables: {
        raw_movies: movies,
        raw_links: links,
        raw_ratings: rawRatings,
        processed_interactions: processed,
        recommendations: recs,
      },
      meta: { serverTime, NEXT_PUBLIC_BASE_URL: baseUrlEnv },
    })
  } catch (e: any) {
    return NextResponse.json({ ok: false, error: e?.message || "unknown error" }, { status: 500 })
  }
}
