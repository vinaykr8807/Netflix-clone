import { NextResponse } from "next/server"
import { tmdbFetch } from "../_utils"

export async function GET() {
  try {
    const data = await tmdbFetch("/trending/all/week")
    return NextResponse.json(data)
  } catch (err: any) {
    return NextResponse.json({ error: err?.message || "TMDB request failed" }, { status: 500 })
  }
}
