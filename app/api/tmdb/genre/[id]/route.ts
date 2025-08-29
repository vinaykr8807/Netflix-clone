import { NextResponse } from "next/server"
import { tmdbFetch } from "../../_utils"

type Params = { params: { id: string } }

export async function GET(_req: Request, { params }: Params) {
  try {
    const data = await tmdbFetch("/discover/movie", {
      with_genres: params.id,
      sort_by: "popularity.desc",
      page: 1,
    })
    return NextResponse.json(data)
  } catch (err: any) {
    return NextResponse.json({ error: err?.message || "TMDB request failed" }, { status: 500 })
  }
}
