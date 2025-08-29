import { type NextRequest, NextResponse } from "next/server"
import { cookies } from "next/headers"
import { createServerClient } from "@supabase/ssr"

/*
  Read recommendations for a userId from Supabase and return JSON.
  Security: server-side only, uses service envs via SSR client.
*/

export async function GET(req: NextRequest, { params }: { params: { userId: string } }) {
  const userId = Number(params.userId)
  if (!Number.isFinite(userId)) {
    return NextResponse.json({ error: "Invalid userId" }, { status: 400 })
  }

  const cookieStore = cookies()
  const supabase = createServerClient(process.env.SUPABASE_URL!, process.env.SUPABASE_ANON_KEY!, {
    cookies: {
      get(name: string) {
        return cookieStore.get(name)?.value
      },
    },
  })

  // Query recommendations table
  const { data, error } = await supabase
    .from("recommendations")
    .select("items, updated_at")
    .eq("user_id", userId)
    .maybeSingle()

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 })
  }

  if (!data) {
    return NextResponse.json({ items: [], updated_at: null })
  }

  return NextResponse.json(data)
}
