import { cookies } from "next/headers"
import { createServerClient } from "@supabase/ssr"

let _client: ReturnType<typeof createServerClient> | null = null

export function getServerSupabase() {
  if (_client) return _client
  const cookieStore = cookies()
  _client = createServerClient(process.env.NEXT_PUBLIC_SUPABASE_URL!, process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!, {
    cookies: () => cookieStore,
  })
  return _client
}
