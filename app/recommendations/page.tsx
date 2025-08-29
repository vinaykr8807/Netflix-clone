export const dynamic = "force-dynamic"

import { RecommendationsList } from "@/components/recommendations/recommendations-list"

/*
  Simple page to browse recommendations by user id.
*/

export default function RecommendationsPage() {
  return (
    <main className="mx-auto max-w-6xl px-4 py-8 space-y-6">
      <header className="space-y-2">
        <h1 className="text-2xl md:text-3xl font-semibold text-balance">Personalized Recommendations</h1>
        <p className="text-sm text-muted-foreground">
          Enter a user id to view their top movie recommendations computed from interactions and trust signals.
        </p>
      </header>
      <RecommendationsList />
    </main>
  )
}
