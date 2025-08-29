async function getStats() {
  const res = await fetch(`${process.env.NEXT_PUBLIC_BASE_URL || ""}/api/debug/db-stats`, { cache: "no-store" }).catch(
    () => null,
  )
  if (!res || !res.ok) return null
  return res.json()
}

export default async function DbStatsPage() {
  const data = await getStats()
  return (
    <main className="p-6 space-y-4">
      <h1 className="text-xl font-semibold">Database Stats</h1>
      {!data ? (
        <p className="text-sm text-muted-foreground">
          Could not fetch stats. Try calling /api/debug/db-stats directly.
        </p>
      ) : (
        <pre className="text-sm bg-muted p-4 rounded">{JSON.stringify(data, null, 2)}</pre>
      )}
      <p className="text-sm text-muted-foreground">
        Tip: After running ingestion and training, refresh this page to see updated counts.
      </p>
    </main>
  )
}
