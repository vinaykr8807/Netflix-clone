"use client"

import { useMemo, useState } from "react"
import { getBrowserSupabase } from "@/lib/supabase/client"

export default function UploadPage() {
  const [file, setFile] = useState<File | null>(null)
  const [status, setStatus] = useState<string>("")
  const [uploading, setUploading] = useState(false)
  const [uploadedPath, setUploadedPath] = useState<string | null>(null)

  const supabase = useMemo(() => getBrowserSupabase(), [])

  async function onUpload() {
    if (!file || uploading) return
    setStatus("Uploading...")
    setUploadedPath(null)
    setUploading(true)
    try {
      const path = `${Date.now()}-${file.name}`
      const { error } = await supabase.storage.from("datasets").upload(path, file, {
        cacheControl: "3600",
        upsert: false,
      })
      if (error) {
        setStatus(`Upload failed: ${error.message}. If the "datasets" bucket does not exist, create it in Supabase.`)
        return
      }
      setUploadedPath(path)
      setStatus("Upload complete")
    } catch (e: any) {
      setStatus(`Upload failed: ${e?.message || "Unknown error"}`)
    } finally {
      setUploading(false)
    }
  }

  return (
    <main className="min-h-screen bg-black text-white px-4 md:px-8 py-10">
      <h1 className="text-2xl font-semibold mb-4">Upload Dataset</h1>
      <p className="text-sm text-neutral-400 mb-6">
        Upload a CSV of interactions (user_id, item_id, timestamp, event_type, value). Ensure a Supabase Storage bucket
        named <span className="text-neutral-200 font-medium">datasets</span> exists.
      </p>
      <div className="flex flex-col sm:flex-row items-start gap-4">
        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          className="text-sm file:mr-3 file:rounded file:border-0 file:bg-red-600 file:text-white file:px-3 file:py-2"
        />
        <button
          onClick={onUpload}
          disabled={!file || uploading}
          className="px-4 py-2 rounded bg-red-600 hover:bg-red-500 disabled:opacity-60 disabled:cursor-not-allowed text-white text-sm"
        >
          {uploading ? "Uploading..." : "Upload"}
        </button>
      </div>
      {status && <p className="mt-4 text-sm text-neutral-300">{status}</p>}
      {uploadedPath && (
        <p className="mt-2 text-xs text-neutral-400">
          Stored in bucket <span className="text-neutral-200">datasets</span> at key{" "}
          <span className="text-neutral-200 break-all">{uploadedPath}</span>
        </p>
      )}
    </main>
  )
}
