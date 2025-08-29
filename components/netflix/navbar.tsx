"use client"

import Link from "next/link"
import { useState } from "react"

export function Navbar() {
  const [open, setOpen] = useState(false)

  return (
    <header className="sticky top-0 z-40 w-full bg-black/70 backdrop-blur supports-[backdrop-filter]:bg-black/50 border-b border-white/5">
      <div className="mx-auto flex max-w-6xl items-center justify-between px-4 sm:px-6 lg:px-8 h-14">
        <Link href="/" className="flex items-center gap-2 focus:outline-none focus:ring-2 focus:ring-[#3b82f6] rounded">
          <span className="h-6 w-6 bg-[#e50914] inline-block rounded-sm" aria-hidden="true" />
          <span className="text-lg font-semibold">Netflix Demo</span>
        </Link>

        <nav className="hidden md:flex items-center gap-6 text-sm text-neutral-200">
          <Link className="hover:text-white transition-colors" href="#">
            Home
          </Link>
          <Link className="hover:text-white transition-colors" href="#">
            TV Shows
          </Link>
          <Link className="hover:text-white transition-colors" href="#">
            Movies
          </Link>
          <Link className="hover:text-white transition-colors" href="#">
            New & Popular
          </Link>
          <Link className="hover:text-white transition-colors" href="#">
            My List
          </Link>
          <Link className="hover:text-white transition-colors" href="/upload">
            Upload
          </Link>
        </nav>

        <button
          className="md:hidden inline-flex items-center gap-2 text-neutral-200 focus:outline-none focus:ring-2 focus:ring-[#3b82f6] rounded px-2 py-1"
          aria-expanded={open}
          aria-controls="mobile-menu"
          onClick={() => setOpen((v) => !v)}
        >
          <span className="sr-only">Toggle menu</span>
          <span className="i-[menu]" aria-hidden="true">
            {"≡"}
          </span>
        </button>
      </div>

      <div id="mobile-menu" hidden={!open} className="md:hidden border-t border-white/5">
        <div className="px-4 py-3 space-y-3 bg-[#141414]">
          <Link className="block hover:text-white text-neutral-300" href="#">
            Home
          </Link>
          <Link className="block hover:text-white text-neutral-300" href="#">
            TV Shows
          </Link>
          <Link className="block hover:text-white text-neutral-300" href="#">
            Movies
          </Link>
          <Link className="block hover:text-white text-neutral-300" href="#">
            New & Popular
          </Link>
          <Link className="block hover:text-white text-neutral-300" href="#">
            My List
          </Link>
          <Link className="block hover:text-white text-neutral-300" href="/upload">
            Upload
          </Link>
        </div>
      </div>
    </header>
  )
}
