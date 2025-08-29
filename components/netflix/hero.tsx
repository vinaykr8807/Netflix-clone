"use client"

export function Hero() {
  return (
    <section className="relative isolate bg-[#141414] overflow-hidden" aria-label="Featured title">
      <div className="absolute inset-0">
        <img
          src="/placeholder.svg?height=680&width=1280"
          alt="Featured cinematic background"
          className="h-full w-full object-cover opacity-60"
        />
        <div className="absolute inset-0 bg-gradient-to-t from-black via-black/60 to-black/10" />
      </div>

      <div className="relative mx-auto max-w-6xl px-4 sm:px-6 lg:px-8 py-20 sm:py-28">
        <div className="max-w-xl">
          <h1 className="text-3xl sm:text-4xl lg:text-5xl font-semibold text-balance">
            Watch Limitless Stories, Anytime
          </h1>
          <p className="mt-4 text-neutral-300 leading-relaxed">
            Stream trending movies and TV shows on our demo Netflix-style UI. No signup required—just a clean, fast,
            accessible web experience.
          </p>
          <div className="mt-6 flex items-center gap-3">
            <a
              href="#"
              className="inline-flex items-center justify-center rounded bg-[#e50914] px-4 py-2 text-sm font-medium text-white hover:bg-[#c40812] focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-[#3b82f6] focus:ring-offset-black"
            >
              Play
            </a>
            <a
              href="#"
              className="inline-flex items-center justify-center rounded bg-white/10 px-4 py-2 text-sm font-medium text-white hover:bg-white/20 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-[#3b82f6] focus:ring-offset-black"
            >
              More Info
            </a>
          </div>
        </div>
      </div>
    </section>
  )
}
