import { Navbar } from "@/components/netflix/navbar"
import { Hero } from "@/components/netflix/hero"
import { Row } from "@/components/netflix/row"
import { TmdbRow } from "@/components/netflix/tmdb-row"

export default function HomePage() {
  return (
    <main className="min-h-dvh bg-background text-foreground">
      <Navbar />
      <Hero />
      <section className="space-y-10 px-4 sm:px-6 lg:px-10 py-8">
        {/* Dynamic rows from TMDB */}
        <TmdbRow title="Trending Now" endpoint="/api/tmdb/trending" />
        <TmdbRow title="Top Rated" endpoint="/api/tmdb/top-rated" />
        {/* Static sections */}
        <Row
          title="Popular on Netflix"
          items={[
            { id: "p1", title: "The Heist", image: "/movie-poster-dark-minimalist.png" },
            { id: "p2", title: "Edge of Night", image: "/movie-poster-action-neon.png" },
            { id: "p3", title: "Deep Space", image: "/sci-fi-space-movie-poster.png" },
            { id: "p4", title: "Lost City", image: "/movie-poster-adventure-jungle.png" },
            { id: "p5", title: "Cold Case", image: "/movie-poster-thriller-noir.png" },
            { id: "p6", title: "Neon Drive", image: "/movie-poster-neon-car.png" },
            { id: "p7", title: "Zero Hour", image: "/movie-poster-time-thriller.png" },
            { id: "p8", title: "Afterlight", image: "/movie-poster-drama-silhouette.png" },
          ]}
        />
        <Row
          title="New Releases"
          items={[
            { id: "n1", title: "Blackout", image: "/placeholder.svg?height=180&width=120" },
            { id: "n2", title: "Glass Trail", image: "/placeholder.svg?height=180&width=120" },
            { id: "n3", title: "Sea of Stars", image: "/placeholder.svg?height=180&width=120" },
            { id: "n4", title: "Undercover", image: "/placeholder.svg?height=180&width=120" },
            { id: "n5", title: "Monolith", image: "/placeholder.svg?height=180&width=120" },
            { id: "n6", title: "Pulse", image: "/placeholder.svg?height=180&width=120" },
            { id: "n7", title: "Echoes", image: "/placeholder.svg?height=180&width=120" },
            { id: "n8", title: "Voltage", image: "/placeholder.svg?height=180&width=120" },
          ]}
        />
      </section>
      <footer className="px-4 sm:px-6 lg:px-10 py-10 text-sm bg-secondary text-muted-foreground">
        <p className="max-w-5xl text-pretty">
          This is a demo Netflix-style UI built for the web. Images are placeholders. No affiliation with Netflix.
        </p>
      </footer>
    </main>
  )
}
