"use client"

export function Card({ title, image }: { title: string; image: string }) {
  return (
    <article className="w-[120px] shrink-0 snap-start" role="listitem" aria-label={title}>
      <div className="relative aspect-[2/3] overflow-hidden rounded bg-[#141414] ring-1 ring-white/10">
        <img
          src={image || "/placeholder.svg"}
          alt={`${title} poster`}
          className="h-full w-full object-cover transition-transform duration-200 group-hover:scale-[1.03]"
        />
      </div>
      <h3 className="mt-2 text-xs text-neutral-300 line-clamp-2">{title}</h3>
    </article>
  )
}
