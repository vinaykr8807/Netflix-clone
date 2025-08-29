"use client"

import { Card } from "./title-card"

type Item = {
  id: string
  title: string
  image: string
}

export function Row({ title, items }: { title: string; items: Item[] }) {
  return (
    <section aria-label={title} className="space-y-3">
      <h2 className="text-lg font-semibold">{title}</h2>
      <div className="group relative">
        <div
          className="flex gap-3 overflow-x-auto no-scrollbar snap-x snap-mandatory pr-2"
          role="list"
          aria-label={`${title} carousel`}
        >
          {items.map((item) => (
            <Card key={item.id} title={item.title} image={item.image} />
          ))}
        </div>
      </div>
    </section>
  )
}
