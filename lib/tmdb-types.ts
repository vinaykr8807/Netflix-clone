export type TmdbItem = {
  id: number
  title?: string
  name?: string
  overview?: string
  poster_path?: string | null
  backdrop_path?: string | null
}
export type TmdbResponse = {
  results: TmdbItem[]
}
