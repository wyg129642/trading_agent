import { useState, useEffect, useCallback } from 'react'
import api from '../services/api'

export function useFavorites(itemType: string) {
  const [favoriteIds, setFavoriteIds] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (!itemType) return
    setLoading(true)
    api
      .get<string[]>('/favorites/ids', { params: { item_type: itemType } })
      .then((res) => {
        setFavoriteIds(new Set(res.data))
      })
      .catch(() => {})
      .finally(() => setLoading(false))
  }, [itemType])

  const isFavorited = useCallback(
    (itemId: string): boolean => favoriteIds.has(itemId),
    [favoriteIds],
  )

  const toggleFavorite = useCallback(
    async (type: string, itemId: string): Promise<void> => {
      const currentlyFavorited = favoriteIds.has(itemId)
      // Optimistic update
      setFavoriteIds((prev) => {
        const next = new Set(prev)
        if (currentlyFavorited) {
          next.delete(itemId)
        } else {
          next.add(itemId)
        }
        return next
      })

      try {
        if (currentlyFavorited) {
          await api.delete('/favorites', {
            params: { item_type: type, item_id: itemId },
          })
        } else {
          await api.post('/favorites', { item_type: type, item_id: itemId })
        }
      } catch {
        // Revert on failure
        setFavoriteIds((prev) => {
          const next = new Set(prev)
          if (currentlyFavorited) {
            next.add(itemId)
          } else {
            next.delete(itemId)
          }
          return next
        })
      }
    },
    [favoriteIds],
  )

  return { favoriteIds, isFavorited, toggleFavorite, loading }
}
