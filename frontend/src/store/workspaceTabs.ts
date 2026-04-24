/**
 * Browser-style document tabs for the workspace.
 *
 * A "tab" represents one open document in the editor canvas. Tabs persist
 * across folder navigation — switching stocks in the tree doesn't close
 * what's open, matching the PaiWork reference. Tabs are persisted to
 * sessionStorage so a page refresh restores the exact layout (but a new
 * login starts fresh).
 *
 * Documents are distinguished by:
 *   - kind: 'workbook' (Univer / estilo Excel) | 'markdown' | 'file'
 *           (generic read-only file, falls back to the legacy detail drawer)
 *   - id:   mongo document id
 *
 * We cap the number of open tabs at 10. Opening an 11th evicts the LRU
 * non-active tab to avoid runaway memory growth.
 */
import { create } from 'zustand'
import { persist, createJSONStorage } from 'zustand/middleware'

export type DocKind = 'workbook' | 'markdown' | 'file'

export interface OpenTab {
  id: string
  title: string
  kind: DocKind
  folderId: string | null
  // Stock meta — optional convenience so the doc tab bar can show a
  // ticker chip next to the filename when the doc lives under a stock folder.
  stockTicker?: string | null
  dirty?: boolean
  // When was this tab last focused? Used for LRU eviction.
  lastActive: number
}

interface TabsState {
  tabs: OpenTab[]
  activeId: string | null
  open: (tab: Omit<OpenTab, 'lastActive'>) => void
  close: (id: string) => void
  activate: (id: string) => void
  rename: (id: string, title: string) => void
  markDirty: (id: string, dirty: boolean) => void
  reorder: (id: string, beforeId: string | null) => void
  closeAll: () => void
}

const MAX_OPEN_TABS = 10

export const useWorkspaceTabs = create<TabsState>()(
  persist(
    (set, get) => ({
      tabs: [],
      activeId: null,
      open: (tab) => {
        const st = get()
        const existing = st.tabs.find((t) => t.id === tab.id)
        if (existing) {
          set({
            activeId: existing.id,
            tabs: st.tabs.map((t) => t.id === existing.id
              ? { ...t, lastActive: Date.now(), title: tab.title || t.title, kind: tab.kind, folderId: tab.folderId, stockTicker: tab.stockTicker ?? t.stockTicker }
              : t),
          })
          return
        }
        let tabs = st.tabs.slice()
        if (tabs.length >= MAX_OPEN_TABS) {
          // Evict the least-recently-active non-active tab.
          const evictable = tabs
            .filter((t) => !t.dirty && t.id !== st.activeId)
            .sort((a, b) => a.lastActive - b.lastActive)[0]
          if (evictable) {
            tabs = tabs.filter((t) => t.id !== evictable.id)
          }
        }
        const next: OpenTab = { ...tab, lastActive: Date.now() }
        set({ tabs: [...tabs, next], activeId: next.id })
      },
      close: (id) => {
        const st = get()
        const remaining = st.tabs.filter((t) => t.id !== id)
        let active = st.activeId
        if (active === id) {
          // Focus the neighbor (or nothing if no tabs left).
          if (remaining.length === 0) active = null
          else {
            const idx = st.tabs.findIndex((t) => t.id === id)
            const neighbor = remaining[Math.min(idx, remaining.length - 1)]
            active = neighbor?.id ?? null
          }
        }
        set({ tabs: remaining, activeId: active })
      },
      activate: (id) => {
        const st = get()
        if (!st.tabs.some((t) => t.id === id)) return
        set({
          activeId: id,
          tabs: st.tabs.map((t) => t.id === id ? { ...t, lastActive: Date.now() } : t),
        })
      },
      rename: (id, title) => set((st) => ({
        tabs: st.tabs.map((t) => t.id === id ? { ...t, title } : t),
      })),
      markDirty: (id, dirty) => set((st) => ({
        tabs: st.tabs.map((t) => t.id === id ? { ...t, dirty } : t),
      })),
      reorder: (id, beforeId) => set((st) => {
        const src = st.tabs.find((t) => t.id === id)
        if (!src) return st
        const without = st.tabs.filter((t) => t.id !== id)
        if (beforeId === null) return { tabs: [...without, src] }
        const idx = without.findIndex((t) => t.id === beforeId)
        if (idx < 0) return { tabs: [...without, src] }
        return { tabs: [...without.slice(0, idx), src, ...without.slice(idx)] }
      }),
      closeAll: () => set({ tabs: [], activeId: null }),
    }),
    {
      name: 'workspace:open-tabs:v1',
      storage: createJSONStorage(() => sessionStorage),
    },
  ),
)
