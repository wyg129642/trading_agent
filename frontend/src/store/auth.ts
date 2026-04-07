import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import api from '../services/api'

interface User {
  id: string
  username: string
  email: string
  display_name: string | null
  role: 'admin' | 'boss' | 'trader' | 'viewer'
  language: string
}

interface AuthState {
  token: string | null
  refreshToken: string | null
  user: User | null
  login: (username: string, password: string) => Promise<void>
  register: (username: string, email: string, password: string, displayName?: string) => Promise<void>
  logout: () => void
  fetchProfile: () => Promise<void>
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      token: null,
      refreshToken: null,
      user: null,

      login: async (username: string, password: string) => {
        const res = await api.post('/auth/login', { username, password })
        set({ token: res.data.access_token, refreshToken: res.data.refresh_token })
        // Fetch profile
        const profile = await api.get('/auth/me', {
          headers: { Authorization: `Bearer ${res.data.access_token}` },
        })
        set({ user: profile.data })
      },

      register: async (username: string, email: string, password: string, displayName?: string) => {
        await api.post('/auth/register', {
          username,
          email,
          password,
          display_name: displayName || username,
        })
      },

      logout: () => {
        set({ token: null, refreshToken: null, user: null })
      },

      fetchProfile: async () => {
        try {
          const res = await api.get('/auth/me')
          set({ user: res.data })
        } catch {
          get().logout()
        }
      },
    }),
    { name: 'auth-storage', partialize: (state) => ({ token: state.token, refreshToken: state.refreshToken }) },
  ),
)
