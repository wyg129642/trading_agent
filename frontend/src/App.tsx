import { Routes, Route, Navigate } from 'react-router-dom'
import { ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import { useAuthStore } from './store/auth'
import AppLayout from './components/AppLayout'
import Login from './pages/Login'
import Dashboard from './pages/Dashboard'
import NewsFeed from './pages/NewsFeed'
import NewsDetail from './pages/NewsDetail'
import Watchlist from './pages/Watchlist'
import Sources from './pages/Sources'
import EngineStatus from './pages/EngineStatus'
import Analytics from './pages/Analytics'
import Settings from './pages/Settings'
import Admin from './pages/Admin'
import AdminFeed from './pages/AdminFeed'
import Portfolio from './pages/Portfolio'
import AlphaPaiDigest from './pages/AlphaPaiDigest'
import AlphaPaiFeed from './pages/AlphaPaiFeed'
import AlphaPaiRoadshows from './pages/AlphaPaiRoadshows'
import AlphaPaiComments from './pages/AlphaPaiComments'
import JiuqianForum from './pages/JiuqianForum'
import JiuqianMinutes from './pages/JiuqianMinutes'
import JiuqianWechat from './pages/JiuqianWechat'
import Favorites from './pages/Favorites'
import StockSearch from './pages/StockSearch'
import TopicRadar from './pages/TopicRadar'
import Leaderboard from './pages/Leaderboard'
import AnalystRating from './pages/AnalystRating'
import AIChat from './pages/AIChat'
import PredictionSubmit from './pages/PredictionSubmit'
import PredictionList from './pages/PredictionList'
import PredictionBacktest from './pages/PredictionBacktest'

function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const token = useAuthStore((s) => s.token)
  if (!token) return <Navigate to="/login" replace />
  return <>{children}</>
}

function AdminRoute({ children }: { children: React.ReactNode }) {
  const user = useAuthStore((s) => s.user)
  if (user?.role !== 'admin') return <Navigate to="/" replace />
  return <>{children}</>
}

function BossOrAdminRoute({ children }: { children: React.ReactNode }) {
  const user = useAuthStore((s) => s.user)
  if (user?.role !== 'admin' && user?.role !== 'boss') return <Navigate to="/" replace />
  return <>{children}</>
}

const theme = {
  token: {
    colorPrimary: '#2563eb',
    borderRadius: 6,
    colorBgContainer: '#ffffff',
    colorBgLayout: '#f1f5f9',
    fontFamily: "-apple-system, BlinkMacSystemFont, 'Segoe UI', 'Inter', Roboto, sans-serif",
    colorSuccess: '#10b981',
    colorWarning: '#f59e0b',
    colorError: '#ef4444',
    colorTextBase: '#1e293b',
    colorBorder: '#e2e8f0',
  },
}

export default function App() {
  return (
    <ConfigProvider theme={theme} locale={zhCN}>
      <Routes>
        <Route path="/login" element={<Login />} />
        <Route
          path="/"
          element={
            <ProtectedRoute>
              <AppLayout />
            </ProtectedRoute>
          }
        >
          <Route index element={<Dashboard />} />
          <Route path="feed" element={<NewsFeed />} />
          <Route path="news/:id" element={<NewsDetail />} />
          <Route path="watchlists" element={<Watchlist />} />
          <Route path="favorites" element={<Favorites />} />
          <Route path="stock-search" element={<StockSearch />} />
          <Route path="topic-radar" element={<TopicRadar />} />
          <Route path="sources" element={<Sources />} />
          <Route path="leaderboard" element={<Leaderboard />} />
          <Route path="analyst-rating" element={<AnalystRating />} />
          <Route path="ai-chat" element={<AIChat />} />
          <Route path="predictions" element={<PredictionList />} />
          <Route path="predictions/submit" element={<PredictionSubmit />} />
          <Route path="predictions/backtest" element={<BossOrAdminRoute><PredictionBacktest /></BossOrAdminRoute>} />
          <Route path="settings" element={<Settings />} />
          <Route path="portfolio" element={<BossOrAdminRoute><Portfolio /></BossOrAdminRoute>} />
          <Route path="alphapai/digest" element={<AlphaPaiDigest />} />
          <Route path="alphapai/feed" element={<AlphaPaiFeed />} />
          <Route path="alphapai/roadshows" element={<AlphaPaiRoadshows />} />
          <Route path="alphapai/comments" element={<AlphaPaiComments />} />
          <Route path="jiuqian/forum" element={<JiuqianForum />} />
          <Route path="jiuqian/minutes" element={<JiuqianMinutes />} />
          <Route path="jiuqian/wechat" element={<JiuqianWechat />} />
          {/* Admin-only routes */}
          <Route path="admin" element={<AdminRoute><Admin /></AdminRoute>} />
          <Route path="admin/feed" element={<AdminRoute><AdminFeed /></AdminRoute>} />
          <Route path="admin/sources" element={<AdminRoute><Sources /></AdminRoute>} />
          <Route path="engine" element={<AdminRoute><EngineStatus /></AdminRoute>} />
          <Route path="analytics" element={<AdminRoute><Analytics /></AdminRoute>} />
        </Route>
      </Routes>
    </ConfigProvider>
  )
}
