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
import AlphaPaiDigest from './pages/AlphaPaiDigest'
import AlphaPaiFeed from './pages/AlphaPaiFeed'
import AlphaPaiRoadshows from './pages/AlphaPaiRoadshows'
import AlphaPaiComments from './pages/AlphaPaiComments'
import AlphaPaiReports from './pages/AlphaPaiReports'
import PlatformInfo from './pages/PlatformInfo'
import JinmenDB from './pages/JinmenDB'
import JinmenReports from './pages/JinmenReports'
import JinmenPlatformInfo from './pages/JinmenPlatformInfo'
import GangtisePlatformInfo from './pages/GangtisePlatformInfo'
import MeritcoDB from './pages/MeritcoDB'
import ThirdBridgeDB from './pages/ThirdBridgeDB'
import FundaDB from './pages/FundaDB'
import FundaSentiment from './pages/FundaSentiment'
import GangtiseDB from './pages/GangtiseDB'
import AceCampDB from './pages/AceCampDB'
import AceCampPlatformInfo from './pages/AceCampPlatformInfo'
import AlphaEngineDB from './pages/AlphaEngineDB'
import JiuqianForum from './pages/JiuqianForum'
import JiuqianMinutes from './pages/JiuqianMinutes'
import JiuqianWechat from './pages/JiuqianWechat'
import Favorites from './pages/Favorites'
import StockSearch from './pages/StockSearch'
import Leaderboard from './pages/Leaderboard'
import AnalystRating from './pages/AnalystRating'
import AIChat from './pages/AIChat'
import MyKnowledgeBase from './pages/MyKnowledgeBase'
import MyMemories from './pages/MyMemories'
import AudioDetailPage from './pages/AudioDetailPage'
import PredictionSubmit from './pages/PredictionSubmit'
import PredictionList from './pages/PredictionList'
import PredictionBacktest from './pages/PredictionBacktest'
import DataSources from './pages/DataSources'
import ResearchLogs from './pages/ResearchLogs'
import DatabaseOverview from './pages/DatabaseOverview'
import RevenueModelList from './pages/RevenueModelList'
import RevenueModel from './pages/RevenueModel'
import RecipeEditor from './pages/RecipeEditor'
import RecipeCanvasEditor from './pages/RecipeCanvasEditor'
import RecipeABCompare from './pages/RecipeABCompare'
import ExpertCallRequests from './pages/ExpertCallRequests'
import PlaybookReview from './pages/PlaybookReview'
import CostDashboard from './pages/CostDashboard'
import FeedbackDashboard from './pages/FeedbackDashboard'
import KbDocViewer from './pages/KbDocViewer'
import PackEditor from './pages/PackEditor'

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
          <Route path="sources" element={<Sources />} />
          <Route path="leaderboard" element={<Leaderboard />} />
          <Route path="analyst-rating" element={<AnalystRating />} />
          <Route path="ai-chat" element={<AIChat />} />
          <Route path="my-knowledge" element={<MyKnowledgeBase />} />
          <Route path="my-knowledge/audio/:documentId" element={<AudioDetailPage />} />
          <Route path="my-memories" element={<MyMemories />} />
          <Route path="predictions" element={<PredictionList />} />
          <Route path="predictions/submit" element={<PredictionSubmit />} />
          <Route path="predictions/backtest" element={<BossOrAdminRoute><PredictionBacktest /></BossOrAdminRoute>} />
          <Route path="settings" element={<Settings />} />
          <Route path="alphapai/digest" element={<AlphaPaiDigest />} />
          <Route path="alphapai/platform-info" element={<PlatformInfo />} />
          <Route path="alphapai/feed" element={<AlphaPaiFeed />} />
          <Route path="alphapai/roadshows" element={<AlphaPaiRoadshows />} />
          <Route path="alphapai/comments" element={<AlphaPaiComments />} />
          <Route path="alphapai/reports" element={<AlphaPaiReports />} />
          <Route path="jinmen/platform-info" element={<JinmenPlatformInfo />} />
          <Route path="gangtise/platform-info" element={<GangtisePlatformInfo />} />
          <Route path="jinmen/meetings" element={<JinmenDB />} />
          <Route path="jinmen/reports" element={<JinmenReports />} />
          <Route path="jinmen/oversea-reports" element={<JinmenReports />} />
          <Route path="meritco/forum" element={<MeritcoDB />} />
          <Route path="meritco/:slug" element={<MeritcoDB />} />
          <Route path="thirdbridge/interviews" element={<ThirdBridgeDB />} />
          <Route path="funda/items" element={<FundaDB />} />
          <Route path="funda/sentiment" element={<FundaSentiment />} />
          <Route path="funda/:slug" element={<FundaDB />} />
          {/* Single dynamic route — the 3 static routes it replaces captured
              slug through the pathname but DID NOT populate useParams().category,
              leaving GangtiseDB stuck on the fallback "summary" view. */}
          <Route path="gangtise/:category" element={<GangtiseDB />} />
          <Route path="acecamp/platform-info" element={<AceCampPlatformInfo />} />
          <Route path="acecamp/:category" element={<AceCampDB />} />
          <Route path="alphaengine/:category" element={<AlphaEngineDB />} />
          <Route path="jiuqian/forum" element={<JiuqianForum />} />
          <Route path="jiuqian/minutes" element={<JiuqianMinutes />} />
          <Route path="jiuqian/wechat" element={<JiuqianWechat />} />
          {/* Admin-only routes */}
          <Route path="admin" element={<AdminRoute><Admin /></AdminRoute>} />
          <Route path="admin/feed" element={<AdminRoute><AdminFeed /></AdminRoute>} />
          <Route path="admin/sources" element={<AdminRoute><Sources /></AdminRoute>} />
          <Route path="admin/research-logs" element={<AdminRoute><ResearchLogs /></AdminRoute>} />
          <Route path="data-sources" element={<DataSources />} />
          <Route path="engine" element={<AdminRoute><EngineStatus /></AdminRoute>} />
          <Route path="analytics" element={<AdminRoute><Analytics /></AdminRoute>} />
          <Route path="database-overview" element={<DatabaseOverview />} />
          {/* ── Revenue Modeling ── */}
          <Route path="modeling" element={<RevenueModelList />} />
          <Route path="modeling/recipes" element={<RecipeEditor />} />
          <Route path="modeling/recipes/:id" element={<RecipeCanvasEditor />} />
          <Route path="modeling/:id/ab/:session" element={<RecipeABCompare />} />
          <Route path="modeling/expert-calls" element={<ExpertCallRequests />} />
          <Route path="modeling/playbook" element={<BossOrAdminRoute><PlaybookReview /></BossOrAdminRoute>} />
          <Route path="modeling/cost" element={<CostDashboard />} />
          <Route path="modeling/feedback" element={<BossOrAdminRoute><FeedbackDashboard /></BossOrAdminRoute>} />
          <Route path="modeling/kb-viewer" element={<KbDocViewer />} />
          <Route path="modeling/packs/:slug/edit" element={<BossOrAdminRoute><PackEditor /></BossOrAdminRoute>} />
          <Route path="modeling/:id" element={<RevenueModel />} />
        </Route>
      </Routes>
    </ConfigProvider>
  )
}
