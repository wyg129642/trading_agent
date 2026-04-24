import { useState } from 'react'
import { Typography, Badge } from 'antd'
import { ExperimentOutlined } from '@ant-design/icons'
import MemoriesPanel from '../components/MemoriesPanel'

const { Title, Text } = Typography

export default function MyMemories() {
  const [activeCount, setActiveCount] = useState<number>(0)

  return (
    <div style={{ padding: 20 }}>
      <div style={{ marginBottom: 16 }}>
        <Title level={3} style={{ margin: 0 }}>
          <ExperimentOutlined /> 我的记忆 <Badge count={activeCount} color="blue" style={{ marginLeft: 8 }} />
        </Title>
        <Text type="secondary">
          AI 助手从您的对话反馈中自动学习长期偏好。这些记忆在每次新对话中自动生效，您可以随时查看、修改或删除。
        </Text>
      </div>
      <MemoriesPanel onActiveCountChange={setActiveCount} />
    </div>
  )
}
