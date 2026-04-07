import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Card, Form, Input, Button, Typography, Tabs, message } from 'antd'
import { UserOutlined, LockOutlined, MailOutlined } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'

const { Title, Text } = Typography

export default function Login() {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const { login, register } = useAuthStore()
  const [loading, setLoading] = useState(false)
  const [activeTab, setActiveTab] = useState('login')

  const handleLogin = async (values: { username: string; password: string }) => {
    setLoading(true)
    try {
      await login(values.username, values.password)
      message.success(t('auth.loginSuccess'))
      navigate('/')
    } catch (err: any) {
      message.error(err.response?.data?.detail || t('auth.loginFailed'))
    } finally {
      setLoading(false)
    }
  }

  const handleRegister = async (values: {
    username: string
    email: string
    password: string
    display_name?: string
  }) => {
    setLoading(true)
    try {
      await register(values.username, values.email, values.password, values.display_name)
      message.success(t('auth.registerSuccess'))
      setActiveTab('login')
    } catch (err: any) {
      message.error(err.response?.data?.detail || t('auth.registerFailed'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="login-bg">
      <Card className="login-card" bordered={false}>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <div
            style={{
              width: 48,
              height: 48,
              background: 'linear-gradient(135deg, #3b82f6, #2563eb)',
              borderRadius: 12,
              display: 'inline-flex',
              alignItems: 'center',
              justifyContent: 'center',
              color: 'white',
              fontWeight: 700,
              fontSize: 20,
              marginBottom: 12,
            }}
          >
            TI
          </div>
          <Title level={3} style={{ margin: 0 }}>
            {t('app.title')}
          </Title>
          <Text type="secondary" style={{ fontSize: 13 }}>
            {t('app.subtitle')}
          </Text>
        </div>

        <Tabs
          activeKey={activeTab}
          onChange={setActiveTab}
          centered
          items={[
            {
              key: 'login',
              label: t('auth.login'),
              children: (
                <Form onFinish={handleLogin} size="large">
                  <Form.Item
                    name="username"
                    rules={[{ required: true, message: t('auth.username') }]}
                  >
                    <Input prefix={<UserOutlined />} placeholder={t('auth.username')} />
                  </Form.Item>
                  <Form.Item
                    name="password"
                    rules={[{ required: true, message: t('auth.password') }]}
                  >
                    <Input.Password prefix={<LockOutlined />} placeholder={t('auth.password')} />
                  </Form.Item>
                  <Form.Item>
                    <Button type="primary" htmlType="submit" loading={loading} block>
                      {t('auth.login')}
                    </Button>
                  </Form.Item>
                </Form>
              ),
            },
            {
              key: 'register',
              label: t('auth.register'),
              children: (
                <Form onFinish={handleRegister} size="large">
                  <Form.Item name="username" rules={[{ required: true, min: 3, max: 50 }]}>
                    <Input prefix={<UserOutlined />} placeholder={t('auth.username')} />
                  </Form.Item>
                  <Form.Item name="email" rules={[{ required: true, type: 'email' }]}>
                    <Input prefix={<MailOutlined />} placeholder={t('auth.email')} />
                  </Form.Item>
                  <Form.Item name="password" rules={[{ required: true, min: 6 }]}>
                    <Input.Password prefix={<LockOutlined />} placeholder={t('auth.password')} />
                  </Form.Item>
                  <Form.Item name="display_name">
                    <Input
                      prefix={<UserOutlined />}
                      placeholder={t('auth.displayNamePlaceholder')}
                    />
                  </Form.Item>
                  <Form.Item>
                    <Button type="primary" htmlType="submit" loading={loading} block>
                      {t('auth.register')}
                    </Button>
                  </Form.Item>
                </Form>
              ),
            },
          ]}
        />
      </Card>
    </div>
  )
}
