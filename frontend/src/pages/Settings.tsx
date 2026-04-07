import { useState, useEffect } from 'react'
import { Card, Form, Input, Select, Button, Typography, message } from 'antd'
import { useTranslation } from 'react-i18next'
import { useAuthStore } from '../store/auth'
import api from '../services/api'

export default function Settings() {
  const { t, i18n } = useTranslation()
  const user = useAuthStore((s) => s.user)
  const fetchProfile = useAuthStore((s) => s.fetchProfile)
  const [form] = Form.useForm()
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (user) {
      form.setFieldsValue({
        display_name: user.display_name,
        email: user.email,
        language: user.language,
      })
    }
  }, [user, form])

  const handleSave = async (values: any) => {
    setLoading(true)
    try {
      await api.put('/auth/me', values)
      if (values.language) {
        i18n.changeLanguage(values.language)
        localStorage.setItem('language', values.language)
      }
      await fetchProfile()
      message.success(t('settings.saveSuccess'))
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('settings.saveFailed'))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ maxWidth: 600, margin: '0 auto' }}>
      <Typography.Title level={4}>{t('settings.title')}</Typography.Title>

      <Card title={t('settings.profile')}>
        <Form form={form} onFinish={handleSave} layout="vertical">
          <Form.Item label={t('auth.username')}>
            <Input value={user?.username} disabled />
          </Form.Item>
          <Form.Item name="display_name" label={t('settings.displayName')}>
            <Input />
          </Form.Item>
          <Form.Item name="email" label={t('auth.email')} rules={[{ type: 'email' }]}>
            <Input />
          </Form.Item>
          <Form.Item name="language" label={t('settings.language')}>
            <Select
              options={[
                { value: 'zh', label: '中文' },
                { value: 'en', label: 'English' },
              ]}
            />
          </Form.Item>
          <Form.Item>
            <Button type="primary" htmlType="submit" loading={loading}>
              {t('settings.save')}
            </Button>
          </Form.Item>
        </Form>
      </Card>

      <Card title={t('settings.preferences')} style={{ marginTop: 16 }}>
        <Typography.Text type="secondary">
          {t('settings.prefNote')}
        </Typography.Text>
      </Card>
    </div>
  )
}
