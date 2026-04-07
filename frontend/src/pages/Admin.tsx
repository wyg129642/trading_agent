import { useEffect, useState } from 'react'
import { Card, Table, Tag, Select, message, Typography } from 'antd'
import { useTranslation } from 'react-i18next'
import api from '../services/api'
import dayjs from 'dayjs'

interface User {
  id: string
  username: string
  email: string
  display_name: string | null
  role: string
  is_active: boolean
  created_at: string
  last_login_at: string | null
}

const ROLE_COLORS: Record<string, string> = {
  admin: 'red',
  boss: 'gold',
  trader: 'blue',
  viewer: 'default',
}

export default function Admin() {
  const { t } = useTranslation()
  const [users, setUsers] = useState<User[]>([])
  const [loading, setLoading] = useState(true)

  const fetchUsers = async () => {
    try {
      const res = await api.get('/admin/users')
      setUsers(res.data)
    } catch (e: any) {
      message.error(e.response?.data?.detail || t('common.error'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    fetchUsers()
  }, [])

  const handleRoleChange = async (userId: string, role: string) => {
    try {
      await api.put(`/admin/users/${userId}`, { role })
      message.success(t('admin.roleUpdated'))
      fetchUsers()
    } catch {
      message.error(t('common.error'))
    }
  }

  const handleActiveChange = async (userId: string, is_active: boolean) => {
    try {
      await api.put(`/admin/users/${userId}`, { is_active })
      message.success(t('admin.statusUpdated'))
      fetchUsers()
    } catch {
      message.error(t('common.error'))
    }
  }

  return (
    <div>
      <Typography.Title level={4} style={{ marginBottom: 16 }}>
        {t('admin.userManagement')}
      </Typography.Title>

      <Card>
        <Table
          loading={loading}
          dataSource={users}
          rowKey="id"
          size="small"
          columns={[
            {
              title: t('admin.username'),
              dataIndex: 'username',
              key: 'username',
              width: 120,
              render: (v: string) => <Typography.Text strong>{v}</Typography.Text>,
            },
            {
              title: t('admin.email'),
              dataIndex: 'email',
              key: 'email',
              width: 200,
            },
            {
              title: t('admin.displayName'),
              dataIndex: 'display_name',
              key: 'dn',
              width: 120,
              render: (v: string | null) => v || '-',
            },
            {
              title: t('admin.role'),
              dataIndex: 'role',
              key: 'role',
              width: 130,
              render: (role: string, record: User) => (
                <Select
                  value={role}
                  size="small"
                  style={{ width: 110 }}
                  onChange={(v) => handleRoleChange(record.id, v)}
                  options={[
                    { value: 'admin', label: t('admin.roleAdmin') },
                    { value: 'boss', label: t('admin.roleBoss') },
                    { value: 'trader', label: t('admin.roleTrader') },
                    { value: 'viewer', label: t('admin.roleViewer') },
                  ]}
                />
              ),
            },
            {
              title: t('admin.isActive'),
              dataIndex: 'is_active',
              key: 'active',
              width: 80,
              render: (active: boolean, record: User) => (
                <Tag
                  color={active ? 'green' : 'red'}
                  style={{ cursor: 'pointer' }}
                  onClick={() => handleActiveChange(record.id, !active)}
                >
                  {active ? t('admin.activeLabel') : t('admin.inactiveLabel')}
                </Tag>
              ),
            },
            {
              title: t('admin.created'),
              dataIndex: 'created_at',
              key: 'created',
              width: 110,
              render: (v: string) => dayjs(v).tz('Asia/Shanghai').format('YYYY-MM-DD'),
            },
            {
              title: t('admin.lastLogin'),
              dataIndex: 'last_login_at',
              key: 'login',
              width: 140,
              render: (v: string | null) =>
                v ? dayjs(v).tz('Asia/Shanghai').format('YYYY-MM-DD HH:mm') : t('admin.never'),
            },
          ]}
        />
      </Card>
    </div>
  )
}
