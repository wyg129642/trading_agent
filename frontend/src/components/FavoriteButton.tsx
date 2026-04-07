import React from 'react'
import { Tooltip } from 'antd'
import { StarOutlined, StarFilled } from '@ant-design/icons'
import { useTranslation } from 'react-i18next'

interface FavoriteButtonProps {
  itemType: string
  itemId: string
  favoriteIds: Set<string>
  onToggle: (itemType: string, itemId: string) => void
}

const FavoriteButton: React.FC<FavoriteButtonProps> = ({
  itemType,
  itemId,
  favoriteIds,
  onToggle,
}) => {
  const { t } = useTranslation()
  const isFav = favoriteIds.has(itemId)

  return (
    <Tooltip title={isFav ? t('favorites.remove') : t('favorites.add')}>
      <span
        onClick={(e) => {
          e.stopPropagation()
          onToggle(itemType, itemId)
        }}
        style={{ cursor: 'pointer', fontSize: 18, lineHeight: 1 }}
      >
        {isFav ? (
          <StarFilled style={{ color: '#fadb14' }} />
        ) : (
          <StarOutlined style={{ color: '#d9d9d9' }} />
        )}
      </span>
    </Tooltip>
  )
}

export default FavoriteButton
