// Maps portfolio (stock_ticker, stock_market) to CODE.MARKET canonical id.
// Mirrors backend ticker_normalizer._canonical_from_code_market — keeping both
// in sync is cheap because the rule set is tiny and deterministic.

function classifyAshare(code: string): 'SH' | 'SZ' | 'BJ' | null {
  if (!/^\d{6}$/.test(code)) return null
  const p3 = code.slice(0, 3)
  const p2 = code.slice(0, 2)
  if (['600', '601', '603', '605', '688', '900'].includes(p3)) return 'SH'
  if (['000', '001', '002', '003', '300', '301', '200'].includes(p3)) return 'SZ'
  if (['43', '83', '87', '88', '92'].includes(p2)) return 'BJ'
  return null
}

export function toCanonical(ticker: string, market: string): string | null {
  const t = (ticker || '').trim()
  if (!t) return null
  if (market === '美股') return `${t.toUpperCase()}.US`
  if (market === '港股') {
    const digits = t.replace(/\D/g, '').padStart(5, '0')
    return digits ? `${digits}.HK` : null
  }
  if (market === '主板' || market === '创业板' || market === '科创板') {
    const cls = classifyAshare(t)
    return cls ? `${t}.${cls}` : null
  }
  if (market === '韩股') return `${t.toUpperCase()}.KS`
  if (market === '日股') return `${t.toUpperCase()}.JP`
  if (market === '澳股') return `${t.toUpperCase()}.AU`
  if (market === '德股') return `${t.toUpperCase()}.DE`
  return null
}
