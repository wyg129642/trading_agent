"""Tag reference data: active THS concepts and CITIC level-1 industries.

Provides two lists used by the tagging phase of the analysis pipeline:
  - ACTIVE_CONCEPTS: ~390 active concept names from THS (同花顺) concept board
  - CITIC_INDUSTRIES: 30 CITIC (中信) level-1 industry names

Concept list is refreshed from the remote database on startup via refresh_concepts().
If the remote DB is unreachable, falls back to a hardcoded snapshot.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ── CITIC Level-1 Industries (中信一级行业, static) ──────────────────

CITIC_INDUSTRIES: list[str] = [
    "交通运输", "传媒", "农林牧渔", "医药", "商贸零售",
    "国防军工", "基础化工", "家电", "建材", "建筑",
    "房地产", "有色金属", "机械", "汽车", "消费者服务",
    "煤炭", "电力及公用事业", "电力设备及新能源", "电子", "石油石化",
    "纺织服装", "综合", "综合金融", "计算机", "轻工制造",
    "通信", "钢铁", "银行", "非银行金融", "食品饮料",
]

# ── Active THS Concepts (同花顺概念板块) ─────────────────────────────
# Hardcoded snapshot as fallback; refreshed from DB on startup.

_CONCEPT_SNAPSHOT: list[str] = [
    "新能源汽车", "大飞机", "参股保险", "网络游戏", "食品安全",
    "智能电网", "流感", "京津冀一体化", "海峡两岸", "禽流感",
    "军工", "煤化工", "氟化工概念", "横琴新区", "磷化工",
    "参股券商", "海工装备", "生物疫苗", "3D打印", "PM2.5",
    "草甘膦", "车联网", "创投", "时空大数据", "移动支付",
    "风电", "期货概念", "固废处理", "光热发电", "国产航母",
    "核电", "黄金概念", "建筑节能", "举牌", "可燃冰",
    "苹果概念", "燃料电池", "石墨烯", "水利", "钛白粉概念",
    "碳纤维", "特高压", "天然气", "污水处理", "物联网",
    "稀土永磁", "新疆振兴", "页岩气", "云计算", "转基因",
    "猪肉", "智慧城市", "智能医疗", "中字头股票", "卫星导航",
    "无线充电", "锂电池", "网络安全", "充电桩", "太赫兹",
    "安防", "文化传媒", "小金属概念", "家用电器", "机器人概念",
    "量子科技", "乡村振兴", "新型城镇化", "5G", "特钢概念",
    "土地流转", "ST板块", "新股与次新股", "无人机", "高端装备",
    "三胎概念", "融资融券", "语音技术", "智能家居", "生态农业",
    "土壤修复", "超级电容", "碳交易", "工业母机", "航运概念",
    "乳业", "传感器", "净水概念", "手机游戏", "光伏概念",
    "芯片概念", "钠离子电池", "足球概念", "化肥", "金属回收",
    "可降解塑料", "特斯拉", "大豆", "上海自贸区", "电子纸",
    "基因测序", "OLED", "人脸识别", "深圳国企改革", "钒电池",
    "智能穿戴", "冷链物流", "互联网金融", "旅游概念", "在线教育",
    "天津自贸区", "福建自贸区", "两轮车", "眼科医疗", "机器视觉",
    "百度概念", "参股银行", "小米概念", "有机硅概念", "无人驾驶",
    "染料", "农机", "民营医院", "新型烟草", "一带一路",
    "信托概念", "上海国企改革", "核污染防治", "摘帽", "华为概念",
    "航空发动机", "汽车电子", "工业大麻", "沪股通", "粤港澳大湾区",
    "养老概念", "白酒概念", "啤酒概念", "医疗器械概念", "金属镍",
    "高铁", "职业教育", "中韩自贸区", "细胞免疫治疗", "央企国企改革",
    "阿里巴巴概念", "跨境电商", "医药电商", "玉米", "养鸡",
    "金属铜", "金属锌", "赛马概念", "体育产业", "地下管网",
    "PPP概念", "互联网保险", "广东自贸区", "农村电商", "深股通",
    "供销社", "虚拟现实", "中船系", "证金持股", "人民币贬值受益",
    "工业互联网", "军民融合", "青蒿素", "智能物流", "数字货币",
    "腾讯概念", "电子竞技", "租售同权", "人工智能", "区块链",
    "债转股(AMC概念)", "智能音箱", "网约车", "蚂蚁金服概念",
    "股权转让", "共享单车", "钴", "雄安新区", "特色小镇",
    "无人零售", "丙烯酸", "装配式建筑", "储能", "自由贸易港",
    "超级品牌", "宁德时代概念", "石墨电极", "冰雪产业",
    "国家大基金持股", "动力电池回收", "水泥概念", "盐湖提锂",
    "存储芯片", "富士康概念", "独角兽概念", "创新药", "农业种植",
    "知识产权保护", "环氧丙烷", "固态电池", "MCU芯片", "抖音概念",
    "消费电子概念", "海南自贸区", "长三角一体化", "芬太尼", "柔性屏",
    "电力物联网", "数字孪生", "氢能源", "华为汽车", "人造肉",
    "数字乡村", "华为海思概念股", "国产操作系统", "脑机接口",
    "动物疫苗", "黑龙江自贸区", "ETC", "烟草", "垃圾分类",
    "仿制药一致性评价", "光刻胶", "宠物经济", "智慧政务", "无线耳机",
    "阿尔茨海默概念", "云游戏", "MiniLED", "网红经济", "HJT电池",
    "云办公", "消毒剂", "医疗废物处理", "数据中心", "C2M概念",
    "免税店", "快手概念", "中芯国际概念", "NMN概念", "汽车拆解概念",
    "代糖概念", "注册制次新股", "科创次新股", "第三代半导体",
    "辅助生殖", "拼多多概念", "医美概念", "煤炭概念", "物业管理",
    "同花顺漂亮100", "汽车芯片", "碳中和", "光伏建筑一体化",
    "鸿蒙概念", "共同富裕示范区", "牙科医疗", "CRO概念", "专精特新",
    "PVDF概念", "NFT概念", "元宇宙", "抽水蓄能", "绿色电力",
    "虚拟电厂", "培育钻石", "换电概念", "WiFi 6", "虚拟数字人",
    "数据安全", "EDR概念", "汽车热管理", "高压快充", "DRG/DIP",
    "柔性直流输电", "预制菜", "幽门螺杆菌概念", "重组蛋白",
    "东数西算（算力）", "硅能源", "PCB概念", "民爆概念", "智慧灯杆",
    "俄乌冲突概念", "中俄贸易概念", "跨境支付（CIPS）", "托育服务",
    "金属铅", "电子身份证", "数字经济", "国资云", "华为鲲鹏",
    "家庭医生", "华为欧拉", "毛发医疗", "MicroLED概念", "统一大市场",
    "肝炎概念", "露营经济", "猴痘概念", "粮食概念", "超超临界发电",
    "比亚迪概念", "F5G概念", "一体化压铸", "生物质能发电",
    "钙钛矿电池", "TOPCON电池", "减速器", "先进封装", "空气能热泵",
    "信创", "Web3.0", "高压氧舱", "AIGC概念", "PET铜箔",
    "国企改革", "数据确权", "POE胶膜", "血氧仪", "成飞概念",
    "ChatGPT概念", "共封装光学（CPO）", "数字水印", "毫米波雷达",
    "6G概念", "超导概念", "ERP概念", "MLOps概念", "数据要素",
    "液冷服务器", "同花顺中特估100", "MR（混合现实）", "英伟达概念",
    "空间计算", "算力租赁", "减肥药", "BC电池", "光刻机",
    "星闪概念", "新型工业化", "华为昇腾", "智能座舱", "短剧游戏",
    "长安汽车概念", "多模态AI", "PEEK材料", "小米汽车", "可控核聚变",
    "飞行汽车(eVTOL)", "低空经济", "Sora概念(文生视频)", "人形机器人",
    "AI手机", "AI PC", "高股息精选", "铜背板连接", "AI语料",
    "同花顺出海50", "军工信息化", "合成生物", "商业航天", "财税数字化",
    "维生素", "同花顺果指数", "光纤概念", "AI眼镜", "西部大开发",
    "房屋检测", "回购增持再贷款概念", "智谱AI", "华为手机",
    "华为数字能源", "华为盘古", "IP经济(谷子经济）", "同花顺新质50",
    "小红书概念", "AI智能体", "DeepSeek概念", "兵装重组概念",
    "中国AI50", "雅下水电概念", "2025三季报预增", "2025年报预增",
    "AI应用",
]

ACTIVE_CONCEPTS: list[str] = list(_CONCEPT_SNAPSHOT)


async def refresh_concepts(
    host: str = "192.168.31.176",
    port: int = 3306,
    user: str = "researcher",
    password: str = "researcher",
    database: str = "thsetl",
) -> list[str]:
    """Refresh ACTIVE_CONCEPTS from the remote THS database.

    Queries concept_change_record for concepts that are currently active
    (added but not negated, or negated then re-activated).
    Updates the module-level ACTIVE_CONCEPTS list in place.

    Returns the refreshed list, or the existing snapshot on failure.
    """
    global ACTIVE_CONCEPTS
    try:
        import pymysql
        conn = pymysql.connect(
            host=host, port=port, user=user, password=password,
            database=database, connect_timeout=5,
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.concept_id, a.concept_name
            FROM concept_change_record a
            WHERE a.change_type = 'conceptAdd'
              AND a.concept_id NOT IN (
                SELECT n.concept_id
                FROM concept_change_record n
                WHERE n.change_type = 'conceptNegation'
                  AND NOT EXISTS (
                    SELECT 1 FROM concept_change_record e
                    WHERE e.concept_id = n.concept_id
                      AND e.change_type = 'conceptEffective'
                      AND e.change_time > n.change_time
                  )
              )
            ORDER BY a.concept_id
        """)
        seen: dict[int, str] = {}
        for row in cursor.fetchall():
            seen[int(row[0])] = row[1]
        conn.close()

        if seen:
            ACTIVE_CONCEPTS = list(seen.values())
            logger.info("[Tags] Refreshed %d active concepts from THS DB", len(ACTIVE_CONCEPTS))
        else:
            logger.warning("[Tags] Query returned 0 concepts, keeping snapshot")
    except Exception as e:
        logger.warning("[Tags] Failed to refresh concepts from THS DB: %s — using snapshot", e)

    return ACTIVE_CONCEPTS
