import time
import requests
import json
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

class DataCollector:
    """数据采集层：支持列表层查询与详情层查询的双级架构"""
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Referer': 'https://www.sse.com.cn/'
        }
        self.list_url = 'https://query.sse.com.cn/commonSoaQuery.do'
        self.detail_url = 'https://query.sse.com.cn/commonSoaQuery.do'

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((requests.exceptions.RequestException, ConnectionError)),
        before_sleep=lambda retry_state: logger.warning(
            f"[Collector] 网络请求失败，准备第 {retry_state.attempt_number} 次重试. 异常: {retry_state.outcome.exception()}"
        )
    )
    def _fetch(self, url, params):
        response = requests.get(url, params=params, headers=self.headers, timeout=(5, 15))
        
        if response.status_code != 200:
            logger.error(f"[Collector] 接口请求异常，HTTP 状态码: {response.status_code}")
            raise ConnectionError(f"HTTP Status {response.status_code}")
            
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            logger.critical(f"[Collector] 接口返回数据格式异动，非合法JSON格式！疑似被阻断拦截。")
            raise ValueError("Data structure mutated. Anti-scraping triggered.") from e
            
        return data

    def fetch_index_list(self):
        """Stage 1: 获取全盘 IPO 排队企业目录"""
        logger.info(f"[Collector] [Stage 1] 抓取科创板/主板 IPO 名单索引...")
        params = {
            'sqlId': 'SH_XM_LB',
            'issueMarketType': '1,2',
            'isPagination': 'false' 
        }
        raw_data = self._fetch(self.list_url, params)
        # 上交所如果 isPagination 为 false 时可能会有不同的结构，
        # 测试中发现不带分页可能会取不到或者全量，如果接口不支持全量，我们可以手动改为抓前 100 条做阶段测试。
        # 稳妥起见我们模拟 pagination=true 的测试环境取第一页数据
        params['isPagination'] = 'true'
        params['pageHelp.pageSize'] = 200
        params['pageHelp.pageNo'] = 1
        raw_data = self._fetch(self.list_url, params)
        
        return raw_data

    def fetch_timeline_detail(self, audit_id):
        """Stage 2: 下钻获取指定 AuditID 的时间轴详情"""
        logger.debug(f"[Collector] [Stage 2] 获取 {audit_id} 时间轴详情...")
        params = {
            'sqlId': 'GP_GPZCZ_XMDTZTYYLB',
            'stockAuditNum': audit_id,
            'order': 'qianDate|desc',
            'isPagination': 'false'
        }
        return self._fetch(self.detail_url, params)
