import json
import re
import scrapy
from scrapy_splash import SplashRequest

class GovPolicySpider(scrapy.Spider):
    name = "gov_policy"
    custom_settings = {
        'SPLASH_URL': 'http://localhost:8050',
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy_splash.SplashCookiesMiddleware': 723,
            'scrapy_splash.SplashMiddleware': 725,
            'scrapy.downloadermiddlewares.httpcompression.HttpCompressionMiddleware': 810,
        },
        'SPIDER_MIDDLEWARES': {
            'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
        },
        'DUPEFILTER_CLASS': 'scrapy_splash.SplashAwareDupeFilter',
        'HTTPCACHE_STORAGE': 'scrapy_splash.SplashAwareFSCacheStorage',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'INFO',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }
    }

    def start_requests(self):
        with open('config.json', encoding='utf-8') as f:
            cfg_list = json.load(f)
        # 支持顺序爬取多个网站
        if not isinstance(cfg_list, list):
            self.logger.error("config.json 格式错误，需为列表包裹多个配置")
            return
        for cfg in cfg_list:
            site_name = cfg['name']
            start_url = cfg['url']
            selectors = cfg['selectors']
            regex_replacements = cfg.get('regex_replacements', {})
            self.logger.info(f" 开始抓取: {site_name} → {start_url}")
            # 将配置保存到实例变量，供后续解析使用
            meta = {
                'site_name': site_name,
                'selectors': selectors,
                'regex_replacements': regex_replacements
            }
            yield SplashRequest(
                url=start_url,
                callback=self.parse_list,
                meta=meta,
                args={
                    'wait': 3,
                    'render_all': 1
                },
                dont_filter=True
            )

    def parse_list(self, response):
        meta = response.meta
        site_name = meta['site_name']
        selectors = meta['selectors']
        regex_replacements = meta['regex_replacements']
        self.logger.info(f" 解析列表页: {response.url}")
        titles = response.xpath(selectors['title']).getall()
        links  = response.xpath(selectors['link']).getall()
        self.logger.info(f" 列表页共找到 {len(titles)} 条标题，{len(links)} 条链接")
        if not titles or not links:
            self.logger.warning(f" 在 {response.url} 未找到标题或链接，请检查 XPath 选择器或页面加载问题。")
        for idx, (title, href) in enumerate(zip(titles, links)):
            title = title.strip()
            title = self.apply_regex_replacement('title', title, regex_replacements)
            detail_url = response.urljoin(href)
            self.logger.info(f" 准备抓取详情: {title} → {detail_url}")
            detail_meta = {
                'title': title,
                'site_name': site_name,
                'selectors': selectors,
                'regex_replacements': regex_replacements
            }
            yield SplashRequest(
                url=detail_url,
                callback=self.parse_detail,
                meta=detail_meta,
                args={'wait': 1},
            )
        next_href = response.xpath(selectors['next_page']).get()
        if next_href:
            next_url = response.urljoin(next_href)
            self.logger.info(f" 跟进下一页: {next_url}")
            yield SplashRequest(
                url=next_url,
                callback=self.parse_list,
                meta=meta,
                args={'wait': 3},
            )
        else:
            self.logger.info(" 没有找到下一页，列表解析结束。")

    def parse_detail(self, response):
        meta = response.meta
        title = meta['title']
        site_name = meta['site_name']
        selectors = meta['selectors']
        regex_replacements = meta['regex_replacements']
        self.logger.info(f" 解析详情页: {title} → {response.url}")
        content_dict = {}
        if isinstance(selectors["content"], dict):
            for idx, (key, value) in enumerate(selectors["content"].items()):
                paras = response.xpath(value).getall()
                content = "\n".join(p.strip() for p in paras if p.strip())
                modified_content = self.apply_regex_replacement('content', content, regex_replacements, index=idx)
                content_dict[key] = modified_content
        yield {
            'site':    site_name,
            'title':   title,
            'url':     response.url,
            'content': content_dict,
        }

    def apply_regex_replacement(self, field_type, text, regex_replacements, index=None):
        if not text:
            return text
        if field_type not in regex_replacements:
            return text
        replacements = regex_replacements[field_type]
        field_replacements = []
        if field_type == "title":
            if isinstance(replacements, list):
                field_replacements = replacements
        elif field_type == "content":
            if isinstance(replacements, list):
                if index is not None and index < len(replacements):
                    content_rules = replacements[index]
                    field_replacements = content_rules
        for rule in field_replacements:
            if isinstance(rule, list) and len(rule) == 2:
                pattern, repl = rule
                try:
                    text = re.sub(pattern, repl, text, flags=re.UNICODE)
                except re.error as e:
                    self.logger.error(f"[{field_type}:{index}] 正则错误: {e}，模式: {pattern}")
            else:
                self.logger.warning(f"[{field_type}:{index}] 无效规则: {rule}，需为 [pattern, repl]")
        return text