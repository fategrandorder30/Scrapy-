import json
import logging
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
        'FEED_FORMAT': 'json',
        'FEED_URI': 'policies.json',
        'FEED_EXPORT_ENCODING': 'utf-8',
        'LOG_LEVEL': 'DEBUG',
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }
    }

    def start_requests(self):
        with open('config.json', encoding='utf-8') as f:
            cfg = json.load(f)[0]
        self.site_name = cfg['name']
        self.selectors = cfg['selectors']
        start_url = cfg['url']
        self.logger.info(f" 开始抓取: {self.site_name} → {start_url}")
        yield SplashRequest(
            url=start_url,
            callback=self.parse_list,
            args={
                'wait': 3,
                'render_all': 1
            },
            dont_filter=True
        )

    def parse_list(self, response):
        self.logger.info(f" 解析列表页: {response.url}")
        titles = response.xpath(self.selectors['title']).getall()
        links  = response.xpath(self.selectors['link']).getall()
        self.logger.info(f" 列表页共找到 {len(titles)} 条标题，{len(links)} 条链接")
        if not titles or not links:
            self.logger.warning(f" 在 {response.url} 未找到标题或链接，请检查 XPath 选择器或页面加载问题。")
        for title, href in zip(titles, links):
            title = title.strip()
            detail_url = response.urljoin(href)
            self.logger.info(f" 准备抓取详情: {title} → {detail_url}")
            yield SplashRequest(
                url=detail_url,
                callback=self.parse_detail,
                meta={'title': title},
                args={'wait': 1},
            )
        next_href = response.xpath(self.selectors['next_page']).get()
        if next_href:
            next_url = response.urljoin(next_href)
            self.logger.info(f" 跟进下一页: {next_url}")
            yield SplashRequest(
                url=next_url,
                callback=self.parse_list,
                args={'wait': 3},
            )
        else:
            self.logger.info(" 没有找到下一页，列表解析结束。")

    def parse_detail(self, response):
        title = response.meta['title']
        self.logger.info(f" 解析详情页: {title} → {response.url}")
        content_list = []
        for selector in self.selectors["content"]:
            paras = response.xpath(selector).getall()
            content = "\n".join(p.strip() for p in paras if p.strip())
            content_list.append(content)
        self.logger.info(f" 已抓取《{title}》")
        yield {
            'site':    self.site_name,
            'title':   title,
            'url':     response.url,
            'content': content_list,
        }