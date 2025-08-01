import json
import logging

import scrapy
from scrapy_splash import SplashRequest


class GovPolicySpider(scrapy.Spider):
    name = "gov_policy"
    custom_settings = {
        # Splash 服务 URL，请确保你的 Splash 服务正在运行
        'SPLASH_URL': 'http://localhost:8050',
        # 必要的 Splash 中间件配置
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
        # 输出 JSON 文件配置
        'FEED_FORMAT': 'json',
        'FEED_URI': 'policies.json',
        'FEED_EXPORT_ENCODING': 'utf-8',
        # 日志级别 (DEBUG 会输出详细信息，便于调试)
        'LOG_LEVEL': 'INFO',
        # 设置默认请求头，模拟浏览器访问
        'DEFAULT_REQUEST_HEADERS': {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36',
        }
    }

    def start_requests(self):
        # 读取配置
        with open('config.json', encoding='utf-8') as f:
            cfg = json.load(f)[0] # 假设 config.json 始终是一个包含单个配置对象的列表
        self.site_name = cfg['name']
        self.selectors = cfg['selectors']
        start_url = cfg['url']

        self.logger.info(f" 开始抓取: {self.site_name} → {start_url}")
        yield SplashRequest(
            url=start_url,
            callback=self.parse_list,
            args={
                'wait': 3,  # **增加等待时间**，确保页面内容加载完成
                'render_all': 1 # 尝试渲染所有资源，确保所有JS内容加载
            },
            dont_filter=True
        )

    def parse_list(self, response):
        self.logger.info(f" 解析列表页: {response.url}")

        # **调试步骤：将 Splash 渲染的 HTML 内容保存到本地文件**
        # 运行爬虫后，打开 'debug_list_page.html' 文件，在浏览器开发者工具中验证你的 XPath
        # with open('debug_list_page.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        # self.logger.info("已将列表页响应内容保存到 'debug_list_page.html' 进行调试。")


        # 获取标题与链接
        # 针对广东省人民政府网站的特点，可以尝试更具体的 XPath
        # 例如，检查 `zwgk_list` div 下是否直接是 `ul/li/a` 结构
        titles = response.xpath(self.selectors['title']).getall()
        links  = response.xpath(self.selectors['link']).getall()

        self.logger.info(f" 列表页共找到 {len(titles)} 条标题，{len(links)} 条链接")

        # 检查是否获取到内容
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
                args={'wait': 1}, # 详情页通常也需要等待
            )

        # 翻页处理
        # 检查 '下一页' 链接的 XPath 是否准确
        # 有时 '下一页' 文本可能被包裹在其他标签中，或者有额外的空格
        next_href = response.xpath(self.selectors['next_page']).get()
        if next_href:
            next_url = response.urljoin(next_href)
            self.logger.info(f" 跟进下一页: {next_url}")
            yield SplashRequest(
                url=next_url,
                callback=self.parse_list,
                args={'wait': 3}, # 翻页同样需要等待
            )
        else:
            self.logger.info(" 没有找到下一页，列表解析结束。")

    def parse_detail(self, response):
        title = response.meta['title']
        self.logger.info(f" 解析详情页: {title} → {response.url}")

        # **调试步骤：将详情页响应内容保存到本地文件**
        # 运行爬虫后，打开 'debug_detail_page.html' 文件，验证你的内容 XPath
        # with open('debug_detail_page.html', 'w', encoding='utf-8') as f:
        #     f.write(response.text)
        # self.logger.info(f"已将详情页响应内容保存到 'debug_detail_page.html' 进行调试。")

        # 提取正文段落
        paras = response.xpath(self.selectors['content']).getall()
        content = "\n".join(p.strip() for p in paras if p.strip())
        self.logger.info(f" 已抓取《{title}》，正文共 {len(content)} 字符")

        yield {
            'site':    self.site_name,
            'title':   title,
            'url':     response.url,
            'content': content,
        }