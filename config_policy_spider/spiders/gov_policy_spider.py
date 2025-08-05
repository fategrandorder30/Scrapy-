import json
import logging
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
        # 从配置获取正则替换规则
        self.regex_replacements = cfg.get('regex_replacements', {})
        # 确保content的正则规则是列表，与selectors对应
        if 'content' in self.regex_replacements and not isinstance(self.regex_replacements['content'], list):
            self.regex_replacements['content'] = [self.regex_replacements['content']]
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
            # 对标题应用正则修改
            title = self.apply_regex_replacement('title', title)
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
        # 遍历每个content选择器
        for i, selector in enumerate(self.selectors["content"]):
            paras = response.xpath(selector).getall()
            content = "\n".join(p.strip() for p in paras if p.strip())
            
            # 对每个content应用对应的正则修改（按索引对应）
            modified_content = self.apply_regex_replacement('content', content, index=i)
            content_list.append(modified_content)
        
        self.logger.info(f" 已抓取《{title}》，共提取 {len(content_list)} 个内容块")
        yield {
            'site':    self.site_name,
            'title':   title,
            'url':     response.url,
            'content': content_list,
        }
    
    def apply_regex_replacement(self, field_name, text, index=None):
        """区分title和content的规则层级，修复解包错误"""
        if not text:
            return text
            
        # 字段没有配置规则，直接返回原始文本
        if field_name not in self.regex_replacements:
            return text
            
        replacements = self.regex_replacements[field_name]
        field_replacements = []  # 存储待应用的替换规则

        # 1. 处理title字段（单层规则列表，无需嵌套）
        if field_name == "title":
            # title的规则应为：[[p1, r1], [p2, r2]]（外层列表直接包含规则）
            if isinstance(replacements, list):
                # 直接使用整个列表作为规则（无需取索引）
                field_replacements = replacements
            else:
                self.logger.warning(f"title规则格式错误，应为列表，实际为: {type(replacements)}")

        # 2. 处理content字段（多层嵌套列表，需按索引取规则）
        elif field_name == "content":
            if isinstance(replacements, list):
                # 按索引取对应content的规则（每个content的规则是嵌套列表）
                if index is not None and index < len(replacements):
                    # content的规则应为：[[[p1, r1]], [[p2, r2]]]
                    content_rules = replacements[index]
                    field_replacements = content_rules if isinstance(content_rules, list) else []
                else:
                    self.logger.warning(f"content索引 {index} 越界，规则总数: {len(replacements)}")
            else:
                self.logger.warning(f"content规则格式错误，应为列表，实际为: {type(replacements)}")

        # 应用替换规则（统一校验格式）
        for rule in field_replacements:
            # 每个规则必须是二元列表 [pattern, repl]
            if isinstance(rule, list) and len(rule) == 2:
                pattern, repl = rule
                try:
                    text = re.sub(pattern, repl, text, flags=re.UNICODE)
                    self.logger.info(f"[{field_name}] 替换: {pattern} → {repl}")
                except re.error as e:
                    self.logger.error(f"[{field_name}] 正则错误: {e}，模式: {pattern}")
            else:
                self.logger.warning(f"[{field_name}] 无效规则: {rule}，需为 [pattern, repl]")

        return text
    