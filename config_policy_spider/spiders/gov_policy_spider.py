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
        self.regex_replacements = cfg.get('regex_replacements', {})
        # 处理新的字典格式的content规则
        if 'content' in self.regex_replacements:
            if isinstance(self.regex_replacements['content'], dict):
                # 如果是字典格式，保持不变
                pass
            elif not isinstance(self.regex_replacements['content'], list):
                # 如果不是字典也不是列表，转换为字典
                self.regex_replacements['content'] = {'default': self.regex_replacements['content']}
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
        content_dict = {}
        
        # 处理字典格式的content选择器
        if isinstance(self.selectors["content"], dict):
            for field_name, selector in self.selectors["content"].items():
                paras = response.xpath(selector).getall()
                content = "\n".join(p.strip() for p in paras if p.strip())
                modified_content = self.apply_regex_replacement('content', content, field_name=field_name)
                content_dict[field_name] = modified_content
                self.logger.info(f" 提取字段 [{field_name}]: {len(content)} 字符")
        else:
            # 兼容旧的列表格式
            self.logger.warning("检测到旧的列表格式content选择器，建议升级为字典格式")
            content_list = []
            for i, selector in enumerate(self.selectors["content"]):
                paras = response.xpath(selector).getall()
                content = "\n".join(p.strip() for p in paras if p.strip())
                modified_content = self.apply_regex_replacement('content', content, index=i)
                content_list.append(modified_content)
            content_dict = {'content': content_list}
        
        self.logger.info(f" 已抓取《{title}》，共提取 {len(content_dict)} 个内容字段")
        yield {
            'site':    self.site_name,
            'title':   title,
            'url':     response.url,
            'content': content_dict,
        }
    
    def apply_regex_replacement(self, field_type, text, index=None, field_name=None):
        if not text:
            return text
        if field_type not in self.regex_replacements:
            return text
        
        replacements = self.regex_replacements[field_type]
        field_replacements = []
        
        if field_type == "title":
            if isinstance(replacements, list):
                field_replacements = replacements
            else:
                self.logger.warning(f"title规则格式错误，应为列表，实际为: {type(replacements)}")
        elif field_type == "content":
            if isinstance(replacements, dict):
                # 新的字典格式
                if field_name and field_name in replacements:
                    content_rules = replacements[field_name]
                    field_replacements = content_rules if isinstance(content_rules, list) else []
                else:
                    self.logger.debug(f"content字段 '{field_name}' 没有对应的正则替换规则")
            elif isinstance(replacements, list):
                # 兼容旧的列表格式
                if index is not None and index < len(replacements):
                    content_rules = replacements[index]
                    field_replacements = content_rules if isinstance(content_rules, list) else []
                else:
                    self.logger.warning(f"content索引 {index} 越界，规则总数: {len(replacements)}")
            else:
                self.logger.warning(f"content规则格式错误，应为字典或列表，实际为: {type(replacements)}")
        
        for rule in field_replacements:
            if isinstance(rule, list) and len(rule) == 2:
                pattern, repl = rule
                try:
                    text = re.sub(pattern, repl, text, flags=re.UNICODE)
                    self.logger.info(f"[{field_type}:{field_name or index}] 替换: {pattern} → {repl}")
                except re.error as e:
                    self.logger.error(f"[{field_type}:{field_name or index}] 正则错误: {e}，模式: {pattern}")
            else:
                self.logger.warning(f"[{field_type}:{field_name or index}] 无效规则: {rule}，需为 [pattern, repl]")
        
        return text