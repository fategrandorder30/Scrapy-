# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_batch
import logging


class ConfigPolicySpiderPipeline:
    def process_item(self, item, spider):
        return item


class PostgreSQLPipeline:
    def __init__(self, postgres_settings):
        self.postgres_settings = postgres_settings
        self.connection = None
        self.batch_data = []
        self.batch_size = 10
        self.table_validated = False
        self.can_write = False  # 控制是否允许写入
        self.expected_columns = None  # 期望的列结构
        
    @classmethod
    def from_crawler(cls, crawler):
        postgres_settings = crawler.settings.get("POSTGRES_SETTINGS")
        if not postgres_settings:
            raise ValueError("POSTGRES_SETTINGS not found in spider settings")
        return cls(postgres_settings)
    
    def open_spider(self, spider):
        """爬虫开始时创建数据库连接"""
        try:
            self.connection = psycopg2.connect(
                database=self.postgres_settings['dbname'],
                user=self.postgres_settings['user'],
                password=self.postgres_settings['password'],
                host=self.postgres_settings['host'],
                port=self.postgres_settings['port']
            )
            spider.logger.info("PostgreSQL 连接成功")
        except OperationalError as e:
            spider.logger.error(f"PostgreSQL 连接失败: {e}")
            raise
    
    def close_spider(self, spider):
        """爬虫结束时批量插入剩余数据并关闭连接"""
        if self.batch_data and self.can_write:
            self._insert_batch(spider)
        
        if self.connection:
            self.connection.close()
            spider.logger.info("PostgreSQL 连接已关闭")
    
    def process_item(self, item, spider):
        """处理每个item，累积批量数据"""
        if not self.connection:
            spider.logger.error("PostgreSQL 连接不存在")
            return item
            
        # 验证表结构
        if not self.table_validated:
            self.can_write = self._validate_table_structure(item, spider)
            self.table_validated = True
            
            if not self.can_write:
                spider.logger.error("表结构验证失败，停止数据写入")
                return item
        
        # 只有验证通过才允许写入
        if self.can_write:
            # 添加到批处理列表
            self.batch_data.append(dict(item))
            
            # 达到批处理大小时执行插入
            if len(self.batch_data) >= self.batch_size:
                self._insert_batch(spider)
        else:
            spider.logger.warning("由于表结构不匹配，跳过数据写入")
        
        return item
    
    def _validate_table_structure(self, item, spider):
        """验证表结构是否与item匹配"""
        table_name = self.postgres_settings['table']
        cursor = self.connection.cursor()
        
        try:
            # 检查表是否存在
            cursor.execute("""
                SELECT EXISTS(
                    SELECT * FROM information_schema.tables 
                    WHERE table_name = %s
                )
            """, (table_name,))
            
            table_exists = cursor.fetchone()[0]
            
            # 准备期望的列结构（清理字段名）
            item_columns = set()
            for field_name in item.keys():
                clean_field = field_name.replace(' ', '_').replace('-', '_').replace('.', '_')
                item_columns.add(clean_field)
            
            self.expected_columns = sorted(list(item_columns))
            
            if not table_exists:
                # 表不存在，创建新表
                columns = []
                for clean_field in self.expected_columns:
                    columns.append(f"{clean_field} TEXT")
                
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    {', '.join(columns)}
                );
                """
                
                cursor.execute(create_table_sql)
                self.connection.commit()
                spider.logger.info(f"表 {table_name} 创建成功，列: {self.expected_columns}")
                return True
            
            else:
                # 表存在，验证列结构
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND column_name NOT IN ('id', 'created_at')
                    ORDER BY ordinal_position
                """, (table_name,))
                
                existing_columns = [row[0] for row in cursor.fetchall()]
                existing_columns_set = set(existing_columns)
                
                spider.logger.info(f"表 {table_name} 现有列 ({len(existing_columns)}): {existing_columns}")
                spider.logger.info(f"期望的列 ({len(self.expected_columns)}): {self.expected_columns}")
                
                # 严格比较列结构
                if existing_columns_set == item_columns:
                    spider.logger.info("✅ 表结构验证通过：列数和列名完全匹配")
                    return True
                else:
                    spider.logger.error("❌ 表结构验证失败：列数或列名不匹配")
                    
                    # 详细报告差异
                    missing_in_table = item_columns - existing_columns_set
                    extra_in_table = existing_columns_set - item_columns
                    
                    if missing_in_table:
                        spider.logger.error(f"表中缺少的列: {sorted(list(missing_in_table))}")
                    if extra_in_table:
                        spider.logger.error(f"表中多余的列: {sorted(list(extra_in_table))}")
                    
                    spider.logger.error("请确保表结构与爬取数据的字段完全一致，或删除现有表让程序重新创建")
                    return False
                
        except Exception as e:
            spider.logger.error(f"验证表结构时出错: {e}")
            self.connection.rollback()
            return False
        finally:
            cursor.close()
    
    def _insert_batch(self, spider):
        """批量插入数据到PostgreSQL"""
        if not self.batch_data or not self.can_write:
            return
            
        table_name = self.postgres_settings['table']
        cursor = self.connection.cursor()
        
        try:
            # 使用期望的列顺序构建SQL
            columns_str = ', '.join(self.expected_columns)
            placeholders = ', '.join(['%s'] * len(self.expected_columns))
            
            insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 准备批量数据
            batch_values = []
            for item_data in self.batch_data:
                # 按照期望列的顺序准备数据
                row_values = []
                for col in self.expected_columns:
                    # 查找原始字段名
                    original_field = None
                    for field in item_data.keys():
                        clean_field = field.replace(' ', '_').replace('-', '_').replace('.', '_')
                        if clean_field == col:
                            original_field = field
                            break
                    
                    if original_field:
                        value = item_data.get(original_field, '')
                    else:
                        value = ''
                    
                    # 处理可能的None值
                    if value is None:
                        value = ''
                    row_values.append(str(value))
                
                batch_values.append(tuple(row_values))
            
            # 批量执行插入
            execute_batch(cursor, insert_sql, batch_values, page_size=self.batch_size)
            self.connection.commit()
            
            spider.logger.info(f"成功插入 {len(self.batch_data)} 条数据到 {table_name}")
            
            # 清空批处理列表
            self.batch_data.clear()
            
        except Exception as e:
            spider.logger.error(f"批量插入数据时出错: {e}")
            spider.logger.error(f"SQL: {insert_sql}")
            spider.logger.error(f"期望列数: {len(self.expected_columns)}, 实际数据列数: {len(batch_values[0]) if batch_values else 0}")
            self.connection.rollback()
            # 不抛出异常，避免中断爬虫
        finally:
            cursor.close()
