import psycopg2
import pandas as pd
from psycopg2 import OperationalError, sql
from psycopg2.extras import execute_batch
import ast

def create_db_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("数据库连接成功")
    except OperationalError as e:
        print(f"连接错误: {e}")
    return connection

def table_exists(connection, table_name):
    if connection is None:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name=%s)",
            (table_name,)
        )
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"检查表是否存在时出错: {e}")
        return False
    finally:
        cursor.close()

def get_table_column_count(connection, table_name):
    if connection is None:
        return -1
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name=%s AND column_name != 'id'
            """,
            (table_name,)
        )
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"获取表列数时出错: {e}")
        return -1
    finally:
        cursor.close()

def create_table(connection, table_name, df):
    if connection is None:
        print("没有有效的数据库连接")
        return False
    cursor = connection.cursor()
    columns = []
    for col in df.columns:
        clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_')
        columns.append(f"{clean_col} TEXT")
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join(columns)}
    );
    """
    try:
        cursor.execute(create_table_sql)
        connection.commit()
        print(f"表 {table_name} 创建成功")
        return True
    except Exception as e:
        print(f"创建表时出错: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

def expand_content_column(df):
    content_dicts = df['content'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else {})
    content_df = pd.DataFrame(content_dicts.tolist())
    df = pd.concat([df.drop(columns=['content']), content_df], axis=1)
    return df

def insert_csv_to_postgres(connection, table_name, csv_file_path, batch_size=1000):
    if connection is None:
        print("没有有效的数据库连接")
        return False
    try:
        df = pd.read_csv(csv_file_path)
        print(f"成功读取CSV文件，共 {len(df)} 行数据，{len(df.columns)} 列")
        if 'content' in df.columns:
            df = expand_content_column(df)
            print(f"展开 content 字段后，列变为: {df.columns.tolist()}")
    except Exception as e:
        print(f"读取CSV文件时出错: {e}")
        return False
    table_exist = table_exists(connection, table_name)
    if not table_exist:
        print(f"表 {table_name} 不存在，将创建新表")
        if create_table(connection, table_name, df):
            return insert_data(connection, table_name, df, batch_size)
        return False
    else:
        table_column_count = get_table_column_count(connection, table_name)
        csv_column_count = len(df.columns)
        
        if table_column_count == -1:
            print("无法获取表的列数信息")
            return False
        print(f"表 {table_name} 已存在，表列数: {table_column_count}, CSV列数: {csv_column_count}")
        if table_column_count != csv_column_count:
            print(f"错误: 表 {table_name} 列数与CSV文件列数不一致，添加失败")
            return False
        else:
            print(f"表 {table_name} 列数与CSV文件一致，将添加数据")
            return insert_data(connection, table_name, df, batch_size)

def insert_data(connection, table_name, df, batch_size):
    clean_columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]
    columns_str = ', '.join(clean_columns)
    placeholders = ', '.join(['%s'] * len(clean_columns))
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    cursor = connection.cursor()
    try:
        data = [tuple(row) for row in df.values]
        execute_batch(cursor, insert_sql, data, page_size=batch_size)
        connection.commit()
        print(f"成功插入 {len(df)} 行数据到表 {table_name}")
        return True
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

def main():
    db_config = {
        "db_name": "postgres",  # 替换为你的数据库名
        "db_user": "postgres",  # 替换为你的用户名
        "db_password": "My@1335600", # 替换为你的密码
        "db_host": "localhost",
        "db_port": "5432"
    }
    csv_file_path = "./data.csv"  # 替换为你的CSV文件路径
    table_name = "csv_data"     # 替换为你想要创建的表名
    connection = create_db_connection(** db_config)
    if connection:
        result = insert_csv_to_postgres(connection, table_name, csv_file_path)
        if result:
            print("操作成功完成")
        else:
            print("操作失败")
        connection.close()
        print("数据库连接已关闭")

if __name__ == "__main__":
    main()