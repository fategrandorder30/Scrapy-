from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
import json
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import subprocess
import asyncio
import re
import psutil
import threading
from queue import Queue
from typing import List, Optional, Dict, Any, Union
import signal
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_batch
import ast

app = FastAPI()

scrapy_instances: Dict[int, Dict] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ImportCSVRequest(BaseModel):
    db_name: str
    db_user: str
    db_password: str
    db_host: str
    db_port: str
    table_name: str
    csv_file_path: str

class RegexReplacementStep(BaseModel):
    pattern: str
    repl: str

class FieldReplacements(BaseModel):
    steps: List[RegexReplacementStep]

class FormData(BaseModel):
    name: str
    url: str
    title: str
    link: str
    content: Dict[str, str]
    next_page: str
    regex_replacements: Optional[dict[str, Any]] = None

class AddressData(BaseModel):
    address: str

class ProcessRequest(BaseModel):
    pid: int

def create_db_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database = db_name,
            user = db_user,
            password = db_password,
            host = db_host,
            port = db_port
        )
    except OperationalError as e:
        print(f"连接错误：{e}")
    return connection

def table_exists(connection, table_name):
    if connection is None:
        return False
    cursor = connection.cursor()
    try:
        cursor.execute(
            "SELECT EXISTS(SELECT * FROM information_schema.tables WHERE table_name = %s)",
            (table_name,)
        )
        return cursor.fetchone()[0]
    except Exception as e:
        print(f"检查表是否存在时出错：{e}")
        return False
    finally:
        cursor.close()

def get_table_column_count(connection, table_name):
    if connection is None:
        return -1
    cursor = connection.cursor
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
        return True
    except Exception as e:
        print(f"创建表时出错: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

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
        return True
    except Exception as e:
        print(f"插入数据时出错: {e}")
        connection.rollback()
        return False
    finally:
        cursor.close()

def insert_csv_to_postgres(connection, table_name, csv_file_path, batch_size=1000):
    if connection is None:
        return False, "数据库连接失败"
    
    cursor = connection.cursor()
    try:
        # 开始事务
        cursor.execute("BEGIN;")
        
        df = pd.read_csv(csv_file_path)
        table_exist = table_exists(connection, table_name)
        
        if not table_exist:
            if create_table(connection, table_name, df):
                result = insert_data(connection, table_name, df, batch_size)
                if result:
                    cursor.execute("COMMIT;")
                    return True, "表创建并插入数据成功"
                else:
                    cursor.execute("ROLLBACK;")
                    return False, "插入数据失败"
            else:
                cursor.execute("ROLLBACK;")
                return False, "创建表失败"
        else:
            result = insert_data(connection, table_name, df, batch_size)
            if result:
                cursor.execute("COMMIT;")
                return True, "插入数据成功"
            else:
                cursor.execute("ROLLBACK;")
                return False, "插入数据失败"
                
    except Exception as e:
        cursor.execute("ROLLBACK;")
        return False, f"操作失败: {e}"
    finally:
        cursor.close()
        
@app.post("/import_csv_to_postgres")
async def import_csv_to_postgres_api(req: ImportCSVRequest = Body(...)):
    connection = create_db_connection(
        req.db_name, req.db_user, req.db_password, req.db_host, req.db_port
    )
    if not connection:
        return {"status": "error", "message": "数据库连接失败"}
    result, msg = insert_csv_to_postgres(connection, req.table_name, req.csv_file_path)
    connection.close()
    if result:
        return {"status": "success", "message": msg}
    else:
        return {"status": "error", "message": msg}

@app.post("/submit_form")
async def submit_form(data: List[FormData] = Body(...)):
    config_list = []
    for item in data:
        entry = {
            "name": item.name,
            "url": item.url,
            "selectors": {
                "title": item.title,
                "link": item.link,
                "content": item.content,
                "next_page": item.next_page
            }
        }
        if item.regex_replacements:
            entry["regex_replacements"] = item.regex_replacements
        config_list.append(entry)
    filename = "./config.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(config_list, f, ensure_ascii=False, indent=4)
    return {
        "status": "success",
        "message": "JSON数据已成功保存",
        "data": config_list
    }

def scrapy_output_reader(process, queue):
    while process.poll() is None:
        line = process.stdout.readline()
        if not line:
            break
        pattern = r"\[gov_policy\]"
        if re.search(pattern, line):
            queue.put(line)
    remaining_output = process.stdout.read() if process.stdout else ""
    if remaining_output:
        for line in remaining_output.splitlines(keepends=True):
            if re.search(pattern, line):
                queue.put(line)
    pid = process.pid
    if pid in scrapy_instances:
        del scrapy_instances[pid]

async def run_scrapy_command(pid: int, address = None):
    while True:
        try:
            queue = scrapy_instances[pid]["queue"]
            line = queue.get(timeout=0.1)
            yield line
        except:
            if pid not in scrapy_instances or scrapy_instances[pid]["process"].poll() is not None:
                break
            await asyncio.sleep(0.1)

@app.post("/start_scrapy")
async def start_scrapy(address: AddressData = Body(...)):
    try:
        cmd = ["scrapy", "crawl", "gov_policy"]
        if address.address:
            cmd.extend(["-o", address.address])
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
        )
        output_queue = Queue()
        pid = process.pid
        scrapy_instances[pid] = {
            "process": process,
            "queue": output_queue
        }
        t = threading.Thread(
            target=scrapy_output_reader, 
            args=(process, output_queue)
        )
        t.daemon = True
        t.start()
        return {
            "status": "success",
            "pid": pid,
            "stream_url": f"/stream_scrapy?pid={pid}",
            "message": "爬虫已启动"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动失败：{str(e)}")

@app.get("/stream_scrapy")
async def stream_scrapy(pid: int):
    if pid not in scrapy_instances:
        raise HTTPException(status_code=404, detail="爬虫实例不存在或已结束")
    return StreamingResponse(
        run_scrapy_command(pid),
        media_type="text/plain"
    )

@app.post("/pause_scrapy")
async def pause_scrapy(req: ProcessRequest):
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    try:
        p = psutil.Process(pid)
        children = p.children(recursive=True)
        for child in children:
            child.suspend()
        p.suspend()
        return {"status": "success", "message": f"已暂停 PID: {pid} 的爬虫进程"}
    except Exception as e:
        return {"status": "error", "message": f"暂停失败: {str(e)}"}

@app.post("/resume_scrapy")
async def resume_scrapy(req: ProcessRequest):
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    try:
        p = psutil.Process(pid)
        children = p.children(recursive=True)
        p.resume()
        for child in children:
            child.resume()
        return {"status": "success", "message": f"已恢复 PID: {pid} 的爬虫进程"}
    except Exception as e:
        return {"status": "error", "message": f"恢复失败: {str(e)}"}

@app.post("/stop_scrapy")
async def stop_scrapy(req: ProcessRequest):
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    try:
        p = psutil.Process(pid)
        if hasattr(signal, "CTRL_BREAK_EVENT") and psutil.WINDOWS:
            p.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            p.send_signal(signal.SIGINT)
        return {"status": "success", "message": f"已终止 PID：{pid} 的爬虫进程"}
    except psutil.NoSuchProcess:
        if pid in scrapy_instances:
            del scrapy_instances[pid]
        return {"status": "success", "message": f"已终止 PID：{pid} 的爬虫进程"}
    except Exception as e:
        return {"status": "error", "message": f"终止失败：{str(e)}"}

@app.get("/list_instances")
async def list_instances():
    instances = []
    for pid, info in scrapy_instances.items():
        try:
            p = psutil.Process(pid)
            status = p.status()
            instances.append({
                "pid": pid,
                "status": status,
                "create_time": p.create_time()
            })
        except:
            del scrapy_instances[pid]
    return {
        "status": "success",
        "count": len(instances),
        "instances": instances
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
