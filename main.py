from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
import json
import os
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import subprocess
import asyncio
import sys
import re
import psutil
import threading
from queue import Queue
from typing import List

app = FastAPI()

scrapy_pid = None
scrapy_output_queue = Queue()
scrapy_process = None

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class FormData(BaseModel):
    name: str
    url: str
    title: str
    link: str
    content: List[str]
    next_page: str

@app.post("/submit_form")
async def submit_form(data: FormData = Body(...)):
    data_entry = {
        "name": data.name,
        "url": data.url,
        "selectors": {
            "title": data.title,
            "link": data.link,
            "content": data.content,
            "next_page": data.next_page
        }
    }
    filename = "./config.json"
    data = [data_entry]
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    return {
        "status": "success",
        "message": "JSON数据已成功保存",
        "data": data_entry
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
    if process.returncode != 0:
        queue.put(f"\n命令执行失败，返回码：{process.returncode}")

async def run_scrapy_command():
    global scrapy_pid, scrapy_process, scrapy_output_queue
    scrapy_output_queue = Queue()
    scrapy_process = subprocess.Popen(
        ["scrapy", "crawl", "gov_policy"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    scrapy_pid = scrapy_process.pid
    t = threading.Thread(target=scrapy_output_reader, args=(scrapy_process, scrapy_output_queue))
    t.daemon = True
    t.start()
    while t.is_alive() or not scrapy_output_queue.empty():
        try:
            line = scrapy_output_queue.get(timeout=0.5)
            yield line
        except:
            await asyncio.sleep(0.1)

@app.post("/start_scrapy")
async def start_scrapy():
    try:
        return StreamingResponse(
            run_scrapy_command(),
            media_type="text/plain"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动失败：{str(e)}")
    
@app.post("/pause_scrapy")
async def pause_scrapy():
    global scrapy_pid
    if scrapy_pid:
        try:
            p = psutil.Process(scrapy_pid)
            children = p.children(recursive=True)
            for child in children:
                child.suspend()
            p.suspend()
            return {"status": "success",
                    "message": "已暂停 Scrapy 进程"}
        except Exception as e:
            return {"status": "error",
                    "message": "暂停失败"}
    else:
        return {"status": "error",
                "message": "进程未启动"}
    
@app.post("/resume_scrapy")
async def resume_scrapy():
    global scrapy_pid
    if scrapy_pid:
        try:
            p = psutil.Process(scrapy_pid)
            children = p.children(recursive=True)
            for child in children:
                child.resume()
            p.resume()
            return {"status": "success",
                    "message": "已恢复 Scrapy 进程"}
        except Exception as e:
            return {"status": "error",
                    "message": "恢复失败"}
    else:
        return {"status": "error",
                "message": "进程未启动"}

@app.post("/stop_scrapy")
async def stop_scrapy():
    global scrapy_pid, scrapy_process
    if scrapy_pid:
        try:
            p = psutil.Process(scrapy_pid)
            children = p.children(recursive=True)
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            p.terminate()
            scrapy_pid = None
            scrapy_process = None
            return {"status": "success",
                    "message": "已终止 Scrapy 进程"}
        except psutil.NoSuchProcess:
            scrapy_pid = None
            scrapy_process = None
            return {"status": "success",
                    "message": "进程已经不存在，已清理状态"}
        except Exception as e:
            return {"status": "error",
                    "message": f"终止失败: {str(e)}"}
    else:
        return {"status": "error",
                "message": "进程未启动"}
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)