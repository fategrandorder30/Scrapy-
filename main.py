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

app = FastAPI()

scrapy_instances: Dict[int, Dict] = {}

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    if data.regex_replacements:
        data_entry["regex_replacements"] = data.regex_replacements
    filename = "./config.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data_entry, f, ensure_ascii=False, indent=4)
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
