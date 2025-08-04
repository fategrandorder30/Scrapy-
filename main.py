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
from typing import List, Optional, Dict

app = FastAPI()

# 使用字典管理多个爬虫实例，key为pid，value为包含process和queue的字典
scrapy_instances: Dict[int, Dict] = {}

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

class AddressData(BaseModel):
    address: Optional[str] = None

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
    """读取爬虫输出并放入队列的函数"""
    while process.poll() is None:
        line = process.stdout.readline()
        if not line:
            break
        pattern = r"\[gov_policy\]"
        if re.search(pattern, line):
            queue.put(line)
    
    # 处理剩余输出
    remaining_output = process.stdout.read() if process.stdout else ""
    if remaining_output:
        for line in remaining_output.splitlines(keepends=True):
            if re.search(pattern, line):
                queue.put(line)
    
    # 处理错误情况
    if process.returncode != 0:
        queue.put(f"\n命令执行失败，返回码：{process.returncode}")
    
    # 移除已结束的实例
    pid = process.pid
    if pid in scrapy_instances:
        del scrapy_instances[pid]

async def run_scrapy_command(pid: int, address = None):
    """运行爬虫命令并流式输出结果"""
    while True:
        try:
            # 获取当前实例的队列
            queue = scrapy_instances[pid]["queue"]
            line = queue.get(timeout=0.5)
            yield line
        except:
            # 检查进程是否还在运行
            if pid not in scrapy_instances or scrapy_instances[pid]["process"].poll() is not None:
                break
            await asyncio.sleep(0.1)

@app.post("/start_scrapy")
async def start_scrapy(address: AddressData = Body(...)):
    """启动新的爬虫实例并返回其PID"""
    try:
        # 准备命令
        cmd = ["scrapy", "crawl", "gov_policy"]
        if address.address:
            cmd.extend(["-o", address.address])
        
        # 启动新进程
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        # 创建输出队列
        output_queue = Queue()
        
        # 存储实例信息
        pid = process.pid
        scrapy_instances[pid] = {
            "process": process,
            "queue": output_queue
        }
        
        # 启动输出读取线程
        t = threading.Thread(
            target=scrapy_output_reader, 
            args=(process, output_queue)
        )
        t.daemon = True
        t.start()
        
        # 返回PID和流式响应
        return {
            "status": "success",
            "pid": pid,
            "stream_url": f"/stream_scrapy?pid={pid}",  # 提供流式输出的URL
            "message": "爬虫已启动"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动失败：{str(e)}")

@app.get("/stream_scrapy")
async def stream_scrapy(pid: int):
    """根据PID获取指定爬虫实例的输出流"""
    if pid not in scrapy_instances:
        raise HTTPException(status_code=404, detail="爬虫实例不存在或已结束")
    
    return StreamingResponse(
        run_scrapy_command(pid),
        media_type="text/plain"
    )

@app.post("/pause_scrapy")
async def pause_scrapy(req: ProcessRequest):
    """暂停指定PID的爬虫实例"""
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    
    try:
        p = psutil.Process(pid)
        children = p.children(recursive=True)
        # 先暂停子进程
        for child in children:
            child.suspend()
        # 再暂停主进程
        p.suspend()
        return {"status": "success", "message": f"已暂停 PID: {pid} 的爬虫进程"}
    except Exception as e:
        return {"status": "error", "message": f"暂停失败: {str(e)}"}

@app.post("/resume_scrapy")
async def resume_scrapy(req: ProcessRequest):
    """恢复指定PID的爬虫实例"""
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    
    try:
        p = psutil.Process(pid)
        children = p.children(recursive=True)
        # 先恢复主进程
        p.resume()
        # 再恢复子进程
        for child in children:
            child.resume()
        return {"status": "success", "message": f"已恢复 PID: {pid} 的爬虫进程"}
    except Exception as e:
        return {"status": "error", "message": f"恢复失败: {str(e)}"}

@app.post("/stop_scrapy")
async def stop_scrapy(req: ProcessRequest):
    """停止指定PID的爬虫实例"""
    pid = req.pid
    if pid not in scrapy_instances:
        return {"status": "error", "message": "爬虫实例不存在或已结束"}
    
    try:
        p = psutil.Process(pid)
        children = p.children(recursive=True)
        # 终止所有子进程
        for child in children:
            try:
                child.terminate()
            except psutil.NoSuchProcess:
                pass
        
        # 终止主进程
        p.terminate()
        
        # 从实例列表中移除
        if pid in scrapy_instances:
            del scrapy_instances[pid]
            
        return {"status": "success", "message": f"已终止 PID: {pid} 的爬虫进程"}
    except psutil.NoSuchProcess:
        # 进程已经不存在，清理状态
        if pid in scrapy_instances:
            del scrapy_instances[pid]
        return {"status": "success", "message": f"PID: {pid} 的进程已经不存在，已清理状态"}
    except Exception as e:
        return {"status": "error", "message": f"终止失败: {str(e)}"}

@app.get("/list_instances")
async def list_instances():
    """列出所有活跃的爬虫实例"""
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
            # 清理已不存在的进程
            del scrapy_instances[pid]
    
    return {
        "status": "success",
        "count": len(instances),
        "instances": instances
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
