from fastapi import FastAPI, Body, HTTPException
from pydantic import BaseModel
import json
import os
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import subprocess
import asyncio
import sys

app = FastAPI()

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
    content: str
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

async def run_scrapy_command():
    process = subprocess.Popen(
        "scrapy crawl gov_policy",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    while process.poll() is None:
        if process.stdout:
            line = process.stdout.readline()
            if line:
                yield line
                await asyncio.sleep(0.01)
    remaining_output = process.stdout.read() if process.stdout else ""
    if remaining_output:
        yield remaining_output
    if process.returncode != 0:
        yield f"\n命令执行失败，返回码：{process.returncode}"

@app.post("/start_scrapy")
async def start_scrapy():
    try:
        return StreamingResponse(
            run_scrapy_command(),
            media_type="text/plain"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"启动失败：{str(e)}")
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)