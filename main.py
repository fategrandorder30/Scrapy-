from fastapi import FastAPI, Body
from pydantic import BaseModel
import json
import os
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 在生产环境中应指定具体的前端域名
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
    
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)