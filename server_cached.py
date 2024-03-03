from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uuid
import aiohttp
from typing import List, Dict
import asyncio
import uvicorn
from redis.asyncio import Redis
from urllib.parse import urlparse

class Task(BaseModel):
    id: str
    status: str
    result: List[Dict[str, str]] = []

app = FastAPI()
tasks = {}

async def create_redis_connection():
    redis = Redis(host='localhost', port=6379, decode_responses=True)
    await redis.ping()
    return redis

CACHE_CLEANUP_TIMEOUT = 3600

async def cache_cleanup():
    redis = await create_redis_connection()
    while True:
        keys = await redis.keys('*')
        for key in keys:
            remaining_ttl = await redis.ttl(key)
            if remaining_ttl == -1:
                await redis.delete(key)

        await asyncio.sleep(CACHE_CLEANUP_TIMEOUT)
        await redis.close()

@app.on_event("startup")
async def on_startup():
    asyncio.create_task(cache_cleanup())        

async def fetch_url(session, url, redis):
    domain = urlparse(url).netloc
    cached_status = await redis.get(url)
    if cached_status:
        await redis.incr(domain)
        return {'url': url, 'status': int(cached_status)}

    try:
        async with session.get(url) as response:
            await redis.set(url, response.status)
            await redis.incr(domain)
            return {'url': url, 'status': response.status}
    except Exception as e:
        return {'url': url, 'status': str(e)}

@app.post("/api/v1/tasks/", status_code=201)
async def create_task(urls: List[str]):
    task_id = str(uuid.uuid4())
    task = Task(id=task_id, status="running")
    tasks[task_id] = task

    async with aiohttp.ClientSession() as session:
        redis = await create_redis_connection()
        tasks[task_id].result = await asyncio.gather(*[fetch_url(session, url, redis) for url in urls])
        tasks[task_id].status = "ready"
        await redis.aclose()
    
    return task

@app.get("/api/v1/tasks/{task_id}")
async def get_task(task_id: str):
    task = tasks.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8888)


