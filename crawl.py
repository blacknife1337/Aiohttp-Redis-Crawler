import aiohttp
import asyncio
import sys

server_url = "http://localhost:8888/api/v1/tasks/"

async def submit_urls(urls):
    async with aiohttp.ClientSession() as session:
        async with session.post(server_url, json=urls) as response:
            return await response.json()

async def get_task_status(task_id):
    async with aiohttp.ClientSession() as session:
        async with session.get(server_url + task_id) as response:
            return await response.json()
        
def format_url(url):
    if not url.startswith(('http://', 'https://')):
        return 'http://' + url
    return url            

async def main(urls):
    urls = [format_url(url) for url in urls]
    task = await submit_urls(urls)
    task_id = task['id']
    while True:
        task = await get_task_status(task_id)
        if task['status'] == 'ready':
            for result in task['result']:
                print(f"{result['status']}\t{result['url']}")
            break
        await asyncio.sleep(0.1)
    

if __name__ == "__main__":
    urls = sys.argv[1:]
    asyncio.run(main(urls))
