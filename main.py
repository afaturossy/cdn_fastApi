import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
import uvicorn
from dotenv import load_dotenv

from fastapi import FastAPI, Response

from cdn_fastapi.controllers.client import Client

load_dotenv()

PORT = 5000
if os.getenv("PORT") is not None:
    PORT = int(os.getenv("PORT"))

app = FastAPI()
kode_ijin = "kopiko"

pool = ThreadPoolExecutor(max_workers=100)
session = requests.Session()
session.headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.77"
}


@app.get("/")
async def root(url: str | None = None):
    if url is not None:
        res = Client.image(url)
        return res
    else:
        return None


@app.put("/")
async def upload_image(url: str | None = None):
    if url is not None:
        b_url = url.encode('utf-8')
        filename = hashlib.sha3_256(b_url).hexdigest()
        file = Path(Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.jpg"))
        if file.exists():
            return "exist"
        else:
            download = Client.download_image(url, session)
            if download:
                return "download"
            else:
                return Response(content="", status_code=500)
    else:
        return Response(content="", status_code=500)


if __name__ == "__main__":
    print(f"server run in port {PORT}")
    uvicorn.run("main:app", port=PORT, host="0.0.0.0", log_level="info")
