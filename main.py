import hashlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
import uvicorn

from fastapi import FastAPI, Response

from controllers.client import Client

PORT = 5000
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
        file = Path(Path.joinpath(Path.cwd(), "public", f"{filename}.jpg"))
        if file.exists():
            return "exist"
        else:
            download = Client.download_image(url, session)
            if download:
                return "download"
            else:
                return Response(content="0", status_code=500)
    else:
        return Response(content="0", status_code=500)


if __name__ == "__main__":
    uvicorn.run("main:app", port=PORT, log_level="info")
