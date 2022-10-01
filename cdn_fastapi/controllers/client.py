import hashlib
from pathlib import Path
from requests import Session
from PIL import Image

from fastapi.responses import FileResponse, Response


class Client:

    @classmethod
    def image(cls, url: str):
        b_url = url.encode('utf-8')
        filename = hashlib.sha3_256(b_url).hexdigest()

        file = Path(Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.webp"))
        if file.exists():
            return FileResponse(str(file.absolute()), media_type="image/webp")
        else:
            return Response(content=None, status_code=404)

    @staticmethod
    def download_image(url, session: Session):
        res = session.get(url)
        filename = hashlib.sha3_256(url.encode('utf-8')).hexdigest()
        if res.status_code < 300:
            p_file = Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.jpg")
            file = Path(p_file)
            try:
                file.write_bytes(res.content)

                im = Image.open(p_file).convert("RGB")
                im.save(Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.webp"), "webp")
                file.unlink()
                return True
            except Exception as e:
                if file.exists():
                    file.unlink()
                print(e)
                return False


if __name__ == "__main__":
    Client.download_image(
        'https://i2.wp.com/bd7207342500dcc9a18edb11.forthumbnail.xyz/uploads/2019/10/Komik-The-Last-Human.jpg?resize=146,208')

