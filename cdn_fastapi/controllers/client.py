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

    list_img = ['https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Rich-Player.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/10/Komik-God-of-Martial-Arts.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/10/Manga-Tomo-chan-wa-Onnanoko-scaled.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/10/Manga-Tonari-no-Kashiwagi-san.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
                'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/12/Manga-Houseki-no-Kuni.jpg?resize=146,208'
                ]
