import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path

from psycopg2.pool import SimpleConnectionPool
from requests import Session
from PIL import Image

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
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Versatile-Mage.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/03/Manga-Yankee-JK-Kuzuhana-chan.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2019/11/Komik-Release-That-Witch.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/04/Komik-Apprentices-Are-All-Female-Devil.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/01/Komik-My-Exclusive-Dream-World-Adventures.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2021/05/Manga-OreAku-Ore-wa-Seikan-Kokka-no-Akutoku-Ryoshu.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2022/01/Komik-All-Hail-the-Sect-Leader.jpg?resize=146,208',
            'https://i2.wp.com/cdn.andakepo.buzz/uploads/2020/07/Komik-Dushi-Xiewang.jpg?resize=146,208',
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

engine = SimpleConnectionPool(minconn=1, maxconn=3, user="arekturu", password="komik_db_yaya", database="komik_backend",
                              host="localhost",
                              port=5432)

th_pool = ThreadPoolExecutor(max_workers=100)


def download_image(data, session: Session):
    url = data[1]
    filename = hashlib.sha3_256(url.encode('utf-8')).hexdigest()
    p_file = Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.jpg")
    file = Path(p_file)
    if file.exists():
        return None
    res = session.get(url)
    if res.status_code < 300:
        try:
            file.write_bytes(res.content)

            im = Image.open(p_file).convert("RGB")
            im.save(Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.webp"), "webp")
            file.unlink()
            return [data[0], filename]
        except Exception as e:
            if file.exists():
                file.unlink()
            print(e)
            return None


if __name__ == "__main__":
    start = time.time()

    session = Session()
    session.headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.77"
    }
    conn = engine.getconn()
    cur = conn.cursor()
    cur.execute("""
        select k.id , k.cover from komik k order by k.id;
    """)
    list_cover = cur.fetchall()

    th_pool.map(download_image, list_cover, repeat(session))

    th_pool.shutdown(wait=True)

    print(time.time() - start)
