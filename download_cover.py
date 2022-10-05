import hashlib
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
from pathlib import Path

import psutil as psutil
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
                              host="javtube.fun",
                              port=5432)

th_pool = ThreadPoolExecutor(max_workers=2)


def download_image(url, session: Session):
    filename = hashlib.sha3_256(url.encode('utf-8')).hexdigest()
    p_file = Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.jpg")
    file = Path(p_file)
    if file.exists():
        # print(f"data exist")
        return True
    res = session.get(url)
    if res.status_code < 300:
        try:
            file.write_bytes(res.content)

            im = Image.open(p_file).convert("RGB")
            im.save(Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.webp"), "webp")
            file.unlink()
            # print(f"data done")
            return True
        except Exception as e:
            if file.exists():
                file.unlink()
            print(e)
            return None


def check_file(url):
    filename = hashlib.sha3_256(url.encode('utf-8')).hexdigest()
    p_file = Path.joinpath(Path.cwd(), "cdn_fastapi/public", f"{filename}.jpg")
    file = Path(p_file)
    if file.exists():
        return None
    else:
        return url


def get_cover(cursor) -> list:
    cursor.execute("""
            select k.cover from komik k order by k.id;
        """)
    return cursor.fetchall()


if __name__ == "__main__":
    start = time.time()

    session = Session()
    session.headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.134 Safari/537.36 Edg/103.0.1264.77"
    }
    conn = engine.getconn()
    cur = conn.cursor()

    if "cover" in sys.argv:
        list_cover = get_cover(cur)
        list_cover = [x[0] for x in list_cover]
        th_pool.map(download_image, list_cover, repeat(session))
    elif "chapter" in sys.argv:
        cur.execute("""select count(*) from chapter c ; """)
        count = cur.fetchone()
        cur.close()
        limit = 10
        proses = psutil.Process(os.getpid())

        for i in range(math.ceil(count[0] / limit)):
            print(i, i * limit)
            print(f"memory usage {proses.memory_info}")
            # print(f"select images from chapter order by id offset {i * limit} limit {limit} ;")
            cur = conn.cursor()
            cur.execute(f"select images from chapter order by id offset {i * limit} limit {limit} ;")
            list_img_double = cur.fetchall()

            for list_img in list_img_double:
                print(i)
                for img in list_img[0]:
                    cek = check_file(img)
                    if cek is not None:
                        r = th_pool.submit(download_image, img, session)
                        r.result()

            cur.close()

    engine.putconn(conn)
    engine.closed()
    print(time.time() - start)
