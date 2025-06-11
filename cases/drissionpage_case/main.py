import csv
import logging
import shutil
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from queue import Queue

import pandas as pd
import requests
from DrissionPage import Chromium, ChromiumOptions

logger = logging.getLogger(__name__)


@dataclass
class QueueMessage:
    browser: Chromium
    timestamp: datetime


def read_task():

    df = pd.read_excel("？？？.xlsx", sheet_name=3)
    print(df)
    column_c_values = df["商品编码"].tolist()
    print(column_c_values)

    crawled_list = []
    cache_file = Path("task-cache.txt")
    if cache_file.exists():

        with open(cache_file, "r", encoding="utf-8") as f:
            line = f.readlines()
        crawled_list = [line.strip() for line in line]
    print(crawled_list)

    return [c for c in column_c_values if c not in crawled_list]


BROWSER_LOCK = {}


class DPCrawler:

    def __init__(self, pool_size=None, load_mode=None):
        self.load_mode = load_mode or "eager"
        self.pool_size = pool_size or 3
        self.port_list = self.get_port_list(self.pool_size)
        self.browser_queue = Queue(maxsize=self.pool_size)
        self.browser_cache_dir = Path("browser_cache")
        self.initialize_browser()

        self.results = []
        self.results_max_cnt = self.pool_size * 2

    def get_port_list(self, pool_size):
        port_area = (20000, 30000)

        def is_port_available(_port):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("localhost", _port))
                    return True
                except OSError:
                    return False

        available_ports = []
        for port in range(port_area[0], port_area[1]):
            if is_port_available(port):
                available_ports.append(port)
                if len(available_ports) >= pool_size:
                    break

        return available_ports

    def initialize_browser(self):
        for port in self.port_list:
            opts = (
                ChromiumOptions()
                # .set_browser_path(
                #     r"C:\Program Files\Sogou\SogouExplorer\SogouExplorer.exe"
                # )
                .set_user_data_path(f"{self.browser_cache_dir}/port_{port}")
                .set_local_port(port)
                .set_retry(2, 1)
                .set_load_mode(self.load_mode)
                .set_argument("--window-size", "1920*1080")
            )
            self.browser_queue.put(
                QueueMessage(
                    browser=Chromium(opts),
                    timestamp=datetime.now(),
                )
            )

    def acquire_browser(self):
        while True:
            if not self.browser_queue.empty():
                queue_msg = self.browser_queue.get()
                logger.info(
                    f"【获取浏览器对象】 主机端口：{queue_msg.browser.address} -- 浏览器创建时间： {queue_msg.timestamp}"
                )
                return queue_msg.browser
            else:
                time.sleep(0.1)

    def return_browser(self, browser):
        self.browser_queue.put(
            QueueMessage(
                browser=browser,
                timestamp=datetime.now(),
            )
        )

    @contextmanager
    def browser_lock(self, browser):
        if browser.address not in BROWSER_LOCK:
            BROWSER_LOCK[browser.address] = threading.Lock()

        lock = BROWSER_LOCK[browser.address]
        with lock:
            try:
                yield
            finally:
                pass

    def crawl_many(self, task_list):
        cnt = 0
        with ThreadPoolExecutor(max_workers=self.pool_size) as executor:
            for task in task_list:
                print(f"开始抓取 {task}")
                self.results.append(executor.submit(self.crawl_one, task))
                if len(self.results) >= self.results_max_cnt:
                    for r in as_completed(self.results):
                        logger.info(r.result())
                        cnt += 1
                    self.results.clear()

        return f"本次完成{cnt}条数据采集！"

    def crawl_one(self, task):
        browser = self.acquire_browser()
        with self.browser_lock(browser):
            try:
                self.rule(browser, task)
            except Exception as e:
                logger.error(e)
            self.return_browser(browser)

    def rule(self, browser, task):
        pass


class Crawler(DPCrawler):

    def rule(self, browser, task):
        url = "https://???/"

        tab = browser.latest_tab
        tab.get(url)
        tab.ele("@class=searchInput").input(str(task))
        tab.wait(1)
        tab.ele("@class=searchBtn").click()
        tab.wait(2)

        search_tab = browser.latest_tab
        search_tab.ele("@text()=隐藏限制交易商品").parent().child().click()
        search_tab.wait(2)
        product_ele = search_tab.ele("@id=searchCpzsAction").eles("@tag()=li")[-1]
        product_ele.click()
        search_tab.wait(2)

        product_tab = browser.latest_tab

        main_url = product_ele.ele("@tag()=img").attr("src")
        carousel_image = (
            product_tab.ele("@id=spec-list").ele("@tag()=ul").eles("@tag()=li")
        )
        carousel_image_urls = [x.ele("@tag()=img").attr("src") for x in carousel_image]
        detail_image = product_tab.ele("@id=LTT1").eles("@tag()=li")
        detail_image_urls = [x.ele("@tag()=img").attr("src") for x in detail_image]

        product_tab.close(others=True)

        self.save_data(task, main_url, carousel_image_urls, detail_image_urls)

    def save_data(self, skuid, main_url, a_url, b_url):
        dl_imgs = []
        imgs = {
            "主图": main_url,
            "轮播图": [e for e in a_url if e not in main_url],
            "详情图": b_url,
        }
        logger.info(f"imgs == {imgs}")

        i = 1
        for k, v in imgs.items():
            if "主图" == k:
                dl_imgs.append(
                    (
                        skuid,
                        "主图",
                        f"{skuid}-{i}",
                        f'图片/{skuid}/{skuid}-{i}.{v.split(".")[-1]}',
                        "",
                        v,
                    )
                )
                i += 1
                continue
            if "轮播图" == k:
                for e in v:
                    is_duplicate = "重复" if e in imgs["详情图"] else ""
                    dl_imgs.append(
                        (
                            skuid,
                            "轮播图",
                            f"{skuid}-{i}",
                            f'图片/{skuid}/{skuid}-{i}.{e.split(".")[-1]}',
                            is_duplicate,
                            e,
                        )
                    )
                    i += 1
                continue
            if "详情图" == k:
                for e in v:
                    is_duplicate = "重复" if e in imgs["轮播图"] else ""
                    dl_imgs.append(
                        (
                            skuid,
                            "详情图",
                            f"{skuid}-{i}",
                            f'图片/{skuid}/{skuid}-{i}.{e.split(".")[-1]}',
                            is_duplicate,
                            e,
                        )
                    )
                    i += 1

        logger.info(f"dl_imgs == {dl_imgs}")

        if not Path("images.csv").exists():
            with open("images.csv", "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                writer.writerow(
                    [
                        "合作电商商品编码",
                        "图片类型",
                        "命名规则",
                        "路径",
                        "是否重复",
                        "URL",
                    ]
                )

        with open("images.csv", "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            for row in dl_imgs:
                writer.writerow(row)
                self.download_img(row[5], row[3])

    def download_img(self, url, path):
        img_path = Path(path)
        img_path.parent.mkdir(parents=True, exist_ok=True)
        if img_path.exists():
            logger.info(f"{path} 已下载")
        print(f"下载 {url} 到 {path}")
        try:
            r = requests.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
                    "DNT": "1",
                    "Referer": "www.baidu.com",
                },
                stream=True,
                timeout=30,
            )
            if r.status_code == 200:
                with open(path, "wb") as f:
                    r.raw.decode_content = True
                    shutil.copyfileobj(r.raw, f)
                logger.info(f"{path} 下载完成!")
            else:
                print(f"{path} - {r.status_code} 下载重试")
                self.download_img(url, path)
        except Exception as e:
            print(f"{path} - {e}")
            self.download_img(url, path)


if __name__ == "__main__":
    tasks = read_task()
    result = Crawler(2, "normal").crawl_many(tasks)
    # result = Crawler(1, "normal").crawl_one(210603)
    logger.info(result)
