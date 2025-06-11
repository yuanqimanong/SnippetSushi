import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager

from DrissionPage import Chromium, ChromiumOptions

_lock_dict = {}
_dict_lock = threading.Lock()


@contextmanager
def key_lock(key):
    # 先获取字典锁来确保线程安全
    with _dict_lock:
        if key not in _lock_dict:
            _lock_dict[key] = threading.Lock()

    # 获取目标锁
    lock = _lock_dict[key]
    with lock:
        try:
            yield
        finally:
            pass


def run(url, cid, cr):
    with key_lock(cid):
        tab = cr.latest_tab

        tab.set.load_mode.eager()

        tab.get(url)
        title = tab.title
        # tab.wait(10)
        cr_list.put({cid: cr})
        return cid, url, title


MAX_WORKERS = 8
FOR_NUM = 100
cr_list = queue.Queue(maxsize=MAX_WORKERS)

if __name__ == "__main__":
    urls = [
        # "https://www.163.com/",
        "https://www.baidu.com/",
        # "https://www.qq.com/",
        "http://httpbin.org/ip",
        # "https://www.sohu.com/",
        # "https://www.ifeng.com/",
        "http://www.people.com.cn/",
        "https://www.jd.com/",
        # "https://www.taobao.com/",
        "https://www.tmall.com/",
    ]
    tasks = []

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for cid in range(MAX_WORKERS):
            co = (
                ChromiumOptions()
                .set_user_data_path(f"C://browser_cache/tmp_{cid}")
                .set_local_port(9111 + 6 + cid)
            )
            cr = Chromium(co)
            cr_list.put({cid: cr})

        init_time = time.time()

        for i in range(FOR_NUM):
            print(f"for {i}")
            for u in urls:
                while True:
                    if not cr_list.empty():
                        cr_dict = cr_list.get()
                        if cr_dict:
                            cid, cr = next(iter(cr_dict.items()))
                            tasks.append(executor.submit(run, u, cid, cr))
                            break
                    else:
                        time.sleep(0)

    for task in as_completed(tasks):
        r1, r2, r3 = task.result()
        print(r1, " --- ", r2, " --- ", r3)

    quit_time = time.time()
    while not cr_list.empty():
        cr_dict = cr_list.get()
        cid, cr = next(iter(cr_dict.items()))
        cr.quit()
        print(f"Quit {cid}")
    end_time = time.time()

    print(f"Init time: {init_time - start_time:.2f}s")
    print(f"Crawl time: {quit_time - init_time:.2f}s")
    print(f"Crawl time AVG: {(quit_time - init_time)/FOR_NUM/len(urls):.2f}s")
    print(f"Quit time: {end_time - quit_time:.2f}s")
    print(f"Total time: {end_time - start_time:.2f}s")
