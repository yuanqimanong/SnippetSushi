import platform
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Optional, Union

import psutil
from playwright.sync_api import sync_playwright


def get_logger(name, log_level: str = "DEBUG"):
    import os
    import sys
    import logging

    DEFAULT_LOG_FMT = "[%(threadName)s] [%(asctime)s] [%(levelname)s] [%(filename)s] [%(lineno)d] - %(message)s"

    name = name.split(os.sep)[-1].split(".")[0]
    _logger = logging.getLogger(name)
    _logger.setLevel(log_level)
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FMT))
    _logger.addHandler(stream_handler)
    return _logger


logger = get_logger(__name__)


def _get_win_app_path(app_name):
    from winreg import (
        OpenKey,
        EnumValue,
        HKEY_CURRENT_USER,
        HKEY_LOCAL_MACHINE,
        KEY_READ,
    )

    for root_key in [HKEY_CURRENT_USER, HKEY_LOCAL_MACHINE]:
        reg_path = rf"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\{app_name}"
        try:
            with OpenKey(root_key, reg_path, 0, KEY_READ) as key:
                return EnumValue(key, 0)[1]
        except FileNotFoundError:
            continue
        except PermissionError:
            logger.error(f"权限不足无法访问: {reg_path}")
        except Exception as e:
            logger.error(f"读取注册表时发生错误: {str(e)}")
    return None


def _find_free_port(port=12345, max_port=65535):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    while port <= max_port:
        try:
            sock.bind(("", port))
            sock.close()
            return port
        except OSError:
            port += 1
    return None


def _detect_chrome_processes(port=None, user_dir=None):
    processes = []
    if None not in (port, user_dir):
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                cmdline = proc.cmdline()
                if any("chrome" in part.lower() for part in cmdline[:2]):
                    port_match = port and any(
                        f"--remote-debugging-port={port}" in arg for arg in cmdline
                    )
                    dir_match = user_dir and any(
                        f"--user-data-dir={user_dir}" in arg for arg in cmdline
                    )

                    if (port and port_match) or (user_dir and dir_match):
                        processes.append(
                            {
                                "pid": proc.pid,
                                "port": next(
                                    (
                                        arg.split("=")[1]
                                        for arg in cmdline
                                        if arg.startswith("--remote-debugging-port=")
                                    ),
                                    None,
                                ),
                                "user_dir": next(
                                    (
                                        arg.split("=")[1]
                                        for arg in cmdline
                                        if arg.startswith("--user-data-dir=")
                                    ),
                                    None,
                                ),
                                "parent_pid": proc.ppid(),
                            }
                        )
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
    return processes


def _kill_chrome_processes(processes):
    for p_info in processes:
        try:
            proc = psutil.Process(p_info["pid"])
            children = proc.children(recursive=True)
            for child in children:
                child.kill()
            proc.kill()
            logger.error(f"已终止进程树 PID:{p_info['pid']}")
        except psutil.NoSuchProcess:
            continue


class PlaywrightSyncHelper:
    def __init__(
        self,
        *,
        browser_type: Optional[str] = "chromium",
        browser_path: Optional[str] = None,
        cdp_port: Optional[int] = None,
        user_data_dir: Union[str, Path] = None,
    ):
        self.process = None
        self.playwright = None
        self.browser_type = browser_type
        self.browser_path = browser_path
        self.cdp_port = cdp_port
        self.user_data_dir = user_data_dir
        self._temp_dir = None

    def __enter__(self):
        if self.browser_type not in ["chromium", "firefox", "webkit"]:
            self.browser_type = "chromium"

        if not self.browser_path:
            if (browser_path := self.get_browser_path()) is None:
                raise RuntimeError("未找到浏览器路径")
            self.browser_path = browser_path

        if not self.cdp_port:
            if (cdp_port := _find_free_port()) is None:
                raise RuntimeError("没有空闲端口")
            self.cdp_port = cdp_port

        if self.user_data_dir:
            user_data_dir = (
                self.user_data_dir.absolute()
                if isinstance(self.user_data_dir, Path)
                else Path(self.user_data_dir)
            )
            if not user_data_dir.is_absolute():
                user_data_dir = Path(__file__).parent / user_data_dir
            user_data_dir = user_data_dir.resolve().as_posix()
        else:
            self._temp_dir = tempfile.TemporaryDirectory(prefix="playwright_helper_")
            user_data_dir = Path(self._temp_dir.name).resolve().as_posix()

        logger.info(f"user_data_dir 路径: {user_data_dir}")
        self.process = subprocess.Popen(
            [
                self.browser_path,
                f"--remote-debugging-port={self.cdp_port}",
                f"--user-data-dir={user_data_dir}",
                # "--start-maximized",
                "--window-position=10,10",
                "--window-size=600,600",
                "--no-first-run",
                "--disable-first-run-ui",
                "--no-default-browser-check",
            ],
            shell=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            # stdout=subprocess.PIPE,
            # stderr=subprocess.STDOUT,
            # universal_newlines=True,
        )

        self.playwright = sync_playwright().start()
        browser = getattr(self.playwright, self.browser_type).connect_over_cdp(
            f"http://127.0.0.1:{self.cdp_port}"
        )
        return browser

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.playwright.stop()
        self.process.terminate()
        time.sleep(2)
        if self._temp_dir:
            try:
                self._temp_dir.cleanup()
                logger.info(f"已清理临时目录: {self._temp_dir.name}")
            except Exception as e:
                logger.error(f"清理临时目录失败: {str(e)}")

    def get_browser_path(self):
        _path = None
        system = platform.system()
        if system == "Windows":
            _path = _get_win_app_path("chrome.exe")
        if system == "Darwin":
            ...
        if system == "Linux":
            ...
        return _path


if __name__ == "__main__":
    # x = _get_win_app_path("chrome.exe")
    # logger.error((x)
    # xx = _detect_chrome_processes(12345)
    # logger.error(xx)
    # _kill_chrome_processes(xx)

    with PlaywrightSyncHelper(user_data_dir="cache") as b:
        page = b.new_page()
        page.goto("https://www.baidu.com")
        print(page.title())
        page.wait_for_timeout(3000)
