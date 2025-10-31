#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
复杂的 NAS 文件扫描器

主要功能：
- 高效的目录遍历（os.scandir）
- 多线程并发处理目录与文件（ThreadPoolExecutor）
- 防止符号链接造成的循环（跟踪 (st_dev, st_ino)）
- 可选 SHA256 校验和计算（并行）
- 可选大小/时间/模式过滤
- 结果持久化到 SQLite，支持断点续传与增量更新
- 提供重复文件检测接口
- 详细的日志与错误处理
"""

import os
import sys
import argparse
import sqlite3
import hashlib
import threading
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import fnmatch
import mimetypes

try:
    from tqdm import tqdm
except Exception:
    tqdm = None  # 进度条为可选

# ------- 配置日志 -------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(threadName)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("nas_scanner")

# ------- SQLite schema -------
SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    path TEXT UNIQUE,
    size INTEGER,
    mtime REAL,
    atime REAL,
    ctime REAL,
    mode INTEGER,
    uid INTEGER,
    gid INTEGER,
    dev INTEGER,
    inode INTEGER,
    sha256 TEXT,
    mime TEXT,
    scanned_at REAL
);
CREATE INDEX IF NOT EXISTS idx_sha256 ON files(sha256);
CREATE INDEX IF NOT EXISTS idx_dev_inode ON files(dev, inode);
"""

# ------- 辅助函数 -------
def safe_stat(path, follow_symlinks=True):
    try:
        return os.stat(path, follow_symlinks=follow_symlinks)
    except (FileNotFoundError, PermissionError, OSError) as e:
        logger.debug("stat failed: %s -> %s", path, e)
        return None

def sha256_file(path, block_size=4 * 1024 * 1024):
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                data = f.read(block_size)
                if not data:
                    break
                h.update(data)
        return h.hexdigest()
    except (PermissionError, FileNotFoundError, OSError) as e:
        logger.debug("hash failed: %s -> %s", path, e)
        return None

def guess_mime(path):
    m, _ = mimetypes.guess_type(path)
    return m or "application/octet-stream"

# ------- SQLite 持久化 -------
class DB:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode = WAL;")
        self._conn.executescript(SQLITE_SCHEMA)
        self._conn.commit()

    def upsert_file(self, meta):
        """
        meta: dict with keys path,size,mtime,atime,ctime,mode,uid,gid,dev,inode,sha256,mime,scanned_at
        """
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                INSERT INTO files(path,size,mtime,atime,ctime,mode,uid,gid,dev,inode,sha256,mime,scanned_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(path) DO UPDATE SET
                  size=excluded.size,
                  mtime=excluded.mtime,
                  atime=excluded.atime,
                  ctime=excluded.ctime,
                  mode=excluded.mode,
                  uid=excluded.uid,
                  gid=excluded.gid,
                  dev=excluded.dev,
                  inode=excluded.inode,
                  sha256=excluded.sha256,
                  mime=excluded.mime,
                  scanned_at=excluded.scanned_at
                """,
                (
                    meta["path"],
                    meta["size"],
                    meta["mtime"],
                    meta["atime"],
                    meta["ctime"],
                    meta["mode"],
                    meta["uid"],
                    meta["gid"],
                    meta["dev"],
                    meta["inode"],
                    meta.get("sha256"),
                    meta.get("mime"),
                    meta["scanned_at"],
                ),
            )
            self._conn.commit()

    def find_duplicates(self):
        with self._lock:
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT sha256, COUNT(*) c FROM files
                WHERE sha256 IS NOT NULL
                GROUP BY sha256 HAVING c > 1
                """
            )
            rows = cur.fetchall()
            dup_map = {}
            for sha, cnt in rows:
                cur.execute("SELECT path,size,mtime FROM files WHERE sha256=? ORDER BY path", (sha,))
                dup_map[sha] = cur.fetchall()
            return dup_map

    def close(self):
        with self._lock:
            self._conn.close()

# ------- 主扫描器 -------
class NASScanner:
    def __init__(
        self,
        root,
        dbpath="nas_scan.db",
        follow_symlinks=False,
        compute_hash=False,
        threads=8,
        pattern=None,
        min_size=0,
        max_size=None,
        resume=True,
    ):
        self.root = os.path.abspath(root)
        self.follow_symlinks = follow_symlinks
        self.compute_hash = compute_hash
        self.threads = max(1, threads)
        self.pattern = pattern
        self.min_size = min_size
        self.max_size = max_size
        self.resume = resume

        self.db = DB(dbpath)
        self.scanned_at = time.time()
        # 为防止符号链接或重复 device/inode 导致的无限循环，跟踪已访问的 (dev, inode)
        self.visited_inodes = set()
        self.visited_lock = threading.Lock()
        self.dir_queue = Queue()
        self.file_futures = []
        self.progress = None

    def should_skip_by_name(self, name):
        if self.pattern is None:
            return False
        return not fnmatch.fnmatch(name, self.pattern)

    def mark_visited(self, st):
        key = (st.st_dev, st.st_ino)
        with self.visited_lock:
            if key in self.visited_inodes:
                return False
            self.visited_inodes.add(key)
            return True

    def process_file(self, path, st):
        # 过滤大小范围
        size = st.st_size
        if size < self.min_size or (self.max_size is not None and size > self.max_size):
            logger.debug("skip by size: %s (%d)", path, size)
            return None

        # 决定是否需要计算 sha256（可以并行）
        sha = None
        if self.compute_hash:
            sha = sha256_file(path)

        meta = {
            "path": path,
            "size": size,
            "mtime": st.st_mtime,
            "atime": st.st_atime,
            "ctime": st.st_ctime,
            "mode": st.st_mode,
            "uid": st.st_uid,
            "gid": st.st_gid,
            "dev": st.st_dev,
            "inode": st.st_ino,
            "sha256": sha,
            "mime": guess_mime(path),
            "scanned_at": self.scanned_at,
        }

        # 写入数据库（断点续传模式下会替换）
        try:
            self.db.upsert_file(meta)
        except Exception as e:
            logger.warning("DB upsert failed for %s: %s", path, e)
        return meta

    def walk_worker(self):
        # 单个线程的目录处理器：从队列取目录并扫描条目
        while True:
            try:
                dirpath = self.dir_queue.get(timeout=1)
            except Empty:
                return
            try:
                with os.scandir(dirpath) as it:
                    for entry in it:
                        try:
                            # 选择是否跟随符号链接
                            st = safe_stat(entry.path, follow_symlinks=self.follow_symlinks)
                            if st is None:
                                continue
                            # 目录 -> 入队
                            if entry.is_dir(follow_symlinks=self.follow_symlinks):
                                # 防止同一 inode 重复走入（符号链接环等）
                                if self.mark_visited(st):
                                    self.dir_queue.put(entry.path)
                                else:
                                    logger.debug("skip visited dir(inode): %s", entry.path)
                                continue
                            # 文件 -> 提交文件处理（计算 metadata/hash 并入库）
                            if entry.is_file(follow_symlinks=self.follow_symlinks):
                                if self.should_skip_by_name(entry.name):
                                    logger.debug("skip by pattern: %s", entry.path)
                                    continue
                                # 如果 resume 且没有变化可以跳过：检查 DB 中当前记录
                                if self.resume:
                                    # 快速检查是否已存在且 mtime/size 未变
                                    cur = self.db._conn.cursor()
                                    cur.execute("SELECT size,mtime,sha256 FROM files WHERE path=?", (entry.path,))
                                    row = cur.fetchone()
                                    if row and row[0] == st.st_size and row[1] == st.st_mtime:
                                        logger.debug("resume skip unchanged: %s", entry.path)
                                        continue
                                # 提交到线程池（这里使用相同线程池做文件处理）
                                yield (entry.path, st)
            except PermissionError as e:
                logger.warning("Permission denied: %s -> %s", dirpath, e)
            except FileNotFoundError:
                logger.debug("Dir vanished while scanning: %s", dirpath)
            except OSError as e:
                logger.warning("OSError scanning %s: %s", dirpath, e)
            finally:
                self.dir_queue.task_done()

    def scan(self):
        # 初始化根目录的 inode 跟踪
        root_stat = safe_stat(self.root, follow_symlinks=self.follow_symlinks)
        if root_stat is None:
            raise FileNotFoundError(self.root)
        self.mark_visited(root_stat)
        self.dir_queue.put(self.root)

        # 用线程池并发处理目录和文件
        pool = ThreadPoolExecutor(max_workers=self.threads)
        file_futures = []
        processed = 0

        # 我们将使用生产-消费模型：多个目录工作者作为生成器产出文件任务
        # 为了简单实现：每个线程执行 walk_worker 函数，但该函数需要把文件任务以 yield 的形式返回
        # 这里用另一种方式：主线程循环读取 dir_queue，由线程池并发运行 os.scandir（通过 submit）
        try:
            # 初始任务：并发从 dir_queue 取目录并扫描
            active = []
            for _ in range(self.threads):
                active.append(pool.submit(self._dir_scan_loop))

            # 等待所有目录工作完成
            for fut in as_completed(active):
                # 返回值为处理文件计数
                processed += fut.result()
        finally:
            pool.shutdown(wait=True)
            self.db.close()
        logger.info("Scanning complete. total processed files: %d", processed)

    def _dir_scan_loop(self):
        processed_files = 0
        # 本循环在每个线程中运行，持续消费 dir_queue 中的路径，直到空超时然后退出
        while True:
            try:
                dirpath = self.dir_queue.get(timeout=1)
            except Empty:
                break
            try:
                with os.scandir(dirpath) as it:
                    for entry in it:
                        try:
                            st = safe_stat(entry.path, follow_symlinks=self.follow_symlinks)
                            if st is None:
                                continue
                            if entry.is_dir(follow_symlinks=self.follow_symlinks):
                                if self.mark_visited(st):
                                    self.dir_queue.put(entry.path)
                                else:
                                    logger.debug("skip visited dir(inode): %s", entry.path)
                                continue
                            if entry.is_file(follow_symlinks=self.follow_symlinks):
                                if self.should_skip_by_name(entry.name):
                                    continue
                                # resume 快速跳过
                                if self.resume:
                                    cur = self.db._conn.cursor()
                                    cur.execute("SELECT size,mtime FROM files WHERE path=?", (entry.path,))
                                    row = cur.fetchone()
                                    if row and row[0] == st.st_size and row[1] == st.st_mtime:
                                        continue
                                # 处理文件（可并发计算 hash，这里为了简化采用当前线程处理）
                                meta = self.process_file(entry.path, st)
                                if meta:
                                    processed_files += 1
                    # end for entries
            except PermissionError as e:
                logger.warning("Permission denied scanning %s: %s", dirpath, e)
            except FileNotFoundError:
                logger.debug("Dir vanished while scanning: %s", dirpath)
            except OSError as e:
                logger.warning("OSError scanning %s: %s", dirpath, e)
            finally:
                self.dir_queue.task_done()
        return processed_files

# ------- CLI -------
def parse_args():
    p = argparse.ArgumentParser(description="复杂的 NAS 路径文件扫描器")
    p.add_argument("root", help="NAS 根路径（本地挂载点）")
    p.add_argument("--db", default="nas_scan.db", help="SQLite DB 文件（默认 nas_scan.db）")
    p.add_argument("--follow-symlinks", action="store_true", help="跟随符号链接（小心环路）")
    p.add_argument("--hash", action="store_true", dest="compute_hash", help="计算 SHA256 校验和（可能很慢）")
    p.add_argument("--threads", type=int, default=8, help="并发线程数（默认 8）")
    p.add_argument("--pattern", help="文件名 glob 模式过滤，例如 '*.mp4'")
    p.add_argument("--min-size", type=int, default=0, help="最小文件大小过滤（字节）")
    p.add_argument("--max-size", type=int, default=None, help="最大文件大小过滤（字节）")
    p.add_argument("--no-resume", action="store_true", help="不启用断点续传（总是重新写入）")
    return p.parse_args()

def main():
    args = parse_args()
    scanner = NASScanner(
        root=args.root,
        dbpath=args.db,
        follow_symlinks=args.follow_symlinks,
        compute_hash=args.compute_hash,
        threads=args.threads,
        pattern=args.pattern,
        min_size=args.min_size,
        max_size=args.max_size,
        resume=not args.no_resume,
    )

    start = time.time()
    try:
        scanner.scan()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.exception("Scanning failed: %s", e)
    finally:
        end = time.time()
        logger.info("Elapsed: %.2f s", end - start)
        # 打印重复文件简要报告
        dup = scanner.db.find_duplicates()
        if dup:
            logger.info("发现重复校验和组：%d", len(dup))
            for sha, items in dup.items():
                logger.info("SHA256=%s (%d files)", sha, len(items))
        else:
            logger.info("未发现重复文件（基于已计算的 SHA256）")

if __name__ == "__main__":
    main()