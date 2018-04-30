"""Recursive downloader for FTP."""
import os.path
import argparse
import configparser
import asyncio
import tempfile
import fnmatch
import datetime
import logging
from pathlib import Path
import concurrent.futures
import time

import attr
import aioftp
from aiocontext import async_contextmanager
import aiofiles


logging.basicConfig()


@attr.s
class DownloadRule:
    host = attr.ib()
    username = attr.ib()
    password = attr.ib()
    local_path = attr.ib(converter=Path)
    
    #: Optional attributes
    remote_path = attr.ib(default="")
    port = attr.ib(converter=int, default=21)

    #: A pattern to identify files to match
    pattern = attr.ib(default="*")

    #: How many downloads to perform in parallel
    parallel = attr.ib(converter=int, default=1) 


def read_config(*paths):
    """Read a server configuration.

    Return a dict of DownloadRule keyed by name.

    """ 
    p = configparser.ConfigParser()
    p.read(paths)

    rules = {}
    for key in p.sections():
        section = p[key]
        rule = DownloadRule(
            host=section['host'],
            port=section.getint('port', fallback=21),
            username=section['username'],
            password=section['password'],
            remote_path=section.get('remote_path', fallback=""),
            local_path=section['local_path'],
            pattern=section.get('pattern', fallback="*"),
            parallel=section.getint('parallel', fallback=1)
        )
        rules[key] = rule
    return rules


@async_contextmanager
async def connection(rule):
    cs = aioftp.ClientSession(
        host=rule.host,
        port=rule.port,
        user=rule.username,
        password=rule.password,
    )
    async with cs as client:
        if rule.remote_path:
            await client.change_directory(rule.remote_path)
        yield client


async def download_worker(worker_id, rule, queue):
    """A worker to download files from a queue.
    
    The worker will retry up to three times in the face of exceptions while
    downloading.
    
    """
    retries = 3
    while retries:
        try:
            return await run_queue(worker_id, rule, queue)
        except concurrent.futures.CancelledError:
            return 1
        except Exception as e:
            retries -= 1
            if not retries:
                raise
            logging.error(f'Retrying due to {type(e).__name__} processing queue: {e}')


async def run_queue(worker_id, rule, queue):
    """Process download requests from the queue."""
    failures = 0
    start = 0
    async with connection(rule) as client:
        while True:
            msg = await queue.get()
            if msg is None:
                return failures
            path, local, info = msg

            local_dir = local.parent
            local_dir.mkdir(parents=True, exist_ok=True)
            
            start = time.time()
            tmpfile = None
            try:
                fd, tmppath = tempfile.mkstemp(dir=local_dir)
                tmpfile = Path(tmppath)

                bs_read = 0
                try:
                    async with aiofiles.open(fd, 'wb') as f:
                        stream = await client.download_stream(path)
                        async for block in stream.iter_by_block(100 * 1024):
                            bs_read += len(block)
                            await f.write(block)
                        await stream.finish()
                except concurrent.futures.CancelledError:
                    tmpfile.unlink()
                    return 1
                except Exception:
                    logging.exception(f'Error downloading {path}')
                    tmpfile.unlink()
                    failures += 1
                    continue

                if bs_read != info['size']:
                    logging.error(
                        f"Downloaded size {bs_read} for {path} does not match "
                        f"expected {info['size']}"
                    )
                    tmpfile.unlink()
                    failures += 1
                    continue
                    
                os.utime(tmpfile, (info['mtime'],) * 2)
                tmpfile.rename(local)
                end = time.time()
                print(f'{worker_id}: {path}: {bs_read} bytes in {end - start:.2f}s')
            finally:
                if tmpfile:
                    try:
                        tmpfile.unlink()
                    except FileNotFoundError:
                        pass


async def download(rule):
    """Run a number of workers to download files under the given rule."""
    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()
    scanner = loop.create_task(scan_remote(rule, queue))
    workers = [loop.create_task(download_worker(i + 1, rule, queue)) for i in range(rule.parallel)]
    tasks = [scanner] + workers

    while True:
        done, pending = await asyncio.wait(tasks, return_when=concurrent.futures.FIRST_EXCEPTION)
        if pending:
            # If we still have pending tasks then at least one worker died with an exception
            # We can cancel the others
            break
        for p in pending:
            p.cancel()
    return sum(w.result() for w in done)


async def scan_remote(rule, queue):
    """Scan paths present on the remote server."""
    async with connection(rule) as client:
        async for path, info in client.list(recursive=True):
            if not fnmatch.fnmatchcase(path.name, rule.pattern):
                continue
            local = rule.local_path / path

            size = info['size'] = int(info['size'])
            mtime = datetime.datetime.strptime(info['modify'], '%Y%m%d%H%M%S').timestamp()
            info['mtime'] = mtime

            try:
                st = local.stat()
            except FileNotFoundError:
                pass
            else:
                if st.st_mtime == mtime and st.st_size == size:
                    continue

            await queue.put((path, local, info))
    for _ in range(rule.parallel):
        await queue.put(None)


def suck(rule):
    """Recursively download according to a rule."""
    if not rule.local_path.exists():
        rule.local_path.mkdir()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(download(rule))


parser = argparse.ArgumentParser()
parser.add_argument(
    '-C', '--conf',
    action="append",
    help="The configuration file(s) to read. May be given multiple times."
)
parser.add_argument('rule', help="The download rule to run.")


def main():
    opts = parser.parse_args()
    confs = read_config(*(os.path.expanduser(p) for p in opts.conf))

    try:
        rule = confs[opts.rule]
    except KeyError:
        parser.error(f"Rule {opts.rule} not found in config files: {opts.conf}")

    suck(rule)



if __name__ == '__main__':
    main()
