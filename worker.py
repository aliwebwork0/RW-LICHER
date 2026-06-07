import os
import re
import threading
import subprocess
import shlex
import time
from queue import Queue
from datetime import datetime

job_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()
processes = {}  # job_id -> subprocess.Popen
processes_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"
DEFAULT_REMOTE = os.environ.get("RCLONE_REMOTE", "Google Drive")


def now():
    return datetime.utcnow().strftime("%H:%M:%S")


def set_job(job_id, **kwargs):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)


def append_log(job_id, text):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["log"] += text


def parse_progress(line):
    """Extract % from curl --progress-bar output like: 45.2%"""
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', line)
    if match:
        return float(match.group(1))
    return None


def is_progress_line(line):
    """True if line is curl progress bar noise (only #, space, %, digits)."""
    stripped = line.strip()
    return bool(re.match(r'^[#=\-\s\d.%|]*$', stripped))


def get_referer(url):
    """Extract base domain as referer."""
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def build_cmd(url, filename):
    """ساخت command با استریم مستقیم و مصرف حافظه کم"""
    safe_url = shlex.quote(url)
    remote_name = os.environ.get("RCLONE_REMOTE", DEFAULT_REMOTE)
    safe_dest = shlex.quote(f"{remote_name}:/Video/{filename}")
    referer = get_referer(url)

    # روش بهینه: بافر کم + استریم مستقیم
    return (
        f"curl -L "
        f"--retry 5 --retry-delay 5 --retry-all-errors "
        f"--speed-limit 1 --speed-time 30 "
        f"--keepalive-time 30 "
        f"--max-time 0 "
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' "
        f"-H 'Referer: {referer}' "
        f"-H 'Accept: */*' "
        f"-H 'Accept-Language: en-US,en;q=0.9' "
        f"-H 'Connection: keep-alive' "
        f"--progress-bar "
        f"--buffer-size 32M "  # بافر 32 مگ (کمتر از 500 مگ)
        f"{safe_url} | "
        f"rclone rcat "
        f"--buffer-size 32M "  # بافر rclone هم 32 مگ
        f"--multi-thread-streams 4 "
        f"--low-level-retries 5 "
        f"--no-check-dest "
        f"{safe_dest}"
    )


def run_job(job):
    job_id = job["id"]
    url = job["url"]
    filename = job["filename"]

    set_job(job_id, status="running", log="", progress=0, retries=0, started_at=now())
    append_log(job_id, f"[{now()}] Starting transfer: {filename}\n")
    append_log(job_id, f"[{now()}] Using remote: {os.environ.get('RCLONE_REMOTE', DEFAULT_REMOTE)}\n")

    cmd = build_cmd(url, filename)
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    retry_count = 0

    while True:
        with jobs_lock:
            current_status = jobs.get(job_id, {}).get("status")
        if current_status == "cancelled":
            append_log(job_id, f"[{now()}] Transfer cancelled.\n")
            return

        try:
            # استفاده از bufsize=1 برای خط خط خوندن
            p = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                bufsize=1,  # line buffered
            )

            with processes_lock:
                processes[job_id] = p

            last_progress = 0
            for line in p.stdout:
                with jobs_lock:
                    if jobs.get(job_id, {}).get("status") == "cancelled":
                        p.kill()
                        append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                        return

                progress = parse_progress(line)
                if progress is not None:
                    if progress > last_progress + 1:  # آپدیت هر 1%
                        last_progress = progress
                        set_job(job_id, progress=progress)

                if not is_progress_line(line):
                    clean = line.strip()
                    if clean:
                        append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()

            with processes_lock:
                processes.pop(job_id, None)

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now())
                append_log(job_id, f"[{now()}] ✓ Transfer complete.\n")
                return
            else:
                retry_count += 1
                set_job(job_id, retries=retry_count)
                append_log(job_id, f"[{now()}] ✗ Failed (exit {p.returncode}), retrying... (#{retry_count})\n")
                time.sleep(5)

        except Exception as e:
            retry_count += 1
            set_job(job_id, retries=retry_count)
            append_log(job_id, f"[{now()}] Exception: {e}, retrying... (#{retry_count})\n")
            time.sleep(5)


def worker_loop():
    while True:
        job = job_queue.get()
        job_id = job["id"]
        try:
            with jobs_lock:
                if jobs.get(job_id, {}).get("status") == "cancelled":
                    job_queue.task_done()
                    continue
            run_job(job)
        except Exception as e:
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "failed"
                    jobs[job_id]["log"] += f"[{now()}] Fatal: {e}\n"
        finally:
            job_queue.task_done()


threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == "__main__":
    threading.Event().wait()
