import os
import re
import threading
import subprocess
import shlex
import time
import signal
from queue import Queue
from datetime import datetime

job_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()
processes = {}
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
    match = re.search(r'(\d+(?:\.\d+)?)\s*%', line)
    if match:
        return float(match.group(1))
    return None


def get_referer(url):
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def build_cmd(url, filename):
    """روش مخصوص فایل چند گیگی - دانلود با aria2 + آپلود تکه تکه"""
    safe_url = shlex.quote(url)
    remote_name = os.environ.get("RCLONE_REMOTE", DEFAULT_REMOTE)
    safe_dest = shlex.quote(f"{remote_name}:/Video/{filename}")
    referer = get_referer(url)
    
    # استفاده از aria2 برای دانلود چندبخشی و استریم همزمان
    return (
        f"aria2c --quiet=true "
        f"--max-connection-per-server=16 "
        f"--split=16 "
        f"--min-split-size=10M "
        f"--max-concurrent-downloads=1 "
        f"--continue=true "
        f"--max-tries=0 "
        f"--retry-wait=5 "
        f"--timeout=60 "
        f"--referer={referer} "
        f"--user-agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' "
        f"--summary-interval=0 "
        f"--console-log-level=warn "
        f"-o - {safe_url} | "
        f"rclone rcat "
        f"--buffer-size=64M "
        f"--multi-thread-streams=8 "
        f"--low-level-retries=10 "
        f"--timeout=30m "
        f"--no-check-dest "
        f"{safe_dest}"
    )


def build_cmd_fallback(url, filename):
    """روش دوم: curl با resume و تکه تکه"""
    safe_url = shlex.quote(url)
    remote_name = os.environ.get("RCLONE_REMOTE", DEFAULT_REMOTE)
    safe_dest = shlex.quote(f"{remote_name}:/Video/{filename}")
    referer = get_referer(url)
    
    return (
        f"curl -L "
        f"--retry 9999 --retry-delay 5 --retry-all-errors "
        f"--speed-limit 1 --speed-time 60 "
        f"--keepalive-time 60 "
        f"--max-time 0 "
        f"--continue-at - "  # resume support
        f"--limit-rate 20M "  # محدودیت سرعت برای پایداری
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' "
        f"-H 'Referer: {referer}' "
        f"--progress-bar "
        f"{safe_url} | "
        f"rclone rcat "
        f"--buffer-size=16M "
        f"--multi-thread-streams=4 "
        f"--low-level-retries=10 "
        f"--timeout=60m "
        f"{safe_dest}"
    )


def run_job(job):
    job_id = job["id"]
    url = job["url"]
    filename = job["filename"]

    set_job(job_id, status="running", log="", progress=0, retries=0, started_at=now())
    append_log(job_id, f"[{now()}] Starting transfer: {filename}\n")
    append_log(job_id, f"[{now()}] Using remote: {os.environ.get('RCLONE_REMOTE', DEFAULT_REMOTE)}\n")
    append_log(job_id, f"[{now()}] ⚡ Large file mode enabled\n")

    # چک کردن وجود aria2
    try:
        subprocess.run(["aria2c", "--version"], capture_output=True, check=True)
        cmd = build_cmd(url, filename)
        append_log(job_id, f"[{now()}] Using aria2 (multi-thread download)\n")
    except:
        cmd = build_cmd_fallback(url, filename)
        append_log(job_id, f"[{now()}] Using curl fallback\n")
    
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
            p = subprocess.Popen(
                cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                bufsize=1,
            )

            with processes_lock:
                processes[job_id] = p

            for line in p.stdout:
                with jobs_lock:
                    if jobs.get(job_id, {}).get("status") == "cancelled":
                        p.kill()
                        append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                        return

                progress = parse_progress(line)
                if progress is not None:
                    set_job(job_id, progress=progress)
                    
                # فیلتر خطوط اضافی
                if "%" not in line or "[" not in line:
                    clean = line.strip()
                    if clean and len(clean) < 200:
                        append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()

            with processes_lock:
                processes.pop(job_id, None)

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now())
                append_log(job_id, f"[{now()}] ✓ Transfer complete for {filename}\n")
                return
            else:
                retry_count += 1
                set_job(job_id, retries=retry_count)
                append_log(job_id, f"[{now()}] ✗ Failed (exit {p.returncode}), retrying... (#{retry_count})\n")
                
                if retry_count > 10:
                    append_log(job_id, f"[{now()}] ❌ Too many retries, aborting\n")
                    set_job(job_id, status="failed")
                    return
                    
                time.sleep(10)

        except Exception as e:
            retry_count += 1
            set_job(job_id, retries=retry_count)
            append_log(job_id, f"[{now()}] Exception: {e}, retrying... (#{retry_count})\n")
            time.sleep(10)


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
