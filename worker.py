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
processes = {}   # job_id -> subprocess.Popen
processes_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"

MAX_RETRIES    = 5      # حداکثر تعداد retry قبل از failed
RETRY_DELAY    = 8      # ثانیه بین retry‌ها
STALL_TIMEOUT  = 120    # اگر ۱۲۰ ثانیه progress نداشت → fail
CONNECT_TIMEOUT = 30    # timeout برای اتصال اولیه curl


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


def is_progress_line(line):
    stripped = line.strip()
    return bool(re.match(r'^[#=\-\s\d.%|]*$', stripped))


def get_referer(url):
    try:
        from urllib.parse import urlparse
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}/"
    except Exception:
        return url


def build_cmd(url, filename):
    safe_url  = shlex.quote(url)
    safe_dest = shlex.quote(f"mega:/Video/{filename}")
    referer   = get_referer(url)

    return (
        f"curl -L "
        f"--connect-timeout {CONNECT_TIMEOUT} "
        f"--retry 3 --retry-delay 5 --retry-all-errors "
        f"--speed-limit 1 --speed-time {STALL_TIMEOUT} "
        f"--keepalive-time 30 "
        f"--max-time 0 "
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' "
        f"-H 'Referer: {referer}' "
        f"-H 'Accept: */*' "
        f"-H 'Accept-Language: en-US,en;q=0.9' "
        f"-H 'Connection: keep-alive' "
        f"--progress-bar "
        f"--fail "
        f"{safe_url} | rclone rcat {safe_dest}"
    )


def kill_job_process(job_id):
    """Kill the process group for a job (kills both curl and rclone)."""
    with processes_lock:
        p = processes.pop(job_id, None)
    if p:
        try:
            import signal
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass


def is_cancelled(job_id):
    with jobs_lock:
        return jobs.get(job_id, {}).get("status") == "cancelled"


def run_job(job):
    job_id   = job["id"]
    url      = job["url"]
    filename = job["filename"]

    set_job(job_id, status="running", log="", progress=0, retries=0, started_at=now())
    append_log(job_id, f"[{now()}] Starting transfer: {filename}\n")

    cmd = build_cmd(url, filename)
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    for attempt in range(1, MAX_RETRIES + 1):
        if is_cancelled(job_id):
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
                start_new_session=True,   # process group جدا → kill همه رو می‌کشه
            )

            with processes_lock:
                processes[job_id] = p

            for line in p.stdout:
                if is_cancelled(job_id):
                    kill_job_process(job_id)
                    append_log(job_id, f"[{now()}] Cancelled mid-transfer.\n")
                    return

                progress = parse_progress(line)
                if progress is not None:
                    set_job(job_id, progress=progress)

                if not is_progress_line(line):
                    clean = line.strip()
                    if clean:
                        append_log(job_id, f"[{now()}] {clean}\n")

            p.wait()

            with processes_lock:
                processes.pop(job_id, None)

            if is_cancelled(job_id):
                append_log(job_id, f"[{now()}] Cancelled.\n")
                return

            if p.returncode == 0:
                set_job(job_id, status="done", progress=100, finished_at=now())
                append_log(job_id, f"[{now()}] ✓ Transfer complete.\n")
                return
            else:
                append_log(job_id, f"[{now()}] ✗ Failed (exit {p.returncode}) — attempt {attempt}/{MAX_RETRIES}\n")
                set_job(job_id, retries=attempt)

                if attempt < MAX_RETRIES:
                    append_log(job_id, f"[{now()}] Retrying in {RETRY_DELAY}s...\n")
                    # قبل از sleep چک کن cancel نشده باشه
                    for _ in range(RETRY_DELAY * 2):
                        if is_cancelled(job_id):
                            append_log(job_id, f"[{now()}] Cancelled during retry wait.\n")
                            return
                        time.sleep(0.5)

        except Exception as e:
            with processes_lock:
                processes.pop(job_id, None)
            append_log(job_id, f"[{now()}] Exception: {e} — attempt {attempt}/{MAX_RETRIES}\n")
            set_job(job_id, retries=attempt)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    # همه retry‌ها تموم شد
    set_job(job_id, status="failed", finished_at=now())
    append_log(job_id, f"[{now()}] ✗ All {MAX_RETRIES} attempts failed. Giving up.\n")


def worker_loop():
    while True:
        job = job_queue.get()
        job_id = job["id"]
        try:
            if is_cancelled(job_id):
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


# یک worker thread کافیه (serial downloads)
threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == "__main__":
    threading.Event().wait()
