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
processes = {}
processes_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"
MIN_VALID_SIZE = 1 * 1024 * 1024  # فایل زیر ۱MB جعلیه


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


def log(job_id, msg):
    line = f"[{now()}] {msg}\n"
    append_log(job_id, line)
    print(f"[JOB {job_id[:8]}] {msg}", flush=True)  # Railway log


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


def human_size(b):
    if b is None:
        return "unknown"
    for unit in ['B', 'KB', 'MB', 'GB']:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} TB"


def get_remote_type():
    """Detect remote name from rclone config."""
    try:
        result = subprocess.run(
            ["rclone", "listremotes"],
            capture_output=True, text=True,
            env={**os.environ, "RCLONE_CONFIG": RCLONE_CONFIG_PATH},
            timeout=10
        )
        remotes = result.stdout.strip().splitlines()
        if remotes:
            return remotes[0].rstrip(":")
    except Exception:
        pass
    return "mega"


REMOTE = None  # lazy init


def get_dest(filename):
    global REMOTE
    if REMOTE is None:
        REMOTE = get_remote_type()
    return f"{REMOTE}:/Video/{filename}"


def get_source_size(url, referer, job_id):
    """HEAD request to get Content-Length of source file."""
    log(job_id, f"🔍 Checking source file size...")
    try:
        result = subprocess.run(
            [
                "curl", "-sLI", "--max-time", "20",
                "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "-H", f"Referer: {referer}",
                "-H", "Accept: */*",
                url
            ],
            capture_output=True, text=True,
            timeout=25
        )
        for line in result.stdout.splitlines():
            if line.lower().startswith("content-length:"):
                size = int(line.split(":", 1)[1].strip())
                log(job_id, f"📦 Source size: {human_size(size)} ({size} bytes)")
                return size
        log(job_id, "⚠️  Could not get source size (server didn't return Content-Length)")
    except Exception as e:
        log(job_id, f"⚠️  HEAD request failed: {e}")
    return None


def get_uploaded_size(dest, job_id):
    """Check size of uploaded file on cloud."""
    try:
        result = subprocess.run(
            ["rclone", "size", dest, "--json"],
            capture_output=True, text=True,
            env={**os.environ, "RCLONE_CONFIG": RCLONE_CONFIG_PATH},
            timeout=30
        )
        import json
        data = json.loads(result.stdout)
        size = data.get("bytes", 0)
        log(job_id, f"☁️  Uploaded size: {human_size(size)} ({size} bytes)")
        return size
    except Exception as e:
        log(job_id, f"⚠️  Could not check uploaded size: {e}")
    return None


def delete_remote_file(dest, job_id):
    """Delete incomplete/fake file from cloud."""
    log(job_id, f"🗑  Deleting incomplete file: {dest}")
    try:
        result = subprocess.run(
            ["rclone", "deletefile", dest],
            capture_output=True, text=True,
            env={**os.environ, "RCLONE_CONFIG": RCLONE_CONFIG_PATH},
            timeout=30
        )
        if result.returncode == 0:
            log(job_id, "✓ Incomplete file deleted successfully")
        else:
            log(job_id, f"⚠️  Delete failed: {result.stderr.strip()}")
    except Exception as e:
        log(job_id, f"⚠️  Delete exception: {e}")


def build_cmd(url, filename):
    safe_url  = shlex.quote(url)
    safe_dest = shlex.quote(get_dest(filename))
    referer   = get_referer(url)

    return (
        f"curl -L "
        f"--limit-rate 3M "
        f"--retry 9999 --retry-delay 5 --retry-all-errors "
        f"--speed-limit 1 --speed-time 30 "
        f"--keepalive-time 30 "
        f"--max-time 0 "
        f"-H 'User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36' "
        f"-H 'Referer: {referer}' "
        f"-H 'Accept: */*' "
        f"-H 'Accept-Language: en-US,en;q=0.9' "
        f"-H 'Connection: keep-alive' "
        f"--progress-bar "
        f"{safe_url} | rclone rcat --ignore-checksum --buffer-size 1M {safe_dest}"
    )


def run_job(job):
    job_id   = job["id"]
    url      = job["url"]
    filename = job["filename"]
    dest     = get_dest(filename)

    set_job(job_id, status="running", log="", progress=0, retries=0, started_at=now())
    log(job_id, f"Starting transfer: {filename}")
    log(job_id, f"Source URL: {url}")
    log(job_id, f"Destination: {dest}")

    referer     = get_referer(url)
    source_size = get_source_size(url, referer, job_id)

    # Warn if source size unknown
    if source_size is None:
        log(job_id, "⚠️  Proceeding without size validation")
    elif source_size < MIN_VALID_SIZE:
        log(job_id, f"❌ ABORT: Source file too small ({human_size(source_size)}) — likely an error page, not a real file!")
        set_job(job_id, status="failed", finished_at=now())
        return

    cmd = build_cmd(url, filename)
    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    retry_count = 0

    while True:
        with jobs_lock:
            if jobs.get(job_id, {}).get("status") == "cancelled":
                log(job_id, "Transfer cancelled.")
                # Clean up incomplete file
                delete_remote_file(dest, job_id)
                return

        try:
            log(job_id, f"🚀 Attempt #{retry_count + 1} — starting curl | rclone pipe")
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
                        log(job_id, "Cancelled mid-transfer.")
                        delete_remote_file(dest, job_id)
                        return

                progress = parse_progress(line)
                if progress is not None:
                    set_job(job_id, progress=progress)

                if not is_progress_line(line):
                    clean = line.strip()
                    if clean:
                        # Flag suspicious lines
                        if any(w in clean.lower() for w in ['error', 'failed', 'fatal', 'denied', 'unauthorized', 'forbidden']):
                            log(job_id, f"❌ {clean}")
                        elif any(w in clean.lower() for w in ['notice', 'warn']):
                            log(job_id, f"⚠️  {clean}")
                        else:
                            log(job_id, clean)

            p.wait()

            with processes_lock:
                processes.pop(job_id, None)

            log(job_id, f"curl|rclone exited with code {p.returncode}")

            if p.returncode == 0:
                # ── Validate uploaded file size ──
                log(job_id, "🔎 Validating uploaded file...")
                uploaded_size = get_uploaded_size(dest, job_id)

                if uploaded_size is None:
                    log(job_id, "⚠️  Could not verify upload — marking done anyway")
                    set_job(job_id, status="done", progress=100, finished_at=now())
                    log(job_id, "✓ Transfer complete (unverified)")
                    return

                if uploaded_size < MIN_VALID_SIZE:
                    log(job_id, f"❌ FAKE FILE DETECTED: uploaded only {human_size(uploaded_size)} — deleting and retrying!")
                    delete_remote_file(dest, job_id)
                    retry_count += 1
                    set_job(job_id, retries=retry_count)
                    time.sleep(10)
                    continue

                if source_size and uploaded_size < source_size * 0.95:
                    log(job_id, f"❌ INCOMPLETE UPLOAD: got {human_size(uploaded_size)} of {human_size(source_size)} ({uploaded_size/source_size*100:.1f}%) — deleting and retrying!")
                    delete_remote_file(dest, job_id)
                    retry_count += 1
                    set_job(job_id, retries=retry_count)
                    time.sleep(10)
                    continue

                # Success!
                set_job(job_id, status="done", progress=100, finished_at=now())
                if source_size:
                    log(job_id, f"✓ Transfer complete! {human_size(uploaded_size)} / {human_size(source_size)} ({uploaded_size/source_size*100:.1f}%)")
                else:
                    log(job_id, f"✓ Transfer complete! {human_size(uploaded_size)}")
                return

            else:
                retry_count += 1
                set_job(job_id, retries=retry_count)
                log(job_id, f"❌ Failed (exit {p.returncode}) — retrying in 5s (#{retry_count})")
                # Clean up partial file before retry
                delete_remote_file(dest, job_id)
                time.sleep(5)

        except Exception as e:
            retry_count += 1
            set_job(job_id, retries=retry_count)
            log(job_id, f"❌ Exception: {e} — retrying in 5s (#{retry_count})")
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
                    jobs[job_id]["log"] += f"[{now()}] ❌ Fatal: {e}\n"
            print(f"[FATAL] Job {job_id[:8]}: {e}", flush=True)
        finally:
            job_queue.task_done()


threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == "__main__":
    threading.Event().wait()
