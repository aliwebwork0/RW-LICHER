import os
import threading
import subprocess
import shlex
from queue import Queue

job_queue = Queue()
jobs = {}
jobs_lock = threading.Lock()

RCLONE_CONFIG_PATH = "/root/.config/rclone/rclone.conf"


def update_job(job_id, **kwargs):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id].update(kwargs)


def append_log(job_id, text):
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["log"] += text


def run_job(job):
    job_id = job["id"]
    url = job["url"]
    filename = job["filename"]

    update_job(job_id, status="running", log="")

    safe_url = shlex.quote(url)
    safe_dest = shlex.quote(f"mega:/Video/{filename}")
    cmd = f"curl -L --progress-bar {safe_url} | rclone rcat {safe_dest}"

    env = os.environ.copy()
    env["RCLONE_CONFIG"] = RCLONE_CONFIG_PATH

    try:
        p = subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )

        for line in p.stdout:
            append_log(job_id, line)

        p.wait()

        final_status = "done" if p.returncode == 0 else "failed"
        update_job(job_id, status=final_status)

    except Exception as e:
        update_job(job_id, status="failed")
        append_log(job_id, f"Exception: {e}\n")


def worker_loop():
    while True:
        job = job_queue.get()
        try:
            run_job(job)
        except Exception as e:
            with jobs_lock:
                if job["id"] in jobs:
                    jobs[job["id"]]["status"] = "failed"
                    jobs[job["id"]]["log"] += f"Unhandled error: {e}\n"
        finally:
            job_queue.task_done()


threading.Thread(target=worker_loop, daemon=True).start()

if __name__ == "__main__":
    # Keep process alive when run standalone (called from start.sh)
    threading.Event().wait()
