from flask import Flask, request, render_template, jsonify
import uuid
from worker import job_queue, jobs, jobs_lock

app = Flask(__name__)


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/start", methods=["POST"])
def start():
    url = request.form.get("url", "").strip()
    filename = request.form.get("filename", "").strip()

    if not url or not filename:
        return jsonify({"error": "URL and filename are required"}), 400

    if not url.startswith("http://") and not url.startswith("https://"):
        return jsonify({"error": "URL must start with http:// or https://"}), 400

    job_id = str(uuid.uuid4())

    with jobs_lock:
        jobs[job_id] = {
            "status": "queued",
            "log": "",
            "url": url,
            "filename": filename,
        }

    job_queue.put({"id": job_id, "url": url, "filename": filename})

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
    with jobs_lock:
        job = jobs.get(job_id)

    if not job:
        return jsonify({"status": "not_found", "log": ""}), 404

    return jsonify(job)


@app.route("/jobs")
def all_jobs():
    with jobs_lock:
        snapshot = dict(jobs)
    return jsonify(snapshot)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
