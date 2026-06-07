#!/bin/sh
set -e

# نصب aria2 برای دانلود چندگیگی
apt-get update && apt-get install -y aria2

mkdir -p /root/.config/rclone

if [ -z "$RCLONE_CONF" ]; then
    echo "ERROR: RCLONE_CONF environment variable is not set"
    exit 1
fi

printf "%b" "$RCLONE_CONF" > /root/.config/rclone/rclone.conf
chmod 600 /root/.config/rclone/rclone.conf

export RCLONE_CONFIG=/root/.config/rclone/rclone.conf

echo "==> rclone config written successfully"
echo "==> aria2 version:"
aria2c --version

echo "==> Starting worker..."
python3 worker.py &

echo "==> Starting gunicorn..."
exec gunicorn -b 0.0.0.0:${PORT:-8080} --workers 1 --threads 4 app:app
