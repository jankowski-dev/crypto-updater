# start.sh
#!/bin/bash

while true; do
  exec python3 app.py || echo "[$(date)] Ошибка при выполнении app.py, повторяем через 60 секунд." >&2
  sleep 60
done