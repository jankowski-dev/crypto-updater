# start.sh
#!/bin/bash

while true; do
  echo "Запуск обновления цен..."
  python app.py
  echo "Ожидание 60 секунд перед следующим запуском..."
  sleep 60
done