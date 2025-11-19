# app.py
import os
import time
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime

os.environ["NOTION_API_KEY"] = os.environ.get('NOTION_API_KEY')
os.environ["NOTION_DATABASE_ID"] = os.environ.get('NOTION_DATABASE_ID')
os.environ["COINGECKO_CHUNK_SIZE"] = "200"
os.environ["LOG_LEVEL"] = "INFO"

# Настройка логирования
def setup_logging():
    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

class CoinGeckoClient:
    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, chunk_size: int = 200):
        self.chunk_size = chunk_size

    def get_prices(self, coin_ids: List[str]) -> Dict[str, Any]:
        """Запрашивает цены с CoinGecko, разбивая на чанки."""
        prices = {}
        for i in range(0, len(coin_ids), self.chunk_size):
            chunk = coin_ids[i:i + self.chunk_size]
            params = {
                "ids": ",".join(chunk),
                "vs_currencies": "usd"
            }
            response = self._make_request("simple/price", params)
            if response:
                prices.update(response)
        return prices

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        url = f"{self.BASE_URL}/{endpoint}"
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                logging.warning(f"Попытка {attempt + 1} не удалась для {url}: {e}")
                if attempt == 2:
                    logging.error(f"Не удалось выполнить запрос к CoinGecko: {e}")
                    return None
                time.sleep((2 ** attempt) * 0.5)  # Экспоненциальная задержка
        return None

class NotionClient:
    API_VERSION = "2022-06-28"

    def __init__(self, api_key: str, database_id: str):
        self.api_key = api_key
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Notion-Version": self.API_VERSION,
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.notion.com/v1"

    def get_all_pages(self) -> List[Dict[str, Any]]:
        """Получает все страницы из базы данных с учётом пагинации."""
        pages = []
        url = f"{self.base_url}/databases/{self.database_id}/query"
        payload = {}
        while True:
            try:
                response = requests.post(url, headers=self.headers, json=payload, timeout=10)
                response.raise_for_status()
                data = response.json()
                pages.extend(data.get("results", []))
                if not data.get("has_more"):
                    break
                payload["start_cursor"] = data["next_cursor"]
            except requests.RequestException as e:
                logging.error(f"Ошибка при получении страниц из Notion: {e}")
                raise SystemExit(1)
        return pages

    def update_page(self, page_id: str, properties: Dict[str, Any]) -> bool:
        """Обновляет страницу в Notion с задержкой."""
        url = f"{self.base_url}/pages/{page_id}"
        payload = {"properties": properties}
        try:
            response = requests.patch(url, headers=self.headers, json=payload, timeout=10)
            if response.status_code == 422:
                logging.warning(f"Ошибка валидации при обновлении страницы {page_id}: {response.json()}")
                return False
            response.raise_for_status()
            time.sleep(0.35)  # 350 мс задержка между запросами
            return True
        except requests.RequestException as e:
            logging.error(f"Критическая ошибка при обновлении страницы {page_id}: {e}")
            if response.status_code == 401:
                logging.critical("Ошибка аутентификации в Notion. Проверьте NOTION_API_KEY.")
                raise SystemExit(1)
            return False

def main():
    setup_logging()

    # Проверка обязательных переменных окружения
    notion_api_key = os.getenv("NOTION_API_KEY")
    notion_database_id = os.getenv("NOTION_DATABASE_ID")
    if not notion_api_key:
        logging.critical("Переменная окружения NOTION_API_KEY не установлена.")
        raise SystemExit(1)
    if not notion_database_id:
        logging.critical("Переменная окружения NOTION_DATABASE_ID не установлена.")
        raise SystemExit(1)

    chunk_size = int(os.getenv("COINGECKO_CHUNK_SIZE", 200))

    logging.info("Запуск скрипта обновления цен криптовалют.")

    # Инициализация клиентов
    coingecko = CoinGeckoClient(chunk_size=chunk_size)
    notion = NotionClient(notion_api_key, notion_database_id)

    # Получение всех записей
    pages = notion.get_all_pages()
    logging.info(f"Получено {len(pages)} записей из базы данных Notion.")

    # Извлечение CoinGecko ID и сопоставление с page_id
    coin_records = []
    for page in pages:
        props = page.get("properties", {})
        cg_id_prop = props.get("CoinGecko ID")
        if not cg_id_prop or not cg_id_prop.get("type") == "rich_text" or not cg_id_prop.get("rich_text"):
            continue
        cg_id = cg_id_prop["rich_text"][0].get("text", {}).get("content", "").strip()
        if not cg_id:
            continue
        coin_records.append({
            "page_id": page["id"],
            "coin_id": cg_id,
            "price_prop": props.get("Price"),
            "last_updated_prop": props.get("Last Updated")
        })

    logging.info(f"Найдено {len(coin_records)} записей с указанным CoinGecko ID.")

    if not coin_records:
        logging.info("Нет записей для обновления. Завершение.")
        return

    # Запрос цен с CoinGecko
    coin_ids = [record["coin_id"] for record in coin_records]
    prices = coingecko.get_prices(coin_ids)
    logging.info(f"Получены цены для {len(prices)} из {len(coin_ids)} монет.")

    # Обновление записей в Notion
    updated_count = 0
    for record in coin_records:
        page_id = record["page_id"]
        coin_id = record["coin_id"]
        new_price_data = prices.get(coin_id)

        if not new_price_data or "usd" not in new_price_data:
            logging.debug(f"Цена для {coin_id} не получена от CoinGecko.")
            continue

        new_price = new_price_data["usd"]
        current_price = None
        if record["price_prop"] and record["price_prop"].get("type") == "number":
            current_price = record["price_prop"]["number"]

        if current_price is not None and abs(current_price - new_price) < 1e-10:
            logging.debug(f"Цена для {coin_id} не изменилась: {new_price:.6f}")
            continue

        # Формирование свойств для обновления
        update_props = {}

        # Обновление цены
        if record["price_prop"] and record["price_prop"].get("type") == "number":
            update_props["Price"] = {
                "number": new_price
            }
            logging.info(f"Цена {coin_id}: {current_price} -> {new_price}")

        # Обновление времени
        if record["last_updated_prop"] and record["last_updated_prop"].get("type") == "date":
            now_iso = datetime.utcnow().isoformat() + "Z"  # UTC в формате ISO 8601
            update_props["Last Updated"] = {
                "date": {
                    "start": now_iso,
                    "end": None
                }
            }

        if update_props:
            if notion.update_page(page_id, update_props):
                updated_count += 1
        else:
            logging.debug(f"Нет изменений для {coin_id}")

    logging.info(f"Обновлено {updated_count} записей.")
    logging.info("Завершение работы скрипта.")

if __name__ == "__main__":
    main()