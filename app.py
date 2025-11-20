import os
import requests
import logging
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import concurrent.futures

# --- Настройка логгирования ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_updater.log'),
        logging.StreamHandler()
    ]
)

# --- Загрузка переменных окружения ---
load_dotenv()

# --- Конфигурация через переменные окружения ---
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")

# --- Новые переменные ---
COINGECKO_CHUNK_SIZE = int(os.getenv("COINGECKO_CHUNK_SIZE", 200))  # Сколько монет за 1 запрос в CG
NOTION_SYMBOL_COLUMN_NAME = os.getenv("NOTION_SYMBOL_COLUMN_NAME", "Symbol")  # Колонка с ID монеты (например, "bitcoin")
NOTION_PRICE_COLUMN_NAME = os.getenv("NOTION_PRICE_COLUMN_NAME", "Price")
NOTION_UPDATED_COLUMN_NAME = os.getenv("NOTION_UPDATED_COLUMN_NAME", "Last Updated")

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_coins_from_notion():
    """
    Считывает все страницы из базы Notion.
    Извлекает уникальные 'CoinGecko ID' из указанной колонки (по умолчанию 'Symbol').
    Возвращает set уникальных ID.
    """
    logging.info("🔍 Получение списка криптовалют из Notion...")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    all_coin_ids = set()
    start_cursor = None

    while True:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            response = requests.post(url, json=payload, headers=NOTION_HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            for page in results:
                properties = page.get("properties", {})
                symbol_prop = properties.get(NOTION_SYMBOL_COLUMN_NAME, {})
                # Поддержка RichText и Title
                if symbol_prop.get("type") == "rich_text":
                    text_content = symbol_prop.get("rich_text", [{}])[0].get("text", {}).get("content", "").strip()
                elif symbol_prop.get("type") == "title":
                    text_content = symbol_prop.get("title", [{}])[0].get("text", {}).get("content", "").strip()
                else:
                    text_content = ""

                if text_content:
                    all_coin_ids.add(text_content.lower())  # Нормализуем к нижнему регистру
                else:
                    logging.warning(f"⚠️ Пропущена страница {page['id']}: '{NOTION_SYMBOL_COLUMN_NAME}' пустое или не найдено.")

            if not data.get("has_more"):
                break
            start_cursor = data.get("next_cursor")

        except Exception as e:
            logging.error(f"❌ Ошибка при получении страниц из Notion: {e}")
            break

    logging.info(f"📋 Найдено {len(all_coin_ids)} уникальных ID криптовалют в Notion.")
    return all_coin_ids


def fetch_prices_from_coingecko(coin_ids_list):
    """
    Отправляет запрос в CoinGecko для списка ID.
    Возвращает словарь {coin_id: price}.
    """
    logging.info(f"💸 Запрос цен для {len(coin_ids_list)} криптовалют у CoinGecko...")
    
    # Деление на чанки, если список слишком большой
    chunks = [coin_ids_list[i:i + COINGECKO_CHUNK_SIZE] for i in range(0, len(coin_ids_list), COINGECKO_CHUNK_SIZE)]
    
    all_prices = {}
    for i, chunk in enumerate(chunks):
        ids_str = ",".join(chunk)
        url = f"https://api.coingecko.com/api/v3/simple/price?ids={ids_str}&vs_currencies=usd"
        
        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    all_prices.update({coin_id: data[coin_id]['usd'] for coin_id in data if 'usd' in data[coin_id]})
                    logging.info(f"✅ Получены цены для {len(data)} монет из чанка {i+1}/{len(chunks)}")
                    break
                elif response.status_code == 429:
                    reset_time = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"⏳ Rate limit от CoinGecko. Ожидание {reset_time} секунд...")
                    sleep(reset_time)
                    continue
                else:
                    logging.error(f"❌ Ошибка от CoinGecko (чанк {i+1}): {response.status_code} - {response.text}")
                    if attempt == retries - 1:
                        raise Exception(f"❌ Не удалось получить цены для чанка {i+1} после {retries} попыток.")
            except Exception as e:
                logging.error(f"❌ Ошибка при запросе цен (чанк {i+1}): {e}")
                if attempt == retries - 1:
                    raise e
        sleep(0.1) # Небольшая задержка между чанками

    logging.info(f"📊 Всего получено цен для {len(all_prices)} монет.")
    return all_prices


def get_all_notion_pages_for_update():
    """
    Получает все страницы из Notion, чтобы сопоставить их с ценами.
    Возвращает список словарей: [{'page_id': ..., 'coin_id': ...}, ...]
    """
    logging.info("📋 Получение всех страниц Notion для обновления...")
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    all_pages = []
    start_cursor = None

    while True:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        try:
            response = requests.post(url, json=payload, headers=NOTION_HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            for page in results:
                properties = page.get("properties", {})
                symbol_prop = properties.get(NOTION_SYMBOL_COLUMN_NAME, {})
                
                if symbol_prop.get("type") == "rich_text":
                    text_content = symbol_prop.get("rich_text", [{}])[0].get("text", {}).get("content", "").strip()
                elif symbol_prop.get("type") == "title":
                    text_content = symbol_prop.get("title", [{}])[0].get("text", {}).get("content", "").strip()
                else:
                    text_content = ""

                if text_content:
                    all_pages.append({
                        "page_id": page["id"],
                        "coin_id": text_content.lower() # Нормализуем
                    })
                else:
                    logging.warning(f"⚠️ Пропущена страница {page['id']} для обновления: '{NOTION_SYMBOL_COLUMN_NAME}' пустое.")

            if not data.get("has_more"):
                break
            start_cursor = data.get("next_cursor")

        except Exception as e:
            logging.error(f"❌ Ошибка при получении страниц для обновления: {e}")
            break

    logging.info(f"📋 Найдено {len(all_pages)} страниц для потенциального обновления.")
    return all_pages


def update_single_notion_page(args):
    """
    Обновляет одну страницу Notion.
    Args: (page_id, new_price)
    """
    page_id, new_price = args
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            NOTION_PRICE_COLUMN_NAME: {"number": float(new_price)},
            NOTION_UPDATED_COLUMN_NAME: {"date": {"start": datetime.now().isoformat()}}
        }
    }
    try:
        response = requests.patch(url, json=payload, headers=NOTION_HEADERS, timeout=10)
        response.raise_for_status()
        return True, page_id
    except Exception as e:
        return False, f"{page_id}: {str(e)}"


def update_notion_database():
    """
    Главная функция: получает монеты из Notion, запрашивает цены, обновляет страницы.
    """
    try:
        # 1. Получить список монет из Notion
        coin_ids_from_notion = get_coins_from_notion()
        if not coin_ids_from_notion:
            logging.warning("⚠️ В базе Notion не найдено ни одной монеты для обновления. Завершение.")
            return

        # 2. Запросить цены для этих монет
        prices_map = fetch_prices_from_coingecko(list(coin_ids_from_notion))

        # 3. Получить все страницы для обновления
        pages_to_update = get_all_notion_pages_for_update()

        # 4. Подготовить список задач для обновления
        update_tasks = []
        for page in pages_to_update:
            coin_id = page["coin_id"]
            if coin_id in prices_map:
                update_tasks.append((page["page_id"], prices_map[coin_id]))
            else:
                logging.warning(f"⚠️ Цена для монеты '{coin_id}' не найдена в CoinGecko. Страница {page['page_id']} пропущена.")

        logging.info(f"🔄 Подготовлено {len(update_tasks)} задач на обновление.")

        # 5. Выполнить обновления параллельно
        updated_count = 0
        failed_updates = []
        if update_tasks:
            # Ограничим workers, чтобы не превысить рейт-лимит Notion (3 req/sec)
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                results = list(executor.map(update_single_notion_page, update_tasks))

            for success, info in results:
                if success:
                    updated_count += 1
                    logging.info(f"✅ Обновлена страница {info}")
                else:
                    failed_updates.append(info)
                    logging.error(f"❌ Ошибка обновления: {info}")

        logging.info(f"🎯 ЗАВЕРШЕНО: {updated_count} обновлено, {len(failed_updates)} ошибок.")
        if failed_updates:
            logging.error(f"📋 Список ошибок: {failed_updates}")

    except Exception as e:
        logging.critical("💥 Критическая ошибка в update_notion_database", exc_info=True)
        raise


if __name__ == "__main__":
    update_notion_database()