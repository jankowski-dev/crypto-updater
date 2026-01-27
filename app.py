import os
import requests
import logging
from datetime import datetime, timedelta
from time import sleep
import threading
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

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
COINGECKO_CHUNK_SIZE = int(os.getenv("COINGECKO_CHUNK_SIZE", 200))
NOTION_SYMBOL_COLUMN_NAME = os.getenv("NOTION_SYMBOL_COLUMN_NAME", "Symbol")
NOTION_PRICE_COLUMN_NAME = os.getenv("NOTION_PRICE_COLUMN_NAME", "Price")
NOTION_UPDATED_COLUMN_NAME = os.getenv("NOTION_UPDATED_COLUMN_NAME", "Last Updated")
# --- НОВАЯ ПЕРЕМЕННАЯ ---
NOTION_YESTERDAY_PRICE_COLUMN_NAME = os.getenv("NOTION_YESTERDAY_PRICE_COLUMN_NAME", "Price (Yesterday)")
# ---

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

# --- Основная логика обновления ---

def get_coins_from_notion():
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
                if symbol_prop.get("type") == "rich_text":
                    text_content = symbol_prop.get("rich_text", [{}])[0].get("text", {}).get("content", "").strip()
                elif symbol_prop.get("type") == "title":
                    text_content = symbol_prop.get("title", [{}])[0].get("text", {}).get("content", "").strip()
                else:
                    text_content = ""

                if text_content:
                    all_coin_ids.add(text_content.lower())
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
    logging.info(f"💸 Запрос текущих и вчерашних цен для {len(coin_ids_list)} криптовалют у CoinGecko...")
    chunks = [coin_ids_list[i:i + COINGECKO_CHUNK_SIZE] for i in range(0, len(coin_ids_list), COINGECKO_CHUNK_SIZE)]

    all_current_prices = {}
    all_yesterday_prices = {}

    for i, chunk in enumerate(chunks):
        ids_str = ','.join(chunk)
        # --- ИСПОЛЬЗУЕМ /coins/markets вместо /simple/price ---
        url = f"https://api.coingecko.com/api/v3/coins/markets"
        params = {
            'vs_currency': 'usd',
            'ids': ids_str,
            'sparkline': 'false',
            'price_change_percentage': '24h'
        }

        retries = 3
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    for coin_data in data:
                        coin_id = coin_data.get('id')
                        if coin_id:
                            # Текущая цена
                            current_price = coin_data.get('current_price')
                            if current_price is not None:
                                all_current_prices[coin_id] = current_price
                            else:
                                logging.warning(f"⚠️ Текущая цена для {coin_id} не найдена.")

                            # --- ЛУЧШИЙ ПОДХОД: ОТДЕЛЬНЫЙ ЗАПРОС К /coins/{id}/history ---
                            yesterday_date = (datetime.utcnow() - timedelta(days=1)).strftime('%d-%m-%Y') # Формат dd-mm-yyyy для Coingecko
                            history_url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
                            history_params = {'date': yesterday_date}

                            hist_retries = 2
                            for hist_attempt in range(hist_retries):
                                try:
                                    hist_response = requests.get(history_url, params=history_params, timeout=10)
                                    if hist_response.status_code == 200:
                                        hist_data = hist_response.json()
                                        market_data = hist_data.get('market_data', {})
                                        yesterday_price_data = market_data.get('current_price', {})
                                        yesterday_usd_price = yesterday_price_data.get('usd')

                                        if yesterday_usd_price is not None:
                                            all_yesterday_prices[coin_id] = yesterday_usd_price
                                            logging.debug(f"📅 {coin_id} - Вчерашняя цена (из history): {yesterday_usd_price}")
                                        else:
                                            logging.warning(f"⚠️ Вчерашняя цена для {coin_id} не найдена в history.")
                                        break # Успешно получили цену или её нет, выходим из попыток истории
                                    elif hist_response.status_code == 429:
                                        reset_time = int(hist_response.headers.get('Retry-After', 60))
                                        logging.warning(f"⏳ Rate limit для history {coin_id}. Ожидание {reset_time} секунд...")
                                        sleep(reset_time)
                                        continue
                                    else:
                                        logging.warning(f"⚠️ Ошибка при получении истории для {coin_id}: {hist_response.status_code} - {hist_response.text}")
                                except Exception as e_hist:
                                    logging.error(f"❌ Ошибка при запросе истории (coin: {coin_id}, attempt {hist_attempt+1}): {e_hist}")
                                if hist_attempt == hist_retries - 1:
                                     logging.warning(f"⚠️ Не удалось получить историю для {coin_id} после {hist_retries} попыток.")
                                     # Если не нашли вчерашнюю цену, не добавляем её в словарь (или можно добавить None)
                                     # all_yesterday_prices[coin_id] = None
                        else:
                             logging.warning(f"⚠️ ID монеты не найден в данных markets для chunk {i+1}.")

                    logging.info(f"✅ Получены цены из markets для чанка {i+1}/{len(chunks)}")
                    break # Успешно получил данные markets, выходим из попыток
                elif response.status_code == 429:
                    reset_time = int(response.headers.get('Retry-After', 60))
                    logging.warning(f"⏳ Rate limit от CoinGecko (markets). Ожидание {reset_time} секунд...")
                    sleep(reset_time)
                    continue
                else:
                    logging.error(f"❌ Ошибка от CoinGecko (markets, чанк {i+1}): {response.status_code} - {response.text}")
                    if attempt == retries - 1:
                        raise Exception(f"❌ Не удалось получить цены markets для чанка {i+1} после {retries} попыток.")
            except Exception as e:
                logging.error(f"❌ Ошибка при запросе markets (чанк {i+1}): {e}")
                if attempt == retries - 1:
                    raise e
        sleep(0.1) # Небольшая задержка между чанками

    logging.info(f"📊 Всего получено текущих цен для {len(all_current_prices)} монет.")
    logging.info(f"📊 Всего получено вчерашних цен для {len(all_yesterday_prices)} монет.")
    return all_current_prices, all_yesterday_prices # Возвращаем оба словаря


def get_all_notion_pages_for_update():
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
                        "coin_id": text_content.lower()
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
    page_id, new_current_price, new_yesterday_price = args # Принимаем три аргумента
    url = f"https://api.notion.com/v1/pages/{page_id}"
    payload = {
        "properties": {
            NOTION_PRICE_COLUMN_NAME: {"number": float(new_current_price)}, # Текущая цена
            NOTION_UPDATED_COLUMN_NAME: {"date": {"start": datetime.now().isoformat()}}
        }
    }
    # --- Добавляем вчерашнюю цену в payload, если она есть ---
    if new_yesterday_price is not None:
        payload["properties"][NOTION_YESTERDAY_PRICE_COLUMN_NAME] = {"number": float(new_yesterday_price)}
    # ---

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
            logging.warning("⚠️ В базе Notion не найдено ни одной монеты для обновления. Пропуск.")
            return

        # 2. Запросить цены для этих монет (изменили вызов)
        current_prices_map, yesterday_prices_map = fetch_prices_from_coingecko(list(coin_ids_from_notion))

        # 3. Получить все страницы для обновления
        pages_to_update = get_all_notion_pages_for_update()

        # 4. Подготовить список задач для обновления (изменили формирование задачи)
        update_tasks = []
        for page in pages_to_update:
            coin_id = page["coin_id"]
            current_price = current_prices_map.get(coin_id)
            # --- Получаем вчерашнюю цену ---
            yesterday_price = yesterday_prices_map.get(coin_id) # Может быть None
            # ---

            if current_price is not None: # Обязательно должна быть текущая цена
                # Передаем три значения: page_id, current_price, yesterday_price (может быть None)
                update_tasks.append((page["page_id"], current_price, yesterday_price))
            else:
                logging.warning(f"⚠️ Текущая цена для монеты '{coin_id}' не найдена в CoinGecko. Страница {page['page_id']} пропущена.")

        logging.info(f"🔄 Подготовлено {len(update_tasks)} задач на обновление.")

        # 5. Выполнить обновления
        updated_count = 0
        failed_updates = []
        if update_tasks:
            with ThreadPoolExecutor(max_workers=3) as executor: # Уменьшил workers, т.к. теперь запросов больше
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

# --- Фоновая задача (как в боте) ---
def notion_scheduler():
    """
    Фоновая задача: обновляет Notion каждые N секунд.
    """
    # Интервал в секундах (например, 120 = 2 минуты)
    UPDATE_INTERVAL_SECONDS = int(os.getenv("UPDATE_INTERVAL_SECONDS", 300))  # 5 минут по умолчанию
    logging.info(f"⏰ Запуск планировщика Notion. Интервал: {UPDATE_INTERVAL_SECONDS} секунд.")
    while True:
        try:
            update_notion_database()
        except KeyboardInterrupt:
            logging.info("🛑 Скрипт остановлен пользователем.")
            break
        except Exception as e:
            logging.error(f"❌ Ошибка в основном цикле: {e}", exc_info=True)
        logging.info(f"⏰ Ожидание {UPDATE_INTERVAL_SECONDS} секунд перед следующим обновлением...")
        sleep(UPDATE_INTERVAL_SECONDS)

if __name__ == "__main__":
    # Запускаем планировщик в основном потоке
    notion_scheduler()
