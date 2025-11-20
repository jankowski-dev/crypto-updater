#!/usr/bin/env python3
"""
Скрипт для подключения к таблице Notion
Использует переменные среды Railway: NOTION_TOKEN и NOTION_DATABASE_ID
"""

import os
import sys
import logging
import requests
import json
import time
import concurrent.futures
from datetime import datetime
from typing import Optional, List, Dict, Any

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class NotionConnector:
    """Класс для работы с подключением к Notion"""
    
    def __init__(self):
        self.token: Optional[str] = None
        self.database_id: Optional[str] = None
        self.base_url = "https://api.notion.com/v1"
        self.headers = {}
        self.coingecko_api = None
        self.cryptocurrencies = []
        
    def load_environment_variables(self) -> bool:
        """Загружает переменные среды"""
        try:
            self.token = os.getenv('NOTION_TOKEN')
            self.database_id = os.getenv('NOTION_DATABASE_ID')
            
            if not self.token:
                logger.error("Переменная среды NOTION_TOKEN не найдена")
                return False
                
            if not self.database_id:
                logger.error("Переменная среды NOTION_DATABASE_ID не найдена")
                return False
                
            logger.info("Переменные среды успешно загружены")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке переменных среды: {e}")
            return False
    
    def initialize_client(self) -> bool:
        """Инициализирует HTTP клиент для Notion"""
        try:
            self.headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
                "Notion-Version": "2022-06-28"
            }
            
            # Инициализируем CoinGeckoAPI с правильными параметрами
            self.coingecko_api = CoinGeckoAPI(notion_headers=self.headers, notion_base_url=self.base_url)
            
            logger.info("HTTP клиент Notion успешно инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации клиента Notion: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Тестирует подключение к базе данных Notion"""
        try:
            if not self.headers:
                logger.error("Клиент Notion не инициализирован")
                return False
            
            # Пробуем получить информацию о базе через HTTP запрос
            url = f"{self.base_url}/databases/{self.database_id}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            database = response.json()
            
            logger.info("Подключение к Notion успешно!")
            logger.info(f"Название базы данных: {database.get('title', [{}])[0].get('plain_text', 'Неизвестно')}")
            logger.info(f"ID базы данных: {self.database_id}")
            
            # Анализируем структуру базы данных
            self.analyze_database_structure(database)
            
            # Получаем записи из базы через прямые HTTP запросы
            self.get_database_records()
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных Notion: {e}")
            return False
    
    def analyze_database_structure(self, database):
        """Анализирует структуру базы данных"""
        logger.info("=== АНАЛИЗ СТРУКТУРЫ БАЗЫ ДАННЫХ ===")
        
        logger.info(f"Полная информация о базе: {database}")
        
        properties = database.get('properties', {})
        logger.info(f"Количество полей в базе: {len(properties)}")
        
        if not properties:
            logger.warning("Поля базы данных не найдены или недоступны")
        else:
            for field_name, field_info in properties.items():
                field_type = field_info.get('type', 'unknown')
                logger.info(f"Поле: '{field_name}' - Тип: {field_type}")
        
        logger.info("=== КОНЕЦ АНАЛИЗА СТРУКТУРЫ ===")
    
    def get_database_records(self):
        """Получает записи из базы данных через HTTP запрос"""
        logger.info("=== ПОЛУЧЕНИЕ ЗАПИСЕЙ ИЗ БАЗЫ ===")
        
        try:
            # Получаем все записи из базы одним запросом (как в работающем примере)
            url = f"{self.base_url}/databases/{self.database_id}/query"
            all_pages = []
            start_cursor = None
            
            while True:
                payload = {"page_size": 100}
                if start_cursor:
                    payload["start_cursor"] = start_cursor
                
                response = requests.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
                data = response.json()
                all_pages.extend(data.get("results", []))
                
                if data.get("has_more") and data.get("next_cursor"):
                    start_cursor = data.get("next_cursor")
                else:
                    break
            
            logger.info(f"Всего записей в базе: {len(all_pages)}")
            
            # Анализируем записи для поиска криптовалют
            self.analyze_cryptocurrencies(all_pages)
            
            # Обновляем курсы криптовалют
            self.update_crypto_prices()
            
        except Exception as e:
            logger.error(f"Ошибка при получении записей: {e}")
    
    def analyze_cryptocurrencies(self, records):
        """Анализирует записи для поиска криптовалют"""
        logger.info("=== ПОИСК КРИПТОВАЛЮТ ===")
        
        cryptocurrencies = []
        
        for record in records:
            # Получаем название криптовалюты
            crypto_name = None
            crypto_symbol = None
            
            # Ищем поля с названием и символом
            for field_name, field_value in record.get('properties', {}).items():
                if field_name.lower() in ['name', 'название', 'crypto', 'coin', 'currency']:
                    if field_value.get('title'):
                        crypto_name = field_value['title'][0]['plain_text']
                elif field_name.lower() in ['symbol', 'символ', 'ticker']:
                    if field_value.get('rich_text'):
                        crypto_symbol = field_value['rich_text'][0]['plain_text']
            
            if crypto_name:
                crypto_data = {
                    'name': crypto_name,
                    'symbol': crypto_symbol or '',
                    'page_id': record['id']
                }
                cryptocurrencies.append(crypto_data)
                logger.info(f"Найдена криптовалюта: {crypto_name} ({crypto_symbol})")
        
        logger.info(f"Всего найдено криптовалют: {len(cryptocurrencies)}")
        
        # Сохраняем список криптовалют для дальнейшего использования
        self.cryptocurrencies = cryptocurrencies
        
        logger.info("=== КОНЕЦ ПОИСКА КРИПТОВАЛЮТ ===")
    
    def analyze_cryptocurrencies(self, records):
        """Анализирует записи для поиска криптовалют"""
        logger.info("=== ПОИСК КРИПТОВАЛЮТ ===")
        
        cryptocurrencies = []
        
        for record in records:
            # Получаем название криптовалюты
            crypto_name = None
            crypto_symbol = None
            
            # Ищем поля с названием и символом
            for field_name, field_value in record.get('properties', {}).items():
                if field_name.lower() in ['name', 'название', 'title']:
                    if field_value.get('title'):
                        crypto_name = field_value['title'][0]['plain_text']
                elif field_name.lower() in ['symbol', 'символ']:
                    if field_value.get('rich_text'):
                        crypto_symbol = field_value['rich_text'][0]['plain_text']
            
            if crypto_name:
                crypto_data = {
                    'name': crypto_name,
                    'symbol': crypto_symbol or '',
                    'page_id': record['id']
                }
                cryptocurrencies.append(crypto_data)
                logger.info(f"Найдена криптовалюта: {crypto_name} ({crypto_symbol})")
        
        logger.info(f"Всего найдено криптовалют: {len(cryptocurrencies)}")
        
        # Сохраняем список криптовалют для дальнейшего использования
        self.cryptocurrencies = cryptocurrencies
        
        logger.info("=== КОНЕЦ ПОИСКА КРИПТОВАЛЮТ ===")
    
    def update_crypto_prices(self) -> bool:
        """Обновляет курсы криптовалют из CoinGecko"""
        try:
            if not self.cryptocurrencies:
                logger.warning("Список криптовалют пуст. Сначала нужно просканировать базу данных.")
                return False
            
            logger.info("Начинаем обновление курсов криптовалют...")
            
            # Обновляем курсы через CoinGecko API
            updated_data = self.coingecko_api.update_crypto_rates(self.cryptocurrencies)
            
            if updated_data:
                logger.info(f"✅ Успешно обновлены курсы для {len(updated_data)} криптовалют")
                
                # Сохраняем обновленные данные
                self.updated_crypto_data = updated_data
                
                return True
            else:
                logger.error("❌ Не удалось обновить курсы криптовалют")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при обновлении курсов: {e}")
            return False
        
    def run_connection_test(self) -> bool:
        """Запускает полный тест подключения"""
        logger.info("Начинаем тестирование подключения к Notion...")

        # Загружаем переменные среды
        if not self.load_environment_variables():
            return False
        
        # Инициализируем клиент
        if not self.initialize_client():
            return False
        
        # Тестируем подключение
        if not self.test_connection():
            return False
        
        logger.info("Все тесты пройдены успешно!")
        return True
        

class CoinGeckoAPI:
    """Класс для работы с CoinGecko API"""
    
    def __init__(self, notion_headers=None, notion_base_url=None):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Notion-Crypto-Tracker/1.0'
        })
        # Параметры для обновления Notion
        self.notion_headers = notion_headers
        self.notion_base_url = notion_base_url
    
    def search_cryptocurrency(self, name: str, symbol: str = None) -> Optional[Dict[str, Any]]:
        """Поиск криптовалюты по названию или символу"""
        try:
            # Сначала пробуем найти по символу
            if symbol:
                logger.info(f"Поиск криптовалюты по символу: {symbol}")
                search_url = f"{self.base_url}/search"
                params = {'query': symbol}
                
                response = self.session.get(search_url, params=params)
                response.raise_for_status()
                
                data = response.json()
                coins = data.get('coins', [])
                
                # Ищем точное совпадение по символу
                for coin in coins:
                    if coin.get('symbol', '').upper() == symbol.upper():
                        logger.info(f"Найдена криптовалюта по символу: {coin['name']} ({coin['symbol']})")
                        return coin
            
            # Если не нашли по символу, ищем по названию
            logger.info(f"Поиск криптовалюты по названию: {name}")
            search_url = f"{self.base_url}/search"
            params = {'query': name}
            
            response = self.session.get(search_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            coins = data.get('coins', [])
            
            # Ищем точное совпадение по названию
            for coin in coins:
                if coin.get('name', '').lower() == name.lower():
                    logger.info(f"Найдена криптовалюта по названию: {coin['name']} ({coin['symbol']})")
                    return coin
            
            # Если точное совпадение не найдено, берем первую из результатов
            if coins:
                coin = coins[0]
                logger.info(f"Используем ближайшее совпадение: {coin['name']} ({coin['symbol']})")
                return coin
            
            logger.warning(f"Криптовалюта не найдена: {name} ({symbol})")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при поиске криптовалюты {name}: {e}")
            return None
    
    def get_price_data(self, coin_id: str, vs_currency: str = 'usd') -> Optional[Dict[str, Any]]:
        """Получает данные о цене криптовалюты"""
        try:
            url = f"{self.base_url}/simple/price"
            params = {
                'ids': coin_id,
                'vs_currencies': vs_currency,
                'include_24hr_change': 'true',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true'
            }
            
            response = self.session.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if coin_id in data:
                price_info = data[coin_id]
                logger.info(f"Получены данные для {coin_id}: ${price_info.get(vs_currency, 'N/A')}")
                return price_info
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка при получении цены для {coin_id}: {e}")
            return None
    
    def update_crypto_rates(self, cryptocurrencies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Обновляет курсы для списка криптовалют с оптимизацией запросов"""
        logger.info("=== ОБНОВЛЕНИЕ КУРСОВ КРИПТОВАЛЮТ ===")
        
        # Сначала ищем все криптовалюты
        logger.info("Этап 1: Поиск криптовалют в CoinGecko...")
        coin_mapping = {}
        
        for crypto in cryptocurrencies:
            crypto_name = crypto['name']
            crypto_symbol = crypto['symbol']
            page_id = crypto['page_id']
            
            logger.info(f"Поиск: {crypto_name} ({crypto_symbol})")
            
            # Ищем криптовалюту в CoinGecko
            coin_info = self.search_cryptocurrency(crypto_name, crypto_symbol)
            
            if coin_info:
                coin_mapping[coin_info['id']] = {
                    'page_id': page_id,
                    'name': crypto_name,
                    'symbol': crypto_symbol,
                    'coingecko_id': coin_info['id']
                }
                logger.info(f"✅ Найден: {coin_info['name']} ({coin_info['symbol']})")
            else:
                logger.warning(f"❌ Не найден: {crypto_name}")
            
            # Задержка между поисковыми запросами (сильно увеличиваем)
            time.sleep(5)
        
        if not coin_mapping:
            logger.warning("Не найдено ни одной криптовалюты")
            return []
        
        # Получаем цены батчами
        logger.info(f"Этап 2: Получение цен для {len(coin_mapping)} криптовалют...")
        updated_cryptos = []
        coin_ids = list(coin_mapping.keys())
        
        # Разбиваем на батчи по 10 монет (лимит CoinGecko)
        batch_size = 10
        for i in range(0, len(coin_ids), batch_size):
            batch = coin_ids[i:i + batch_size]
            logger.info(f"Обрабатываем батч {i//batch_size + 1}: {len(batch)} монет")
            
            # Получаем цены для батча
            batch_prices = self.get_batch_prices(batch)
            
            # Обрабатываем результаты
            for coin_id in batch:
                if coin_id in batch_prices:
                    price_data = batch_prices[coin_id]
                    coin_info = coin_mapping[coin_id]
                    
                    updated_crypto = {
                        'page_id': coin_info['page_id'],
                        'name': coin_info['name'],
                        'symbol': coin_info['symbol'],
                        'coingecko_id': coin_id,
                        'price_usd': price_data.get('usd'),
                        'price_change_24h': price_data.get('usd_24h_change'),
                        'market_cap': price_data.get('usd_market_cap'),
                        'volume_24h': price_data.get('usd_24h_vol')
                    }
                    
                    updated_cryptos.append(updated_crypto)
                    
                    # Логируем результаты
                    price = updated_crypto['price_usd']
                    change_24h = updated_crypto['price_change_24h']
                    
                    if price:
                        logger.info(f"✅ {coin_info['name']}: ${price:,.2f}")
                        if change_24h is not None:
                            change_symbol = "📈" if change_24h > 0 else "📉"
                            logger.info(f"   {change_symbol} 24h изменение: {change_24h:+.2f}%")
                    else:
                        logger.warning(f"⚠️ {coin_info['name']}: цена не найдена")
            
            # Задержка между батчами (увеличиваем)
            if i + batch_size < len(coin_ids):
                logger.info("Пауза между батчами...")
                time.sleep(10)
        
        logger.info(f"=== ОБНОВЛЕНИЕ ЗАВЕРШЕНО. Обработано {len(updated_cryptos)} криптовалют ===")
        
        # Обновляем курсы в Notion БД
        if updated_cryptos:
            self.update_notion_database(updated_cryptos, self.notion_headers, self.notion_base_url)
        
        return updated_cryptos
    
    def update_single_page(self, update_data):
        """Обновляет одну страницу в Notion"""
        page_id, price, symbol = update_data
        url = f"{self.notion_base_url}/pages/{page_id}"
        payload = {
            "properties": {
                "Price": {"number": float(price)},
                "Last Updated": {"date": {"start": datetime.now().isoformat()}}
            }
        }
        
        try:
            response = requests.patch(url, json=payload, headers=self.notion_headers, timeout=10)
            response.raise_for_status()
            return symbol, True, None
        except Exception as e:
            return symbol, False, str(e)
    
    def update_notion_database(self, updated_cryptos: List[Dict[str, Any]], notion_headers: Dict[str, str], notion_base_url: str):
        """Обновляем базу данных Notion ОПТИМИЗИРОВАННО"""
        try:
            # Подготавливаем данные для обновления
            updates_to_do = []
            for crypto in updated_cryptos:
                if crypto['price_usd']:
                    updates_to_do.append((crypto['page_id'], crypto['price_usd'], crypto['symbol']))
            
            logger.info(f"=== ОБНОВЛЕНИЕ КУРСОВ В NOTION ===")
            logger.info(f"🔄 Планируем: {len(updates_to_do)} обновлений")
            
            # ВЫПОЛНЯЕМ ВСЕ ОБНОВЛЕНИЯ ПАРАЛЛЕЛЬНО
            updated_count = 0
            if updates_to_do:
                # Используем ThreadPool для параллельных запросов
                with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                    # Передаем self как первый аргумент для метода update_single_page
                    results = list(executor.map(lambda data: self.update_single_page(data), updates_to_do))
                    
                    for symbol, success, error in results:
                        if success:
                            updated_count += 1
                            logger.info(f"✅ Updated {symbol}")
                        else:
                            logger.error(f"❌ Failed to update {symbol}: {error}")
            
            logger.info(f"🎯 COMPLETED: {updated_count} updated")
            
        except Exception as e:
            logger.error(f"💥 Fatal error in Notion update: {e}")
            raise
    
    def get_batch_prices(self, coin_ids: List[str]) -> Dict[str, Any]:
        """Получает цены для списка монет одним запросом с retry"""
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                coin_ids_str = ",".join(coin_ids)
                url = f"{self.base_url}/simple/price"
                params = {
                    'ids': coin_ids_str,
                    'vs_currencies': 'usd',
                    'include_24hr_change': 'true',
                    'include_market_cap': 'true',
                    'include_24hr_vol': 'true'
                }

                response = self.session.get(url, params=params)
                response.raise_for_status()
                
                data = response.json()
                logger.info(f"Получены данные для {len(data)} монет")
                return data
                
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 5  # Exponential backoff
                        logger.warning(f"429 ошибка, ждем {wait_time} сек перед повтором...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Превышено количество попыток для получения цен")
                        return {}
                else:
                    logger.error(f"HTTP ошибка: {e}")
                    return {}
            except Exception as e:
                logger.error(f"Ошибка при получении цен для батча: {e}")
                return {}
        
        return {}
    

def main():
    """Основная функция"""
    connector = NotionConnector()
    
    try:
        success = connector.run_connection_test()
        
        if success:
            logger.info("✅ Подключение к Notion установлено успешно")
            sys.exit(0)
        else:
            logger.error("❌ Не удалось подключиться к Notion")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Выполнение прервано пользователем")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()