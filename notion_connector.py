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
        self.coingecko_api = CoinGeckoAPI()
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
            
            # Получаем записи из базы
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
    
    def get_database_records_simple(self):
        """Упрощенное получение записей из базы"""
        logger.info("=== ПОЛУЧЕНИЕ ЗАПИСЕЙ (УПРОЩЕННЫЙ МЕТОД) ===")
        
        try:
            # Попробуем разные методы для получения записей
            
            # Метод 1: Попробуем query с базовыми параметрами
            logger.info("Пробуем метод 1: databases.query")
            try:
                result = self.client.databases.query(
                    database_id=self.database_id,
                    page_size=100
                )
                records = result.get('results', [])
                logger.info(f"Метод 1 успешен! Получено {len(records)} записей")
                
                if records:
                    self.analyze_simple_records(records)
                    return
                    
            except Exception as e:
                logger.warning(f"Метод 1 не сработал: {e}")
            
            # Метод 2: Попробуем получить через страницы
            logger.info("Пробуем метод 2: через страницы")
            try:
                # Получаем все страницы
                pages_result = self.client.search(
                    filter={
                        'value': 'page',
                        'property': 'object'
                    },
                    sort={
                        'direction': 'descending',
                        'timestamp': 'last_edited_time'
                    }
                )
                
                pages = pages_result.get('results', [])
                database_pages = []
                
                # Фильтруем страницы по базе
                for page in pages:
                    parent = page.get('parent', {})
                    if parent.get('database_id') == self.database_id:
                        database_pages.append(page)
                
                logger.info(f"Метод 2: найдено {len(database_pages)} страниц в базе")
                
                if database_pages:
                    self.analyze_simple_records(database_pages)
                    return
                    
            except Exception as e:
                logger.warning(f"Метод 2 не сработал: {e}")
            
            logger.warning("Ни один метод не сработал для получения записей")
            
            # Метод 3: Получаем содержимое блока-родителя
            self.try_method_3_blocks()
            
        except Exception as e:
            logger.error(f"Ошибка в упрощенном получении записей: {e}")
    
    def try_get_child_database_records(self, child_db_id):
        """Пробуем получить записи из дочерней базы"""
        logger.info(f"Пробуем получить записи из дочерней базы: {child_db_id}")
        
        try:
            # Попробуем query для дочерней базы
            result = self.client.databases.query(
                database_id=child_db_id,
                page_size=100
            )
            records = result.get('results', [])
            logger.info(f"Найдено записей в дочерней базе: {len(records)}")
            
            if records:
                self.analyze_simple_records(records)
                
        except Exception as e:
            logger.error(f"Ошибка при получении записей дочерней базы: {e}")
    
    def try_method_3_blocks(self):
        """Метод 3: получаем содержимое блока-родителя"""
        logger.info("Пробуем метод 3: через блок-родитель")
        try:
            # Получаем информацию о базе для получения parent_block_id
            database = self.client.databases.retrieve(database_id=self.database_id)
            parent_block_id = database.get('parent', {}).get('block_id')
            
            if parent_block_id:
                logger.info(f"Получаем содержимое блока: {parent_block_id}")
                blocks_result = self.client.blocks.children.list(block_id=parent_block_id)
                blocks = blocks_result.get('results', [])
                logger.info(f"Метод 3: найдено {len(blocks)} блоков")
                
                # Ищем базу данных среди блоков
                for block in blocks:
                    if block.get('type') == 'child_database':
                        logger.info(f"Найдена дочерняя база: {block.get('id')}")
                        # Попробуем получить записи из этой базы
                        self.try_get_child_database_records(block.get('id'))
                        return
            else:
                logger.warning("Не найден parent_block_id")
                
        except Exception as e:
            logger.warning(f"Метод 3 не сработал: {e}")
            
        except Exception as e:
            logger.error(f"Ошибка в упрощенном получении записей: {e}")
    
    def analyze_simple_records(self, records):
        """Анализ записей в упрощенном режиме"""
        logger.info("=== АНАЛИЗ ЗАПИСЕЙ (УПРОЩЕННЫЙ) ===")
        
        cryptocurrencies = []
        
        for i, record in enumerate(records[:5]):  # Ограничиваем до 5 записей для отладки
            logger.info(f"Анализируем запись {i+1}: {record.get('id', 'NO_ID')}")
            
            # Ищем название в разных местах
            crypto_name = None
            crypto_symbol = None
            
            # Проверяем title
            if record.get('properties'):
                for field_name, field_value in record['properties'].items():
                    if field_name.lower() in ['name', 'название']:
                        if field_value.get('title'):
                            crypto_name = field_value['title'][0]['plain_text']
                    elif field_name.lower() in ['symbol', 'символ']:
                        if field_value.get('rich_text'):
                            crypto_symbol = field_value['rich_text'][0]['plain_text']
            
            # Если не нашли в properties, ищем в других местах
            if not crypto_name:
                # Проверяем заголовок страницы
                if record.get('properties', {}).get('title'):
                    crypto_name = record['properties']['title'][0]['plain_text']
                elif record.get('url'):
                    # Используем часть URL как название
                    crypto_name = f"Запись_{i+1}"
            
            if crypto_name:
                crypto_data = {
                    'name': crypto_name,
                    'symbol': crypto_symbol or '',
                    'page_id': record['id']
                }
                cryptocurrencies.append(crypto_data)
                logger.info(f"Найдена криптовалюта: {crypto_name} ({crypto_symbol})")
        
        logger.info(f"Всего найдено криптовалют: {len(cryptocurrencies)}")
        
        # Сохраняем список криптовалют
        self.cryptocurrencies = cryptocurrencies
        
        logger.info("=== КОНЕЦ УПРОЩЕННОГО АНАЛИЗА ===")
    
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
    
    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Notion-Crypto-Tracker/1.0'
        })
    
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
            self.update_notion_database(updated_cryptos)
        
        return updated_cryptos
    
    def update_notion_database(self, updated_cryptos: List[Dict[str, Any]]):
        """Обновляет курсы в базе данных Notion"""
        logger.info("=== ОБНОВЛЕНИЕ КУРСОВ В NOTION ===")
        
        success_count = 0
        error_count = 0
        
        for crypto in updated_cryptos:
            page_id = crypto['page_id']
            price = crypto['price_usd']
            crypto_name = crypto['name']
            
            if not price:
                logger.warning(f"Пропускаем {crypto_name}: нет цены")
                continue
            
            try:
                # Подготавливаем данные для обновления
                current_time = datetime.now().isoformat()
                
                payload = {
                    "properties": {
                        "Price": {"number": float(price)},
                        "Last Updated": {"date": {"start": current_time}}
                    }
                }
                
                # Отправляем PATCH запрос для обновления записи
                url = f"{self.base_url}/pages/{page_id}"
                response = requests.patch(url, json=payload, headers=self.headers)
                response.raise_for_status()
                
                success_count += 1
                logger.info(f"✅ Обновлен {crypto_name}: ${price:,.2f}")
                
                # Небольшая задержка между обновлениями
                time.sleep(0.5)
                
            except Exception as e:
                error_count += 1
                logger.error(f"❌ Ошибка обновления {crypto_name}: {e}")
        
        logger.info(f"=== ОБНОВЛЕНИЕ NOTION ЗАВЕРШЕНО ===")
        logger.info(f"✅ Успешно обновлено: {success_count}")
        logger.info(f"❌ Ошибок: {error_count}")
        logger.info(f"📊 Всего обработано: {len(updated_cryptos)}")
    
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