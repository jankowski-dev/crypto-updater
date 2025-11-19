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
from typing import Optional, List, Dict, Any
from notion_client import Client

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
        self.client: Optional[Client] = None
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
        """Инициализирует клиент Notion"""
        try:
            self.client = Client(auth=self.token)
            logger.info("Клиент Notion успешно инициализирован")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации клиента Notion: {e}")
            return False
    
    def test_connection(self) -> bool:
        """Тестирует подключение к базе данных Notion"""
        try:
            if not self.client:
                logger.error("Клиент Notion не инициализирован")
                return False
            
            # Пробуем получить информацию о базе
            database = self.client.databases.retrieve(database_id=self.database_id)
            
            logger.info("Подключение к Notion успешно!")
            logger.info(f"Название базы данных: {database.get('title', [{}])[0].get('plain_text', 'Неизвестно')}")
            logger.info(f"ID базы данных: {self.database_id}")
            
            # Анализируем структуру базы данных
            self.analyze_database_structure(database)
            
            # Попробуем получить записи напрямую
            self.get_database_records_simple()
            
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
        """Получает записи из базы данных"""
        logger.info("=== ПОЛУЧЕНИЕ ЗАПИСЕЙ ИЗ БАЗЫ ===")
        
        try:
            # Получаем все записи из базы
            records = []
            has_more = True
            start_cursor = None
            
            while has_more:
                # Используем pages.list для получения записей из базы
                query_params = {
                    'page_size': 100
                }
                
                if start_cursor:
                    query_params['start_cursor'] = start_cursor
                
                # Фильтруем страницы по базе данных
                result = self.client.pages.list(**query_params)
                
                # Фильтруем только страницы, принадлежащие нашей базе
                database_pages = []
                for page in result.get('results', []):
                    if page.get('parent', {}).get('database_id') == self.database_id:
                        database_pages.append(page)
                
                records.extend(database_pages)
                has_more = result.get('has_more', False)
                start_cursor = result.get('next_cursor', None)
                
                logger.info(f"Получено записей из базы: {len(database_pages)}")
            
            logger.info(f"Всего записей в базе: {len(records)}")
            
            # Анализируем записи для поиска криптовалют
            self.analyze_cryptocurrencies(records)
            
            # Обновляем курсы криптовалют (временно отключено для отладки)
            # self.update_crypto_prices()
            
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
        """Обновляет курсы для списка криптовалют"""
        logger.info("=== ОБНОВЛЕНИЕ КУРСОВ КРИПТОВАЛЮТ ===")
        
        updated_cryptos = []
        
        for crypto in cryptocurrencies:
            crypto_name = crypto['name']
            crypto_symbol = crypto['symbol']
            page_id = crypto['page_id']
            
            logger.info(f"Обработка: {crypto_name} ({crypto_symbol})")
            
            # Ищем криптовалюту в CoinGecko
            coin_info = self.search_cryptocurrency(crypto_name, crypto_symbol)
            
            if not coin_info:
                logger.warning(f"Не удалось найти {crypto_name} в CoinGecko")
                continue
            
            coin_id = coin_info['id']
            
            # Получаем данные о цене
            price_data = self.get_price_data(coin_id)
            
            if not price_data:
                logger.warning(f"Не удалось получить цену для {crypto_name}")
                continue
            
            # Формируем обновленные данные
            updated_crypto = {
                'page_id': page_id,
                'name': crypto_name,
                'symbol': crypto_symbol,
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
                logger.info(f"✅ {crypto_name}: ${price:,.2f}")
                if change_24h is not None:
                    change_symbol = "📈" if change_24h > 0 else "📉"
                    logger.info(f"   {change_symbol} 24h изменение: {change_24h:+.2f}%")
            else:
                logger.warning(f"⚠️ {crypto_name}: цена не найдена")
        
        logger.info(f"=== ОБНОВЛЕНИЕ ЗАВЕРШЕНО. Обработано {len(updated_cryptos)} криптовалют ===")
        return updated_cryptos
    

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