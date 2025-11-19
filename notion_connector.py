#!/usr/bin/env python3
"""
Скрипт для подключения к таблице Notion
Использует переменные среды Railway: NOTION_TOKEN и NOTION_DATABASE_ID
"""

import os
import sys
import logging
from typing import Optional
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
            
            # Получаем количество записей
            query_result = self.client.databases.list(database_id=self.database_id)
            records_count = len(query_result.get('results', []))
            logger.info(f"Количество записей в базе: {records_count}")
            
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при подключении к базе данных Notion: {e}")
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