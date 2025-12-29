#!/usr/bin/env python3
"""
ОПТИМИЗИРОВАННАЯ ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ
Рабочая версия без ошибок
"""

import logging
import psycopg2
from neo4j import GraphDatabase
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import concurrent.futures
import time
import sys

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Конфигурация подключения"""
    POSTGRES_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "database": "benchmark",
        "user": "postgres",
        "password": "password",
        "connect_timeout": 10,
        "application_name": "benchmark_init"
    }
    
    NEO4J_CONFIG = {
        "uri": "bolt://localhost:7687",
        "auth": ("neo4j", "password"),
        "max_connection_lifetime": 7200,
        "max_connection_pool_size": 50,
        "connection_timeout": 30
    }

class PostgresInitializer:
    """Инициализатор PostgreSQL - РАБОЧАЯ версия"""
    
    def __init__(self, config):
        self.config = config
    
    def _get_connection(self, autocommit=True):
        """Создание подключения"""
        conn = psycopg2.connect(**self.config)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    
    def init_schema_for_loading(self):
        """Создание схемы для быстрой загрузки"""
        logger.info("🗃️ PostgreSQL: Создание схемы...")
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. Пользователи
                    cursor.execute("""
                        CREATE UNLOGGED TABLE users (
                            user_id BIGINT PRIMARY KEY,
                            name VARCHAR(100),
                            age INTEGER,
                            city VARCHAR(50),
                            registration_date DATE
                        );
                    """)
                    
                    # 2. Связи дружбы - ИМЯ ДОЛЖНО СОВПАДАТЬ С ЗАГРУЗЧИКОМ!
                    cursor.execute("""
                        CREATE UNLOGGED TABLE friendships (
                            user_id BIGINT NOT NULL,
                            friend_id BIGINT NOT NULL,
                            since DATE NOT NULL,
                            PRIMARY KEY (user_id, friend_id)
                        );
                    """)
                    
                logger.info("   • Созданы UNLOGGED таблицы: users и friendships")
                logger.info("   • Имя таблицы: friendships (совместимо с загрузчиком)")
            
            logger.info("✅ PostgreSQL: Схема создана")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания схемы PostgreSQL: {e}")
            return False
    
    def finalize_after_loading(self):
        """Финальная оптимизация после загрузки данных"""
        logger.info("🔄 PostgreSQL: Финальная оптимизация...")
        
        try:
            # 1. Включаем логирование таблиц и создаем индексы
            conn = self._get_connection()
            
            with conn.cursor() as cursor:
                # Включаем логирование таблиц
                logger.info("   • Включаем логирование таблиц...")
                cursor.execute("ALTER TABLE users SET LOGGED;")
                cursor.execute("ALTER TABLE friendships SET LOGGED;")
                
                # Создаем индексы
                logger.info("   • Создаем индексы...")
                index_queries = [
                    # Основные индексы для запросов
                    "CREATE INDEX idx_friendships_user ON friendships(user_id);",
                    "CREATE INDEX idx_friendships_friend ON friendships(friend_id);",
                    "CREATE INDEX idx_friendships_since ON friendships(since);",
                    
                    # Индексы для users
                    "CREATE INDEX idx_users_city ON users(city);",
                    "CREATE INDEX idx_users_age ON users(age);",
                    "CREATE INDEX idx_users_registration ON users(registration_date);",
                    
                    # Составные индексы для оптимизации
                    "CREATE INDEX idx_friendships_user_friend ON friendships(user_id, friend_id);",
                    "CREATE INDEX idx_friendships_friend_user ON friendships(friend_id, user_id);",
                ]
                
                for query in index_queries:
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        logger.warning(f"   • Ошибка индекса: {e}")
            
            conn.close()
            
            # 2. VACUUM ANALYZE должен выполняться в отдельном соединении без транзакции
            logger.info("   • Выполняем VACUUM ANALYZE...")
            conn_vacuum = self._get_connection()
            conn_vacuum.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            
            with conn_vacuum.cursor() as cursor:
                cursor.execute("VACUUM ANALYZE users;")
                cursor.execute("VACUUM ANALYZE friendships;")
            
            conn_vacuum.close()
            
            logger.info("✅ PostgreSQL: Финальная оптимизация завершена")
            logger.info("   • Таблицы переведены в LOGGED режим")
            logger.info("   • Созданы индексы")
            logger.info("   • VACUUM ANALYZE выполнен")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка финальной оптимизации PostgreSQL: {e}")
            return False

class Neo4jInitializer:
    """Инициализатор Neo4j - РАБОЧАЯ версия"""
    
    def __init__(self, config):
        self.config = config
        self.driver = None
    
    def _get_driver(self):
        """Создание драйвера Neo4j"""
        return GraphDatabase.driver(
            self.config["uri"],
            auth=self.config["auth"],
            max_connection_lifetime=self.config.get("max_connection_lifetime", 7200),
            max_connection_pool_size=self.config.get("max_connection_pool_size", 50),
            connection_timeout=self.config.get("connection_timeout", 30)
        )
    
    def init_schema_for_loading(self):
        """Создание минимальной схемы для загрузки"""
        logger.info("🕸️ Neo4j: Инициализация схемы...")
        
        try:
            self.driver = self._get_driver()
            
            with self.driver.session() as session:
                # Создаем constraint для user_id
                logger.info("   • Создаем constraint для user_id...")
                try:
                    # Удаляем старый constraint если существует
                    session.run("DROP CONSTRAINT user_id_unique IF EXISTS")
                    
                    # Создаем новый constraint
                    session.run("""
                        CREATE CONSTRAINT user_id_unique 
                        FOR (u:User) REQUIRE u.user_id IS UNIQUE
                    """)
                    logger.info("   • Constraint создан успешно")
                except Exception as e:
                    logger.warning(f"   • Ошибка constraint: {e}")
                    # Продолжаем, constraint может не создаваться если уже есть
                
                logger.info("   • Индексы будут созданы после загрузки данных")
            
            logger.info("✅ Neo4j: Схема создана")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Neo4j: {e}")
            return False
        finally:
            if self.driver:
                self.driver.close()
    
    def create_indexes_after_loading(self):
        """Создание индексов после загрузки данных"""
        logger.info("🔍 Neo4j: Создание индексов...")
        
        try:
            self.driver = self._get_driver()
            
            with self.driver.session() as session:
                # Создаем индексы
                logger.info("   • Создаем индексы...")
                
                indexes = [
                    "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.user_id)",
                    "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.city)",
                    "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.age)",
                    "CREATE INDEX IF NOT EXISTS FOR (u:User) ON (u.registration_date)",
                    "CREATE INDEX IF NOT EXISTS FOR ()-[r:FRIENDS_WITH]-() ON (r.since)",
                ]
                
                for idx_query in indexes:
                    try:
                        session.run(idx_query)
                    except Exception as e:
                        logger.warning(f"   • Ошибка индекса: {e}")
                
                # Ждем построения индексов
                logger.info("   • Ожидаем построения индексов...")
                try:
                    session.run("CALL db.awaitIndexes(120)")
                except:
                    logger.warning("   • Пропускаем awaitIndexes")
            
            logger.info("✅ Neo4j: Индексы созданы")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания индексов Neo4j: {e}")
            return False
        finally:
            if self.driver:
                self.driver.close()

def initialize_for_loading():
    """Инициализация для загрузки данных"""
    print("\n" + "="*60)
    print("🚀 ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ ДЛЯ ЗАГРУЗКИ")
    print("   Создание минимальных схем")
    print("="*60)
    
    start_time = time.time()
    
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    # Параллельная инициализация
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_pg = executor.submit(pg_init.init_schema_for_loading)
        future_neo4j = executor.submit(neo4j_init.init_schema_for_loading)
        
        try:
            results.append(("PostgreSQL", future_pg.result(timeout=30)))
        except Exception as e:
            logger.error(f"PostgreSQL timeout: {e}")
            results.append(("PostgreSQL", False))
        
        try:
            results.append(("Neo4j", future_neo4j.result(timeout=30)))
        except Exception as e:
            logger.error(f"Neo4j timeout: {e}")
            results.append(("Neo4j", False))
    
    success = all(result[1] for result in results)
    elapsed_time = time.time() - start_time
    
    print("\n" + "📊 " + "="*50)
    print("РЕЗУЛЬТАТЫ ИНИЦИАЛИЗАЦИИ:")
    print("="*50)
    
    for db_name, result in results:
        status = "✅ УСПЕХ" if result else "❌ ОШИБКА"
        print(f"   {db_name}: {status}")
    
    if success:
        print(f"\n⏱️  Время выполнения: {elapsed_time:.2f} секунд")
        print("\n💡 БАЗЫ ДАННЫХ ГОТОВЫ К ЗАГРУЗКЕ")
        return True
    else:
        print("\n❌ ИНИЦИАЛИЗАЦИЯ НЕ УДАЛАСЬ")
        return False

def finalize_after_loading():
    """Финальная оптимизация после загрузки данных"""
    print("\n" + "="*60)
    print("🔄 ФИНАЛЬНАЯ ОПТИМИЗАЦИЯ ПОСЛЕ ЗАГРУЗКИ")
    print("   Создание индексов и оптимизация")
    print("="*60)
    
    start_time = time.time()
    
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    # Последовательная оптимизация
    print("\n1️⃣ PostgreSQL: Оптимизация...")
    results.append(("PostgreSQL", pg_init.finalize_after_loading()))
    
    print("\n2️⃣ Neo4j: Создание индексов...")
    results.append(("Neo4j", neo4j_init.create_indexes_after_loading()))
    
    success = all(result[1] for result in results)
    elapsed_time = time.time() - start_time
    
    print("\n" + "📊 " + "="*50)
    print("РЕЗУЛЬТАТЫ ОПТИМИЗАЦИИ:")
    print("="*50)
    
    for db_name, result in results:
        status = "✅ УСПЕХ" if result else "❌ ОШИБКА"
        print(f"   {db_name}: {status}")
    
    if success:
        print(f"\n⏱️  Общее время: {elapsed_time:.2f} секунд")
        print("\n🎉 БАЗЫ ДАННЫХ ГОТОВЫ К ТЕСТИРОВАНИЮ")
        return True
    else:
        print("\n⚠️  НЕКОТОРЫЕ ОПЕРАЦИИ НЕ ВЫПОЛНЕНЫ")
        return False

def main():
    """Основная функция"""
    print("\n" + "="*60)
    print("🗄️  МЕНЕДЖЕР ИНИЦИАЛИЗАЦИИ БАЗ ДАННЫХ")
    print("="*60)
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "init":
            return initialize_for_loading()
        elif command == "finalize":
            return finalize_after_loading()
        else:
            print(f"Неизвестная команда: {command}")
            print("Доступные команды: init, finalize")
            return False
    else:
        print("Ошибка синтаксиса команды")
        print("Укажите параметр: init, finalize")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)