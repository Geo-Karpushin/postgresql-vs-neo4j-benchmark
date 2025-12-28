#!/usr/bin/env python3
"""
ОПТИМИЗИРОВАННАЯ ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ
Создание схем для максимальной скорости загрузки данных
Настройки производительности вынесены в Docker
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
    """Инициализатор PostgreSQL - только схемы"""
    
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
                    # Создаем UNLOGGED таблицы для максимальной скорости загрузки
                    cursor.execute("""
                        CREATE UNLOGGED TABLE users (
                            user_id BIGINT PRIMARY KEY,
                            name VARCHAR(100),
                            age INTEGER,
                            city VARCHAR(50),
                            registration_date DATE
                        );
                    """)
                    
                    cursor.execute("""
                        CREATE UNLOGGED TABLE friendships (
                            user_id BIGINT,
                            friend_id BIGINT,
                            since DATE,
                            PRIMARY KEY (user_id, friend_id)
                        );
                    """)
                    
                logger.info("   • Созданы UNLOGGED таблицы users и friendships")
                logger.info("   • Первичные ключи созданы")
            
            logger.info("✅ PostgreSQL: Схема создана (UNLOGGED для быстрой загрузки)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания схемы PostgreSQL: {e}")
            return False
    
    def finalize_after_loading(self):
        """Финальная оптимизация после загрузки данных"""
        logger.info("🔄 PostgreSQL: Финальная оптимизация...")
        
        try:
            conn = self._get_connection()
            
            with conn.cursor() as cursor:
                # 1. Включаем логирование таблиц
                logger.info("   • Включаем логирование таблиц...")
                cursor.execute("ALTER TABLE users SET LOGGED;")
                cursor.execute("ALTER TABLE friendships SET LOGGED;")
                
                # 2. Создаем индексы (без CONCURRENTLY для скорости, т.к. база еще не используется)
                logger.info("   • Создаем оптимизированные индексы...")
                
                # Основные индексы для запросов
                index_queries = [
                    # Для простых запросов друзей
                    "CREATE INDEX idx_friendships_user ON friendships(user_id);",
                    "CREATE INDEX idx_friendships_friend ON friendships(friend_id);",
                    
                    # Для аналитических запросов
                    "CREATE INDEX idx_users_city ON users(city);",
                    "CREATE INDEX idx_users_age ON users(age);",
                    "CREATE INDEX idx_users_registration ON users(registration_date);",
                    
                    # Составные индексы для JOIN
                    "CREATE INDEX idx_users_city_age ON users(city, age);",
                    
                    # Для friendship аналитики
                    "CREATE INDEX idx_friendships_since ON friendships(since);",
                    
                    # Индексы для специфических аналитических запросов
                    "CREATE INDEX idx_users_registration_year ON users((EXTRACT(YEAR FROM registration_date)));",
                ]
                
                for query in index_queries:
                    try:
                        cursor.execute(query)
                    except Exception as e:
                        logger.warning(f"   • Ошибка создания индекса: {e}")
                
                # 3. Добавляем внешние ключи
                logger.info("   • Добавляем внешние ключи...")
                try:
                    cursor.execute("""
                        ALTER TABLE friendships 
                        ADD CONSTRAINT fk_friendships_user 
                        FOREIGN KEY (user_id) REFERENCES users(user_id);
                    """)
                    
                    cursor.execute("""
                        ALTER TABLE friendships 
                        ADD CONSTRAINT fk_friendships_friend 
                        FOREIGN KEY (friend_id) REFERENCES users(user_id);
                    """)
                except Exception as e:
                    logger.warning(f"   • Ошибка добавления внешних ключей: {e}")
                
                # 4. Собираем статистику
                logger.info("   • Собираем статистику...")
                cursor.execute("VACUUM ANALYZE users;")
                cursor.execute("VACUUM ANALYZE friendships;")
            
            conn.close()
            logger.info("✅ PostgreSQL: Финальная оптимизация завершена")
            logger.info("   • Таблицы переведены в LOGGED режим")
            logger.info("   • Созданы все индексы")
            logger.info("   • Добавлены ограничения")
            logger.info("   • Статистика собрана")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка финальной оптимизации PostgreSQL: {e}")
            return False

class Neo4jInitializer:
    """Инициализатор Neo4j - только схемы"""
    
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
                # Создаем ТОЛЬКО constraint для user_id (обязательно для корректной загрузки)
                logger.info("   • Создаем constraint уникальности...")
                try:
                    session.run("""
                        CREATE CONSTRAINT user_id_unique IF NOT EXISTS
                        FOR (u:User) REQUIRE u.user_id IS UNIQUE
                    """)
                    logger.info("   • Constraint создан успешно")
                except Exception as e:
                    logger.error(f"   • Ошибка создания constraint: {e}")
                    return False
                
                # Проверяем APOC (важно для загрузки)
                logger.info("   • Проверяем доступность APOC...")
                try:
                    result = session.run("RETURN apoc.version() as version")
                    version = result.single()["version"]
                    logger.info(f"   • APOC {version} доступен")
                except Exception as e:
                    logger.warning(f"   • APOC может быть недоступен: {e}")
                
                logger.info("   • Индексы будут созданы после загрузки данных")
            
            logger.info("✅ Neo4j: Схема создана (только constraint)")
            logger.info("   • База готова для загрузки через apoc.import.csv")
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
                # Создаем индексы для ускорения запросов
                logger.info("   • Создаем базовые индексы...")
                
                indexes = [
                    # Базовые индексы для пользователей
                    "CREATE INDEX user_city_index IF NOT EXISTS FOR (u:User) ON (u.city)",
                    "CREATE INDEX user_age_index IF NOT EXISTS FOR (u:User) ON (u.age)",
                    "CREATE INDEX user_registration_index IF NOT EXISTS FOR (u:User) ON (u.registration_date)",
                    "CREATE INDEX user_id_index IF NOT EXISTS FOR (u:User) ON (u.user_id)",
                    
                    # Индекс для связей
                    "CREATE INDEX friendship_since_index IF NOT EXISTS FOR ()-[r:FRIENDS_WITH]-() ON (r.since)",
                    
                    # Индексы для аналитических запросов
                    "CREATE INDEX user_city_age_index IF NOT EXISTS FOR (u:User) ON (u.city, u.age)",
                    "CREATE INDEX friendship_since_year_index IF NOT EXISTS FOR ()-[r:FRIENDS_WITH]-() ON (r.since.year)",
                ]
                
                for idx_query in indexes:
                    try:
                        session.run(idx_query)
                    except Exception as e:
                        logger.warning(f"   • Ошибка создания индекса: {e}")
                
                # Ждем построения индексов
                logger.info("   • Ожидаем построения индексов...")
                try:
                    session.run("CALL db.awaitIndexes(300)")
                    logger.info("   • Все индексы построены")
                except:
                    logger.warning("   • Пропускаем awaitIndexes")
                
                # Проверяем состояние индексов
                try:
                    result = session.run("""
                        SHOW INDEXES 
                        YIELD name, type, state, populationPercent
                        WHERE state = 'ONLINE'
                        RETURN count(*) as online_count, 
                               avg(populationPercent) as avg_population
                    """).single()
                    
                    logger.info(f"   • Онлайн индексов: {result['online_count']}")
                    if result['avg_population']:
                        logger.info(f"   • Средняя заполненность: {result['avg_population']:.1f}%")
                except:
                    logger.warning("   • Не удалось проверить состояние индексов")
            
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
    
    # Инициализируем оба типа баз
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    # Параллельная инициализация
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        # Запускаем инициализацию обеих баз параллельно
        future_pg = executor.submit(pg_init.init_schema_for_loading)
        future_neo4j = executor.submit(neo4j_init.init_schema_for_loading)
        
        # Ждем результаты
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
    
    # Проверяем результаты
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
        print("\n💡 БАЗЫ ДАННЫХ ГОТОВЫ К ЗАГРУЗКЕ:")
        print("   1. PostgreSQL: Используйте COPY для загрузки в UNLOGGED таблицы")
        print("   2. Neo4j: Используйте apoc.import.csv")
        print(f"\n   3. После загрузки выполните: python finalize_schemas.py")
        return True
    else:
        print("\n❌ ИНИЦИАЛИЗАЦИЯ НЕ УДАЛАСЬ")
        print("   Проверьте подключение к базам данных")
        return False

def finalize_after_loading():
    """Финальная оптимизация после загрузки данных"""
    print("\n" + "="*60)
    print("🔄 ФИНАЛЬНАЯ ОПТИМИЗАЦИЯ ПОСЛЕ ЗАГРУЗКИ")
    print("   Создание индексов и ограничений")
    print("="*60)
    
    start_time = time.time()
    
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    # Последовательная оптимизация (чтобы не перегружать систему)
    print("\n1️⃣ PostgreSQL: Оптимизация...")
    results.append(("PostgreSQL", pg_init.finalize_after_loading()))
    
    print("\n2️⃣ Neo4j: Создание индексов...")
    results.append(("Neo4j", neo4j_init.create_indexes_after_loading()))
    
    # Проверяем результаты
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
        print("\n🎉 БАЗЫ ДАННЫХ ПОЛНОСТЬЮ ГОТОВЫ К ТЕСТИРОВАНИЮ!")
        return True
    else:
        print("\n⚠️  НЕКОТОРЫЕ ОПЕРАЦИИ НЕ ВЫПОЛНЕНЫ")
        print("   Проверьте логи для подробностей")
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