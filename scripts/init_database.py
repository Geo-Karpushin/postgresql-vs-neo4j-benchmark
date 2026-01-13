#!/usr/bin/env python3
"""
МИНИМАЛИСТИЧНАЯ ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ
Только необходимые индексы для ускорения запросов
"""

import logging
import psycopg2
from neo4j import GraphDatabase
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
import sys

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
    def __init__(self, config):
        self.config = config

    def _get_connection(self):
        return psycopg2.connect(**self.config)
    
    def init_schema_with_indexes(self):
        """Минимальные индексы для быстрой загрузки"""
        try:
            with self._get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    # Таблицы без индексов для быстрой загрузки
                    cursor.execute("""
                        CREATE UNLOGGED TABLE users (
                            user_id BIGSERIAL PRIMARY KEY,
                            name VARCHAR(100) NOT NULL,
                            age INTEGER,
                            city VARCHAR(100),
                            registration_date TIMESTAMP NOT NULL DEFAULT NOW()
                        );
                    """)
                    
                    cursor.execute("""
                        CREATE UNLOGGED TABLE friendships (
                            friendship_id BIGSERIAL PRIMARY KEY,
                            user_id INTEGER NOT NULL,
                            friend_id INTEGER NOT NULL,
                            since TIMESTAMP NOT NULL DEFAULT NOW(),
                            UNIQUE(user_id, friend_id),
                            CONSTRAINT no_self_friendship CHECK (user_id != friend_id),
                            FOREIGN KEY (user_id) REFERENCES users(user_id),
                            FOREIGN KEY (friend_id) REFERENCES users(user_id)
                        );
                    """)
                    
                    # Только самые необходимые индексы для загрузки
                    cursor.execute("""
                        CREATE INDEX idx_friendships_user_friend 
                        ON friendships(user_id, friend_id);
                    """)
                    
                    cursor.execute("""
                        CREATE INDEX idx_friendships_friend_user 
                        ON friendships(friend_id, user_id);
                    """)
            return True
        except Exception as e:
            logger.error(f"PostgreSQL init error: {e}")
            return False
    
    def finalize_after_loading(self):
        """Добавляем индексы для аналитических запросов"""
        try:
            with self._get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cursor:
                    indexes_sql = [
                        # Основные индексы
                        ("idx_users_city", "CREATE INDEX idx_users_city ON users(city);"),
                        ("idx_users_age", "CREATE INDEX idx_users_age ON users(age);"),
                        ("idx_users_registration_date", "CREATE INDEX idx_users_registration_date ON users(registration_date);"),
                        ("idx_friendships_since_btree", "CREATE INDEX idx_friendships_since_btree ON friendships(since);"),
                        
                        # Составные индексы
                        ("idx_friendships_covering", "CREATE INDEX idx_friendships_covering ON friendships(user_id, friend_id) INCLUDE (since);"),
                        ("idx_users_city_user_id", "CREATE INDEX idx_users_city_user_id ON users(city, user_id);"),

                        # Частичные индексы
                        ("idx_users_age_not_null", "CREATE INDEX idx_users_age_not_null ON users(age) WHERE age IS NOT NULL;"),
                        
                        # Специальные индексы
                        ("idx_friendships_both_directions", "CREATE INDEX idx_friendships_composite_search ON friendships USING btree(LEAST(user_id, friend_id), GREATEST(user_id, friend_id));"),
                        ("idx_friendships_since_brin", "CREATE INDEX idx_friendships_since_brin ON friendships USING brin(since);")
                    ]

                    for index_name, sql in indexes_sql:
                        try:
                            cursor.execute(f"DROP INDEX IF EXISTS {index_name};")
                            cursor.execute(sql)
                            logger.info(f"Создан индекс: {index_name}")
                        except Exception as e:
                            logger.error(f"Ошибка создания индекса {index_name}: {e}")
                    
                    # Анализ статистики
                    cursor.execute("ANALYZE users;")
                    cursor.execute("ANALYZE friendships;")
            return True
        except Exception as e:
            logger.error(f"PostgreSQL finalize error: {e}")
            return False

class Neo4jInitializer:
    def __init__(self, config):
        self.driver = GraphDatabase.driver(
            config["uri"], 
            auth=config["auth"],
            max_connection_lifetime=config["max_connection_lifetime"],
            max_connection_pool_size=config["max_connection_pool_size"],
            connection_timeout=config["connection_timeout"]
        )

    def init_schema_with_indexes(self):
        try:
            with self.driver.session() as session:
                queries = [
                    """CREATE CONSTRAINT user_id_unique IF NOT EXISTS 
                       FOR (u:User) REQUIRE u.user_id IS UNIQUE;""",
                    """CREATE INDEX user_city_index IF NOT EXISTS 
                       FOR (u:User) ON (u.city);""",
                    """CREATE INDEX user_age_index IF NOT EXISTS 
                       FOR (u:User) ON (u.age);"""
                ]
                
                for query in queries:
                    session.run(query)
            return True
        except Exception as e:
            logger.error(f"Neo4j init error: {e}")
            return False
    
    def finalize_after_loading(self):
        try:
            with self.driver.session() as session:
                indexes_neo4j = [
                    # Добавляем индекс для since
                    ("friendship_since_index", """
                        CREATE INDEX friendship_since_index IF NOT EXISTS 
                        FOR ()-[r:FRIENDS_WITH]-() ON (r.since);
                    """),

                    # Аналитика по датам
                    ("user_registration_date_index", """
                        CREATE INDEX user_registration_date_index IF NOT EXISTS
                        FOR (u:User) ON (u.registration_date);
                    """),
                    
                    # Составной индекс для частых фильтров
                    ("user_city_age_index", """
                        CREATE INDEX user_city_age_index IF NOT EXISTS 
                        FOR (u:User) ON (u.city, u.age);
                    """)
                ]

                for index_name, query in indexes_neo4j:
                    try:
                        session.run(query)
                        logger.info(f"Создан индекс: {index_name}")
                    except Exception as e:
                        logger.error(f"Ошибка создания индекса {index_name}: {e}")
                
                # Собираем статистику
                try:
                    session.run("CALL db.stats.collect('GRAPH')")
                except Exception as e:
                    logger.error(f"⚠️  Ошибка сбора статистики: {e}")
            return True
        except Exception as e:
            logger.error(f"Neo4j finalize error: {e}")
            return False

def initialize_with_indexes():
    """Инициализация с минимальными индексами"""
    print("\n" + "="*60)
    print("🚀 ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ С МИНИМАЛЬНЫМИ ИНДЕКСАМИ")
    print("   Только необходимые индексы для ускорения запросов")
    print("="*60)
    
    start_time = time.time()
    
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    print("\n1️⃣ PostgreSQL: Создание схемы с индексами...")
    results.append(("PostgreSQL", pg_init.init_schema_with_indexes()))
    
    print("\n2️⃣ Neo4j: Создание схемы с индексами...")
    results.append(("Neo4j", neo4j_init.init_schema_with_indexes()))
    
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
        print("\n⚡ БАЗЫ ДАННЫХ ГОТОВЫ К ЗАГРУЗКЕ ДАННЫХ")
        return True
    else:
        print("\n❌ ИНИЦИАЛИЗАЦИЯ НЕ УДАЛАСЬ")
        return False

def finalize_after_loading():
    """Финальная оптимизация после загрузки данных"""
    print("\n" + "="*60)
    print("🔄 ФИНАЛЬНАЯ ОПТИМИЗАЦИЯ ПОСЛЕ ЗАГРУЗКИ")
    print("   Обновление статистики для оптимизатора запросов")
    print("="*60)
    
    start_time = time.time()
    
    pg_init = PostgresInitializer(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_init = Neo4jInitializer(DatabaseConfig.NEO4J_CONFIG)
    
    results = []
    
    print("\n1️⃣ PostgreSQL: Оптимизация после загрузки...")
    results.append(("PostgreSQL", pg_init.finalize_after_loading()))
    
    print("\n2️⃣ Neo4j: Оптимизация после загрузки...")
    results.append(("Neo4j", neo4j_init.finalize_after_loading()))
    
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
        print("\n🎉 БАЗЫ ДАННЫХ ОПТИМИЗИРОВАНЫ И ГОТОВЫ К ТЕСТИРОВАНИЮ")
        return True
    else:
        print("\n⚠️  НЕКОТОРЫЕ ОПЕРАЦИИ НЕ ВЫПОЛНЕНЫ")
        return False

def main():
    """Основная функция"""
    print("\n" + "="*60)
    print("🗄️  МЕНЕДЖЕР ИНИЦИАЛИЗАЦИИ БАЗ ДАННЫХ")
    print("   Минималистичная версия с рабочими индексами")
    print("="*60)
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "init":
            return initialize_with_indexes()
        elif command == "finalize":
            return finalize_after_loading()
        elif command == "help":
            print("Доступные команды:")
            print("  init     - Создание схемы с минимальными индексами")
            print("  finalize - Обновление статистики после загрузки данных")
            print("\nОсобенности этой версии:")
            print("  • Только необходимые индексы для ускорения запросов")
            print("  • Минимальная конфигурация для обеих СУБД")
            print("  • Быстрая инициализация")
            return True
        else:
            print(f"Неизвестная команда: {command}")
            print("Используйте: init, finalize, help")
            return False
    else:
        print("Ошибка: укажите команду")
        print("Пример: python init_databases.py init")
        print("Используйте 'help' для справки")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)