#!/usr/bin/env python3
"""
Оптимизированная инициализация баз данных PostgreSQL и Neo4j
Только подготовка схем для быстрой загрузки данных
"""

import logging
import psycopg2
from neo4j import GraphDatabase
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseConfig:
    """Конфигурация подключения к базам данных"""
    POSTGRES_CONFIG = {
        "host": "localhost",
        "port": 5432,
        "database": "benchmark", 
        "user": "postgres",
        "password": "password"
    }
    
    NEO4J_CONFIG = {
        "uri": "bolt://localhost:7687",
        "auth": ("neo4j", "password")
    }

class PostgresManager:
    """Менеджер для работы с PostgreSQL"""
    
    def __init__(self, config):
        self.config = config
    
    def _get_connection(self, autocommit=True):
        """Создание подключения с настройками оптимизации"""
        conn = psycopg2.connect(**self.config)
        if autocommit:
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return conn
    
    def init_schema(self):
        """Создание оптимизированной схемы для быстрой загрузки"""
        logger.info("🗃️ Создание схемы PostgreSQL для быстрой загрузки...")
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
                # Критически важные настройки для максимальной скорости загрузки
                cursor.execute("""
                    SET maintenance_work_mem = '1GB';
                    SET max_parallel_workers = 8;
                    SET max_parallel_workers_per_gather = 4;
                    SET max_parallel_maintenance_workers = 4;
                    SET work_mem = '256MB';
                """)
                
                # Создание UNLOGGED таблиц (макс. производительность)
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
                        strength VARCHAR(10),
                        PRIMARY KEY (user_id, friend_id)
                    );
                """)
                
                logger.info("   • UNLOGGED таблицы созданы (без ограничений)")
                
            logger.info("✅ PostgreSQL готова для быстрой загрузки")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания схемы PostgreSQL: {e}")
            return False

class Neo4jManager:
    """Менеджер для работы с Neo4j"""
    
    def __init__(self, config):
        self.config = config
    
    def _get_driver(self):
        """Создание драйвера Neo4j с оптимизацией"""
        return GraphDatabase.driver(
            self.config["uri"],
            auth=self.config["auth"],
            max_connection_lifetime=3600,
            connection_acquisition_timeout=120
        )
    
    def init_schema(self):
        """Оптимизированная инициализация Neo4j для APOC загрузки"""
        logger.info("🕸️ Инициализация Neo4j для быстрой загрузки APOC...")
        
        driver = None
        try:
            driver = self._get_driver()
            
            with driver.session() as session:
                # Проверяем и настраиваем APOC для максимальной производительности
                logger.info("   • Проверяем и настраиваем APOC...")
                
                # Проверяем доступность APOC
                apoc_version = session.run("RETURN apoc.version() AS version").single()
                if not apoc_version:
                    raise RuntimeError("APOC недоступен!")
                
                logger.info(f"     ➜ APOC {apoc_version['version']} доступен")
                
                # Убедимся, что база чистая (дополнительная проверка)
                node_count = session.run("MATCH (n) RETURN count(n) AS count").single()["count"]
                if node_count > 0:
                    logger.info(f"   • Очищаем оставшиеся {node_count} узлов...")
                    session.run("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS")
                
                # Создаем ТОЛЬКО критически важное ограничение
                logger.info("   • Создаем ограничение уникальности user_id...")
                session.run("""
                    CREATE CONSTRAINT user_id_unique IF NOT EXISTS
                    FOR (u:User) REQUIRE u.user_id IS UNIQUE
                """)
                
                # Явно НЕ создаем индексы до загрузки данных
                logger.info("   • Индексы отложены для максимальной скорости загрузки")
                
                # Проверяем состояние схемы
                constraints = session.run("SHOW CONSTRAINTS").data()
                logger.info(f"     ➜ Активные ограничения: {len(constraints)}")
                
            logger.info("✅ Neo4j оптимизирована для быстрой загрузки APOC")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации Neo4j: {e}")
            return False
        finally:
            if driver:
                driver.close()

def enable_postgres_constraints():
    """Включение ограничений PostgreSQL после загрузки данных"""
    logger.info("🔒 Включение ограничений PostgreSQL...")
    
    try:
        conn = psycopg2.connect(**DatabaseConfig.POSTGRES_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        with conn.cursor() as cursor:
            # Включаем логирование таблиц
            logger.info("   • Включаем логирование таблиц...")
            cursor.execute("ALTER TABLE users SET LOGGED;")
            cursor.execute("ALTER TABLE friendships SET LOGGED;")
            
            # Добавляем внешние ключи
            logger.info("   • Добавляем внешние ключи...")
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
            
            # Создаем индексы параллельно
            logger.info("   • Создаем индексы...")
            index_queries = [
                "CREATE INDEX CONCURRENTLY idx_friendships_user_friend ON friendships(user_id, friend_id);",
                "CREATE INDEX CONCURRENTLY idx_friendships_friend_user ON friendships(friend_id, user_id);", 
                "CREATE INDEX CONCURRENTLY idx_users_city ON users(city);",
                "CREATE INDEX CONCURRENTLY idx_users_age ON users(age);",
                "CREATE INDEX CONCURRENTLY idx_users_registration ON users(registration_date);"
            ]
            
            for query in index_queries:
                try:
                    cursor.execute(query)
                except Exception as idx_error:
                    logger.warning(f"Предупреждение при создании индекса: {idx_error}")
            
            # Обновляем статистику
            logger.info("   • Обновляем статистику...")
            cursor.execute("ANALYZE users;")
            cursor.execute("ANALYZE friendships;")
        
        conn.close()
        logger.info("✅ Ограничения PostgreSQL включены")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка включения ограничений PostgreSQL: {e}")
        return False

def enable_neo4j_indexes():
    """Создание индексов Neo4j после загрузки данных"""
    logger.info("🔍 Создание индексов Neo4j...")
    
    driver = None
    try:
        driver = GraphDatabase.driver(
            DatabaseConfig.NEO4J_CONFIG["uri"],
            auth=DatabaseConfig.NEO4J_CONFIG["auth"]
        )
        
        with driver.session() as session:
            indexes = [
                "CREATE INDEX user_city_index IF NOT EXISTS FOR (u:User) ON (u.city)",
                "CREATE INDEX user_age_index IF NOT EXISTS FOR (u:User) ON (u.age)",
                "CREATE INDEX user_registration_index IF NOT EXISTS FOR (u:User) ON (u.registration_date)",
                "CREATE INDEX friendship_strength_index IF NOT EXISTS FOR ()-[r:FRIENDS]-() ON (r.strength)",
                "CREATE INDEX friendship_since_index IF NOT EXISTS FOR ()-[r:FRIENDS]-() ON (r.since)"
                "CREATE INDEX user_friends_index IF NOT EXISTS FOR (u:User) ON (u.user_id, u.name)",
                "CREATE INDEX friendship_direction_index IF NOT EXISTS FOR ()-[r:FRIENDS_WITH]-() ON (r.since)"
            ]
            
            for idx_query in indexes:
                session.run(idx_query)
            
            logger.info(f"   • Создано индексов: {len(indexes)}")
            
            # Ждем доступности индексов
            session.run("CALL db.awaitIndexes(300)")
            
            # Проверяем состояние индексов
            index_status = session.run("""
                SHOW INDEXES 
                WHERE type = 'RANGE' 
                YIELD name, state, populationPercent
                RETURN count(*) as total, 
                       sum(CASE WHEN state = 'ONLINE' THEN 1 ELSE 0 END) as online
            """).single()
            
            logger.info(f"     ➜ Индексы: {index_status['online']}/{index_status['total']} ONLINE")
        
        logger.info("✅ Индексы Neo4j созданы")
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка создания индексов Neo4j: {e}")
        return False
    finally:
        if driver:
            driver.close()

def main():
    """Основная функция инициализации"""
    print("🎯 ОПТИМИЗИРОВАННАЯ ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ")
    print("=" * 50)
    print("Настройка для максимальной скорости загрузки данных")
    print("=" * 50)
    
    # Инициализация менеджеров
    pg_manager = PostgresManager(DatabaseConfig.POSTGRES_CONFIG)
    neo4j_manager = Neo4jManager(DatabaseConfig.NEO4J_CONFIG)
    
    # Инициализация схем (параллельно)
    print("\n1. СОЗДАНИЕ ОПТИМИЗИРОВАННЫХ СХЕМ")
    
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        pg_future = executor.submit(pg_manager.init_schema)
        neo4j_future = executor.submit(neo4j_manager.init_schema)
        
        success_pg = pg_future.result(timeout=60)
        success_neo4j = neo4j_future.result(timeout=60)
    
    if success_pg and success_neo4j:
        print("\n✅ Базы данных готовы для БЫСТРОЙ загрузки!")
        print("\n💡 После загрузки данных выполните:")
        print("   python enable_constraints.py")
        return True
    else:
        print("\n❌ Ошибка при инициализации баз данных")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)