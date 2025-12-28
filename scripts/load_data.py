#!/usr/bin/env python3
"""
УНИВЕРСАЛЬНЫЙ ЗАГРУЗЧИК PostgreSQL и Neo4j
------------------------------------------

Выполняет:
  • Загрузку users.csv и friendships.csv в PostgreSQL (COPY)
  • Загрузку users.csv и friendships.csv в Neo4j (APOC import.csv)
  • Проверяет наличие файлов
  • Выдаёт exit(1) при любой ошибке
  • НЕ очищает базы (очистка выполняется отдельно)
"""

import os
import sys
import traceback
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from neo4j import GraphDatabase
import logging
import time

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---------------- Configuration ----------------

POSTGRES = {
    "host": "localhost",
    "port": 5432,
    "database": "benchmark",
    "user": "postgres",
    "password": "password",
    "connect_timeout": 10
}

NEO4J = {
    "uri": "bolt://localhost:7687",
    "auth": ("neo4j", "password"),
    "max_connection_pool_size": 50,
    "connection_timeout": 30
}

# ------------------------------------------------


def fail(msg):
    logger.error(f"❌ {msg}")
    sys.exit(1)


def info(msg):
    logger.info(f"{msg}")


# =========================================================
#                    PostgreSQL LOADER
# =========================================================

def load_postgres(csv_dir):
    """Загрузка данных в PostgreSQL через COPY"""
    users_path = os.path.join(csv_dir, "users.csv")
    friends_path = os.path.join(csv_dir, "friendships.csv")

    info("🐘 Загрузка данных в PostgreSQL...")

    try:
        conn = psycopg2.connect(**POSTGRES)
        conn.autocommit = True
        cur = conn.cursor()

        # 1. Загрузка пользователей
        info("  • COPY users.csv...")
        start_time = time.time()
        
        with open(users_path, "r", encoding="utf-8") as f:
            cur.copy_expert("""
                COPY users (user_id, name, age, city, registration_date)
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """, f)
        
        users_count = cur.rowcount
        elapsed = time.time() - start_time
        info(f"    ✓ Пользователей загружено: {users_count:,} ({elapsed:.2f} сек)")

        # 2. Загрузка дружбы
        info("  • COPY friendships.csv...")
        start_time = time.time()
        
        with open(friends_path, "r", encoding="utf-8") as f:
            cur.copy_expert("""
                COPY friendships (user_id, friend_id, since)
                FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """, f)
        
        friends_count = cur.rowcount
        elapsed = time.time() - start_time
        info(f"    ✓ Связей загружено: {friends_count:,} ({elapsed:.2f} сек)")

        cur.close()
        conn.close()
        
        info(f"✅ PostgreSQL: {users_count:,} пользователей, {friends_count:,} связей")
        return True

    except Exception as e:
        info(f"❌ Ошибка COPY в PostgreSQL: {e}")
        traceback.print_exc()
        return False


# =========================================================
#                    Neo4j LOADER
# =========================================================

def load_neo4j(csv_dir, batch_size=10000):
    """Загрузка с правильным использованием APOC"""
    
    users_csv = f"file:///{csv_dir}/users.csv"
    friends_csv = f"file:///{csv_dir}/friendships.csv"
    
    try:
        driver = GraphDatabase.driver(NEO4J["uri"], auth=NEO4J["auth"])
        
        with driver.session() as session:
            # 1. Загрузка пользователей (работает)
            info("  • Загрузка пользователей через APOC...")
            
            q_users = f"""
                CALL apoc.periodic.iterate(
                    "LOAD CSV WITH HEADERS FROM '{users_csv}' AS row RETURN row",
                    "
                        CREATE (:User {{
                            user_id: toInteger(row.user_id),
                            name: row.name,
                            age: CASE WHEN row.age = '' THEN NULL ELSE toInteger(row.age) END,
                            city: row.city,
                            registration_date: CASE WHEN row.registration_date = '' THEN NULL ELSE date(row.registration_date) END
                        }})
                    ",
                    {{batchSize:{batch_size}, parallel:true}}
                );
            """

            session.run(q_users)
            
            # Проверка результата
            users_count = session.run("MATCH (u:User) RETURN count(u) AS c").single()["c"]
            if users_count == 0:
                fail("Neo4j: после загрузки количество User = 0")

            info(f"    ✓ User загружено: {users_count}")
            
            # 2. Загрузка связей
            q_rels = f"""
                CALL apoc.periodic.iterate(
                    "LOAD CSV WITH HEADERS FROM '{friends_csv}' AS row RETURN row",
                    "
                        MATCH (u:User {{user_id: toInteger(row.user_id)}})
                        MATCH (v:User {{user_id: toInteger(row.friend_id)}})
                        CREATE (u)-[:FRIENDS_WITH {{
                            since: CASE WHEN row.since = '' THEN NULL ELSE date(row.since) END,
                            strength: row.strength
                        }}]->(v)
                    ",
                    {{batchSize:{batch_size}, parallel:true}}
                );
            """

            session.run(q_rels)

            # Проверка результата
            rels_count = session.run("MATCH ()-[r:FRIENDS_WITH]->() RETURN count(r) AS c").single()["c"]
            if rels_count == 0:
                fail("Neo4j: после загрузки количество relationships = 0")

            info(f"    ✓ FRIENDS_WITH загружено: {rels_count}")
        
        driver.close()
        return True

    except Exception as e:
        traceback.print_exc()
        fail(f"Ошибка загрузки Neo4j: {e}")

    finally:
        driver.close()

    info("✅ Neo4j: загрузка завершена успешно")
    return True


# =========================================================
#                        MAIN
# =========================================================

def load_dataset(size):
    """Основная функция загрузки датасета"""
    csv_dir = f"generated/{size}"
    if not os.path.isdir(csv_dir):
        fail(f"Папка датасета не найдена: {csv_dir}")
    
    info(f"\n{'='*60}")
    info(f"🚀 ЗАГРУЗКА ДАТАСЕТА: {size.upper()}")
    info(f"{'='*60}")
    
    total_start = time.time()
    
    # Загрузка в PostgreSQL
    logger.info("\n1️⃣ PostgreSQL")
    logger.info("-" * 40)
    pg_success = load_postgres(csv_dir)
    
    # Загрузка в Neo4j
    logger.info("\n2️⃣ Neo4j")
    logger.info("-" * 40)
    neo4j_success = load_neo4j(csv_dir)
    
    total_elapsed = time.time() - total_start
    
    # Итоговый отчет
    logger.info(f"\n{'='*60}")
    logger.info("📊 ИТОГИ ЗАГРУЗКИ:")
    logger.info(f"{'='*60}")
    
    status_pg = "✅ УСПЕХ" if pg_success else "❌ ОШИБКА"
    status_neo4j = "✅ УСПЕХ" if neo4j_success else "❌ ОШИБКА"
    
    logger.info(f"   PostgreSQL: {status_pg}")
    logger.info(f"   Neo4j: {status_neo4j}")
    
    logger.info(f"\n⏱️  Общее время: {total_elapsed:.2f} секунд")
    
    if pg_success and neo4j_success:
        logger.info("\n🎉 ВСЕ ДАННЫЕ УСПЕШНО ЗАГРУЖЕНЫ!")
        logger.info("\n💡 Дальнейшие шаги:")
        logger.info("   1. Выполните финализацию схем:")
        logger.info("      python init_schemas.py finalize")
        logger.info("   2. Запустите тестирование:")
        logger.info("      python benchmark.py")
        return True
    else:
        logger.error("\n⚠️  ЗАГРУЗКА ЗАВЕРШЕНА С ОШИБКАМИ")
        logger.error("   Проверьте логи выше для деталей")
        return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logger.error("Использование: python load_data.py <размер_датасета>")
        logger.error("Пример: python load_data.py tiny")
        logger.error("Доступные размеры: tiny, small, medium, large, xlarge, super-tiny")
        sys.exit(1)
    
    size = sys.argv[1]
    valid_sizes = ["tiny", "small", "medium", "large", "xlarge", "super-tiny"]
    
    if size not in valid_sizes:
        logger.error(f"❌ Неверный размер датасета. Доступные: {', '.join(valid_sizes)}")
        sys.exit(1)
    
    success = load_dataset(size)
    sys.exit(0 if success else 1)