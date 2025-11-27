#!/usr/bin/env python3
"""
УНИВЕРСАЛЬНЫЙ ЗАГРУЗЧИК PostgreSQL и Neo4j
------------------------------------------

Выполняет:

  • Загрузку users.csv и friendships.csv в PostgreSQL (COPY)
  • Загрузку users.csv и friendships.csv в Neo4j (APOC periodic.iterate)
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


# ---------------- Configuration ----------------

POSTGRES = {
    "host": "localhost",
    "port": 5432,
    "database": "benchmark",
    "user": "postgres",
    "password": "password"
}

NEO4J = {
    "uri": "bolt://localhost:7687",
    "auth": ("neo4j", "password")
}

# ------------------------------------------------


def fail(msg):
    print(f"❌ ERROR: {msg}")
    sys.exit(1)


def info(msg):
    print(f"INFO: {msg}")


# =========================================================
#                    PostgreSQL LOADER
# =========================================================

def load_postgres(csv_dir, size):
    import psycopg2
    import os

    users_path = os.path.join(csv_dir, "users.csv")
    friends_path = os.path.join(csv_dir, "friendships.csv")

    info("🐘 Загрузка данных в PostgreSQL...")

    try:
        conn = psycopg2.connect(**POSTGRES)
        conn.autocommit = True
        cur = conn.cursor()

        # =========================================================
        # USERS
        # =========================================================
        info("  • COPY users.csv...")
        with open(users_path, "r", encoding="utf-8") as f:
            cur.copy_expert("""
                COPY users (user_id, name, age, city, registration_date)
                FROM STDIN WITH CSV HEADER
            """, f)

        # =========================================================
        # FRIENDSHIPS
        # =========================================================
        info("  • COPY friendships.csv...")
        with open(friends_path, "r", encoding="utf-8") as f:
            cur.execute("""
                COPY friendships(user_id, friend_id, since)
                FROM '/tmp/friendships.csv'
                CSV HEADER;
            """)

        cur.close()
        conn.close()
        info("✅ PostgreSQL загружен")
        return True

    except Exception as e:
        info(f"❌ ERROR: Ошибка COPY в PostgreSQL: {e}")
        return False


# =========================================================
#                        Neo4j LOADER
# =========================================================

def load_neo4j(csv_dir, batch_size=50000):

    info("📥 Neo4j: начало загрузки через APOC")

    csv_folder = os.path.basename(csv_dir)

    users_csv = f"file:///{csv_folder}/users.csv"
    friends_csv = f"file:///{csv_folder}/friendships.csv"

    if not os.path.exists(os.path.join(csv_dir, "users.csv")):
        fail("Neo4j: отсутствует users.csv")
    if not os.path.exists(os.path.join(csv_dir, "friendships.csv")):
        fail("Neo4j: отсутствует friendships.csv")

    # Подключение
    try:
        driver = GraphDatabase.driver(NEO4J["uri"], auth=NEO4J["auth"])
    except Exception as e:
        fail(f"Ошибка подключения к Neo4j: {e}")

    try:
        with driver.session() as session:

            # -------- USERS --------
            info("  • Импорт узлов User ...")

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

            # -------- RELATIONSHIPS --------
            info("  • Импорт связей FRIENDS_WITH ...")

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

            # -------- UNIQUE INDEX --------

            info("  • Создаём UNIQUE constraint user_id ...")
            session.run("""
                CREATE CONSTRAINT user_id_unique IF NOT EXISTS
                FOR (u:User)
                REQUIRE u.user_id IS UNIQUE
            """)

    except Exception as e:
        traceback.print_exc()
        fail(f"Ошибка загрузки Neo4j: {e}")

    finally:
        driver.close()

    info("🎉 Neo4j завершён успешно")
    return True


# =========================================================
#                        MAIN
# =========================================================

def load_dataset(size):
    csv_dir = f"generated/{size}"
    if not os.path.isdir(csv_dir):
        fail(f"Папка датасета не найдена: {csv_dir}")

    info(f"🚀 Загрузка датасета: {size}")

    load_postgres(csv_dir, size)
    load_neo4j(csv_dir)

    info("✅ ВСЕ ЗАГРУЗКИ ЗАВЕРШЕНЫ УСПЕШНО")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Использование: python3 load_data.py <size>")
        sys.exit(1)

    load_dataset(sys.argv[1])
