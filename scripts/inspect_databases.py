#!/usr/bin/env python3
"""
Получение количества записей в PostgreSQL и Neo4j.
Использует APOC, если он доступен.
"""

import logging
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

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

def get_postgres_counts():
    logger.info("📦 Получение количества строк в PostgreSQL...")
    results = {}

    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema='public';
            """)
            tables = [row[0] for row in cur.fetchall()]

            for table in tables:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                results[table] = count

        conn.close()

    except Exception as e:
        logger.error(f"❌ Ошибка PostgreSQL: {e}")

    return results

def get_neo4j_counts():
    logger.info("🕸️ Получение количества узлов и связей в Neo4j...")
    results = {}

    driver = None
    try:
        driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=NEO4J_CONFIG["auth"]
        )

        with driver.session() as session:

            logger.info("   • Проверяем наличие APOC...")

            try:
                apoc_info = session.run("RETURN apoc.version() AS version").single()
                if apoc_info:
                    logger.info(f"     ✓ APOC найден ({apoc_info['version']}) — используем apoc.meta.stats")

                    stats = session.run("CALL apoc.meta.stats()").single()

                    results["nodes_total"] = stats["nodeCount"]
                    results["relationships_total"] = stats["relCount"]

                    results["nodes_by_label"] = stats["labels"]
                    results["relationships_by_type"] = stats["relTypesCount"]

                    return results

            except Exception:
                logger.info("     ⚠️ APOC недоступен — fallback на обычные запросы")

            totals = session.run("MATCH (n) RETURN count(n) AS c").single()
            results["nodes_total"] = totals["c"]

            totals = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()
            results["relationships_total"] = totals["c"]

            label_rows = session.run("""
                CALL db.labels() YIELD label
                CALL {
                    WITH label
                    MATCH (n:`%s`)
                    RETURN count(n) as c
                }
                RETURN label, c
            """ % "%s")

            results["nodes_by_label"] = {
                row["label"]: row["c"] for row in label_rows
            }

            rel_rows = session.run("""
                CALL db.relationshipTypes() YIELD relationshipType AS type
                CALL {
                    WITH type
                    MATCH ()-[r:`%s`]->()
                    RETURN count(r) AS c
                }
                RETURN type, c
            """ % "%s")

            results["relationships_by_type"] = {
                row["type"]: row["c"] for row in rel_rows
            }

    except Exception as e:
        logger.error(f"❌ Ошибка Neo4j: {e}")

    finally:
        if driver:
            driver.close()

    return results

def main():
    print("📊 СБОР СТАТИСТИКИ ИЗ БАЗ ДАННЫХ")
    print("=" * 50)

    pg = get_postgres_counts()
    neo = get_neo4j_counts()

    print("=== PostgreSQL ===")
    for table, count in pg.items():
        print(f"  {table}: {count}")

    print("=== Neo4j ===")
    print(f"  Узлов всего: {neo.get('nodes_total')}")
    print(f"  Связей всего: {neo.get('relationships_total')}")

    print("Узлы по лейблам:")
    for label, count in neo.get("nodes_by_label", {}).items():
        print(f"  {label}: {count}")

    print("Связи по типам:")
    for rtype, count in neo.get("relationships_by_type", {}).items():
        print(f"  {rtype}: {count}")

if __name__ == "__main__":
    main()
