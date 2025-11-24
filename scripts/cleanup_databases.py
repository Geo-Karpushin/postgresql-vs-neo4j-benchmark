#!/usr/bin/env python3
"""
Полная очистка PostgreSQL и Neo4j.
Использование:
    python cleanup_databases.py
"""

import psycopg2
from neo4j import GraphDatabase

def cleanup_postgres():
    print("🧹 Очистка PostgreSQL")
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            database="benchmark",
            user="postgres",
            password="password"
        )

        conn.autocommit = True
        cur = conn.cursor()

        print("   • Удаляем материализованные представления...")
        cur.execute("""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT matviewname FROM pg_matviews)
                LOOP
                    EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS ' || quote_ident(r.matviewname) || ' CASCADE';
                END LOOP;
            END $$;
        """)

        print("   • Очищаем таблицы...")
        cur.execute("""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (
                    SELECT tablename
                    FROM pg_tables
                    WHERE schemaname='public'
                )
                LOOP
                    EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.tablename) || ' RESTART IDENTITY CASCADE';
                END LOOP;
            END $$;
        """)

        print("   • Делаем VACUUM ANALYZE...")
        cur.execute("VACUUM ANALYZE")

        cur.close()
        conn.close()

        print("✅ PostgreSQL очищен")
        return True

    except Exception as e:
        print(f"❌ Ошибка PostgreSQL очистки: {e}")
        return False

def cleanup_neo4j():
    print("🧹 Очистка Neo4j...")

    try:
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")
        )

        with driver.session() as session:
            print("   • Удаляем узлы порциями...")

            delete_query = """
            CALL () {
                MATCH (n)
                WITH n
                DETACH DELETE n
            } IN TRANSACTIONS OF 50000 ROWS;
            """

            session.run(delete_query)

            print("   • Удаляем constraints...")

            constraints = session.run("SHOW CONSTRAINTS").data()

            for c in constraints:
                name = c["name"]
                print(f"     - DROP CONSTRAINT {name}")
                session.run(f"DROP CONSTRAINT {name}")

            print("   • Удаляем indexes...")

            indexes = session.run("SHOW INDEXES").data()

            for idx in indexes:
                name = idx["name"]
                if idx.get("type") == "LOOKUP":
                    continue

                print(f"     - DROP INDEX {name}")
                session.run(f"DROP INDEX {name}")

        driver.close()
        print("✅ Neo4j полностью очищен")
        return True

    except Exception as e:
        print(f"❌ Ошибка Neo4j очистки: {e}")
        return False

    except Exception as e:
        print(f"❌ Ошибка Neo4j очистки: {e}")
        return False

def main():
    ok_pg = cleanup_postgres()
    ok_neo = cleanup_neo4j()

    if ok_pg and ok_neo:
        exit(0)
    else:
        print("\n⚠️ Очистка завершена с ошибками")
        exit(1)


if __name__ == "__main__":
    main()
