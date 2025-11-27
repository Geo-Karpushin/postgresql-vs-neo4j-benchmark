#!/usr/bin/env python3
import subprocess
import time
import sys
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from neo4j import GraphDatabase

# ------------------------- CONFIG -------------------------

POSTGRES = {
    "host": "localhost",
    "user": "postgres",
    "password": "password",
    "port": 5432,
    "database": "benchmark"
}

NEO4J_URI = "bolt://localhost:7687"
NEO4J_AUTH = ("neo4j", "password")

NEO4J_CONTAINER = "database-benchmark-neo4j-1"
NEO4J_VOLUME = "database-benchmark_neo4j_data"   # <-- docker compose volume name

# ----------------------------------------------------------

def sh(cmd):
    """Run shell command with output."""
    print(f"$ {cmd}")
    subprocess.run(cmd, shell=True, check=True)

# --------------------- POSTGRES CLEANUP --------------------

def reset_postgres():
    print("🧹 PostgreSQL: DROP DATABASE benchmark...")
    conn = psycopg2.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
        database="postgres"
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    cur.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'benchmark';")
    cur.execute("DROP DATABASE IF EXISTS benchmark;")
    cur.execute("CREATE DATABASE benchmark;")

    conn.close()
    print("✅ PostgreSQL: создана новая пустая база")

def verify_postgres():
    print("🔍 Проверка PostgreSQL: таблиц быть не должно...")
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public';")
    count = cur.fetchone()[0]
    conn.close()

    if count != 0:
        print(f"❌ В PostgreSQL остались таблицы: {count}")
        sys.exit(1)

    print("✅ PostgreSQL пустая")

# ---------------------- NEO4J CLEANUP ----------------------

def reset_neo4j_container():
    print("🛑 Остановка Neo4j контейнера...")
    subprocess.run(f"docker stop {NEO4J_CONTAINER}", shell=True, check=False)

    print("🗑️ Удаление volumes...")
    subprocess.run(f"docker rm {NEO4J_CONTAINER}", shell=True, check=False)
    subprocess.run(f"docker volume rm {NEO4J_VOLUME}", shell=True, check=False)

    print("▶️ Старт контейнера...")
    sh("docker compose up -d neo4j")


def wait_for_neo4j():
    print("⏳ Ожидание готовности Neo4j...")
    for i in range(60):
        try:
            driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
            with driver.session() as s:
                s.run("RETURN 1")
            print("✅ Neo4j доступен")
            driver.close()
            return
        except:
            time.sleep(1)
    print("❌ Neo4j не поднялся")
    sys.exit(1)


def verify_neo4j():
    print("🔍 Проверка Neo4j: граф должен быть пустым")

    driver = GraphDatabase.driver(NEO4J_URI, auth=NEO4J_AUTH)
    with driver.session() as s:
        cnt = s.run("MATCH (n) RETURN count(n) AS c").single()["c"]

    return cnt

def main():
    print("=========================================")
    print(" 🔄 ПОЛНАЯ ОЧИСТКА PostgreSQL + Neo4j")
    print("=========================================\n")

    reset_postgres()
    verify_postgres()

    # Проверяем Neo4j перед перезапуском
    cnt = verify_neo4j()

    if cnt != 0:
        print(f"♻️ База Neo4j содержит {cnt} узлов — выполняю перезапуск контейнера…")
        reset_neo4j_container()
        wait_for_neo4j()
        verify_neo4j()
    else:
        print("⏭️ Neo4j уже пустой — перезапуск не требуется")

    print("\n🎉 ВСЁ ГОТОВО: обе базы полностью очищены")


if __name__ == "__main__":
    main()
