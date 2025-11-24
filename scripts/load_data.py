#!/usr/bin/env python3
import io
import sys
import csv
import time
import psycopg2
from neo4j import GraphDatabase
from tqdm import tqdm

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def count_lines(file_path):
    """Быстрый подсчет строк в файле"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return sum(1 for _ in f)

def load_postgres(size):
    files = [
        ("users", f"generated/{size}/users.csv",
         ["user_id", "name", "age", "city", "registration_date"],
         "👤 Пользователи"),

        ("friendships", f"generated/{size}/friendships.csv",
         ["user_id", "friend_id", "since", "strength"],
         "🔗 Связи")
    ]

    print(f"📥 PostgreSQL: загрузка датасета {size}...")

    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="benchmark", user="postgres", password="password"
        )
        cursor = conn.cursor()

        for table, path, columns, desc in files:
            print(f"{desc}: загрузка...")
            
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)

                def row_stream():
                    for row in reader:
                        yield ",".join(row) + "\n"

                stream = row_stream()

                class Stream:
                    def read(self, _=None):
                        try:
                            return next(stream).encode("utf-8")
                        except StopIteration:
                            return b""

                wrapped = Stream()
                cursor.copy_expert(
                    f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV",
                    wrapped
                )

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ PostgreSQL: загрузка завершена")
        return True

    except Exception as e:
        print(f"❌ ERROR PostgreSQL: {e}")
        return False

def load_neo4j(size):
    files = [
        ("users", f"file:///generated/{size}/users.csv", "👤 Пользователи"),
        ("friendships", f"file:///generated/{size}/friendships.csv", "🔗 Связи")
    ]

    print(f"📥 Neo4j: загрузка датасета {size}...")

    try:
        driver = GraphDatabase.driver("bolt://localhost:7687",
                                      auth=("neo4j", "password"))

        with driver.session() as session:
            print("📊 Создание индексов...")
            session.run("CREATE INDEX user_id_index IF NOT EXISTS FOR (u:User) ON (u.user_id)")
            
            for table, path, desc in files:
                print(f"{desc}: загрузка...")
                
                if table == "users":
                    query = f"""
                        LOAD CSV WITH HEADERS FROM '{path}' AS row
                        CALL (row) {{
                            CREATE (u:User {{
                                user_id: toInteger(row.user_id),
                                name: row.name,
                                age: toInteger(row.age),
                                city: row.city,
                                registration_date: date(row.registration_date)
                            }})
                        }} IN TRANSACTIONS OF 10000 ROWS
                    """
                else:  # friendships
                    query = f"""
                        LOAD CSV WITH HEADERS FROM '{path}' AS row
                        CALL (row) {{
                            MATCH (u:User {{user_id: toInteger(row.user_id)}})
                            MATCH (v:User {{user_id: toInteger(row.friend_id)}})
                            CREATE (u)-[:FRIENDS_WITH {{
                                since: date(row.since),
                                strength: row.strength
                            }}]->(v)
                        }} IN TRANSACTIONS OF 10000 ROWS
                    """
                
                session.run(query)

        driver.close()
        print("✅ Neo4j: загрузка завершена")
        return True

    except Exception as e:
        print(f"❌ ERROR Neo4j: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Использование: python load_data.py [size]")
        exit(1)
    
    size = sys.argv[1]
    t0 = time.time()
    
    ok_pg = load_postgres(size)
    ok_neo = load_neo4j(size)
    
    total = time.time() - t0
    print(f"⏱️ Общее время загрузки: {total:.2f}s")
    
    exit(0 if ok_pg and ok_neo else 1)

if __name__ == "__main__":
    main()