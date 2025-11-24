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

BATCH_SIZE = 7000

def load_postgres(size):
    files = [
        ("users", f"generated/{size}/users.csv",
         ["user_id", "name", "age", "city", "registration_date"],
         "Users"),

        ("friendships", f"generated/{size}/friendships.csv",
         ["user_id", "friend_id", "since", "strength"],
         "Edges")
    ]

    print(f"📥 PostgreSQL: загрузка датасета {size}...")

    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="benchmark", user="postgres", password="password"
        )
        cursor = conn.cursor()

        for table, path, columns, desc in files:
            print(f"➡️ {desc}: загрузка...")

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

                for _ in tqdm(reader, desc=desc, dynamic_ncols=True,
                              file=sys.stdout, mininterval=0.2, smoothing=0.1):
                    pass

                f.seek(0)
                next(reader := csv.reader(f), None)
                stream = row_stream()

                cursor.copy_expert(
                    f"COPY {table} ({','.join(columns)}) FROM STDIN WITH CSV",
                    wrapped
                )

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Готово")

        return True

    except Exception as e:
        print(f"❌ ERROR: {e}")
        return False

def load_neo4j(size):
    print(f"📥 Neo4j: загрузка датасета {size}...")

    try:
        driver = GraphDatabase.driver("bolt://localhost:7687",
                                      auth=("neo4j", "password"))

        users_csv = f"file:///generated/{size}/users.csv"
        friends_csv = f"file:///generated/{size}/friendships.csv"

        with driver.session() as session:            
            print("👤 Загрузка пользователей...")
            user_query = f"""
                LOAD CSV WITH HEADERS FROM '{users_csv}' AS row
                CREATE (u:User {{
                    user_id: toInteger(row.user_id),
                    name: row.name,
                    age: toInteger(row.age),
                    city: row.city,
                    registration_date: date(row.registration_date)
                }})
            """
            session.run(user_query)

            print("🔗 Загрузка связей...")
            edge_query = f"""
                LOAD CSV WITH HEADERS FROM '{friends_csv}' AS row
                MATCH (u:User {{user_id: toInteger(row.user_id)}})
                MATCH (v:User {{user_id: toInteger(row.friend_id)}})
                CREATE (u)-[:FRIENDS_WITH {{
                    since: date(row.since),
                    strength: row.strength
                }}]->(v)
            """
            session.run(edge_query)

        driver.close()
        print("✅ Neo4j: загрузка завершена")
        return True

    except Exception as e:
        print(f"❌ ERROR loading Neo4j: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Использование: python load_data.py [small|medium|large]")
        exit(1)
    size = sys.argv[1]
    t0 = time.time()
    ok_neo = load_neo4j(size)
    ok_pg = load_postgres(size)
    total = time.time() - t0
    print(f"Время загрузки: {total:.2f}s")
    exit(0 if ok_pg and ok_neo else 1)

if __name__ == "__main__":
    main()
