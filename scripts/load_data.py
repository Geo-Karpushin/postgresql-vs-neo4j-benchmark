#!/usr/bin/env python3
import sys
import time
import psycopg2
from neo4j import GraphDatabase
from concurrent.futures import ThreadPoolExecutor

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

def load_postgres_fast(size):
    print(f"📥 PostgreSQL: загрузка {size}...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="benchmark", user="postgres", password="password"
        )
        cursor = conn.cursor()
        
        cursor.execute("SET session_replication_role = 'replica';")
        
        print("👤 Загрузка пользователей...")
        with open(f"generated/{size}/users.csv", "r", encoding="utf-8") as f:
            cursor.copy_expert(
                "COPY users (user_id, name, age, city, registration_date) FROM STDIN WITH CSV HEADER",
                f
            )
        
        print("🔗 Загрузка связей...")
        with open(f"generated/{size}/friendships.csv", "r", encoding="utf-8") as f:
            cursor.copy_expert(
                "COPY friendships (user_id, friend_id, since, strength) FROM STDIN WITH CSV HEADER",
                f
            )

        cursor.execute("SET session_replication_role = 'origin';")
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL: завершено")
        return True

    except Exception as e:
        print(f"❌ PostgreSQL ошибка: {e}")
        return False

def load_neo4j_fast(size):
    print(f"📥 Neo4j: загрузка {size}...")

    try:
        driver = GraphDatabase.driver("bolt://localhost:7687",
                                    auth=("neo4j", "password"))

        with driver.session() as session:
            print("👤 Загрузка пользователей...")
            user_query = f"""
            LOAD CSV WITH HEADERS FROM 'file:///generated/{size}/users.csv' AS row
            CALL (row) {{
                CREATE (u:User {{
                    user_id: toInteger(row.user_id),
                    name: row.name,
                    age: toInteger(row.age),
                    city: row.city,
                    registration_date: date(row.registration_date)
                }})
            }} IN TRANSACTIONS OF 25000 ROWS
            """
            session.run(user_query)
            
            print("🔗 Загрузка связей...")
            batch_size = 5000
            
            friends_query = f"""
            LOAD CSV WITH HEADERS FROM 'file:///generated/{size}/friendships.csv' AS row
            CALL (row) {{
                MATCH (u:User {{user_id: toInteger(row.user_id)}})
                MATCH (v:User {{user_id: toInteger(row.friend_id)}})
                CREATE (u)-[:FRIENDS_WITH {{
                    since: date(row.since),
                    strength: row.strength
                }}]->(v)
            }} IN TRANSACTIONS OF {batch_size} ROWS
            """
            
            start_time = time.time()
            session.run(friends_query)
            load_time = time.time() - start_time
            
            print(f"✅ Связи загружены за {load_time:.1f}с")

        driver.close()
        print("✅ Neo4j: завершено")
        return True

    except Exception as e:
        print(f"❌ Neo4j ошибка: {e}")
        return False

def load_parallel(size):
    print(f"🚀 Параллельная загрузка {size}")
    
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_pg = executor.submit(load_postgres_fast, size)
        future_neo = executor.submit(load_neo4j_fast, size)
        
        results = [future_pg.result(), future_neo.result()]
    
    return all(results)

def enable_constraints():
    print("🔒 Включение ограничений PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="benchmark",
            user="postgres", password="password"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        cursor.execute("ALTER TABLE users SET LOGGED")
        cursor.execute("ALTER TABLE friendships SET LOGGED")
        
        cursor.execute("""
            ALTER TABLE friendships 
            ADD CONSTRAINT fk_friendships_user 
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        """)
        cursor.execute("""
            ALTER TABLE friendships 
            ADD CONSTRAINT fk_friendships_friend 
            FOREIGN KEY (friend_id) REFERENCES users(user_id)
        """)
        
        cursor.execute("CREATE INDEX CONCURRENTLY idx_friendships_user_friend ON friendships(user_id, friend_id)")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_friendships_friend_user ON friendships(friend_id, user_id)")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_users_city ON users(city)")

        cursor.execute("ANALYZE users")
        cursor.execute("ANALYZE friendships")
        
        cursor.close()
        conn.close()
        
        print("✅ Ограничения включены")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка включения ограничений: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Использование: python load_fast.py [size]")
        exit(1)
    
    size = sys.argv[1]
    
    print(f"⚡ Загрузка данных {size.upper()}")
    print("=" * 40)
    t0 = time.time()
    
    success = load_parallel(size)
    
    if success:
        enable_constraints()
    
    total = time.time() - t0
    print("=" * 40)
    print(f"⏱️ Общее время: {total:.1f}с")
    
    exit(0 if success else 1)

if __name__ == "__main__":
    main()