#!/usr/bin/env python3
"""
Оптимизированная инициализация схемы баз данных PostgreSQL и Neo4j для графовых запросов
"""

import psycopg2
from neo4j import GraphDatabase

def init_postgres_schema():
    print("🗃️ Инициализация схемы PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="benchmark",
            user="postgres", password="password"
        )
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER,
                city VARCHAR(50),
                registration_date DATE
            )
        """)
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS friendships (
                user_id BIGINT,
                friend_id BIGINT,
                since DATE,
                strength VARCHAR(10),
                PRIMARY KEY (user_id, friend_id),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (friend_id) REFERENCES users(user_id)
            )
        """)
        
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_user_friend ON friendships(user_id, friend_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_friend_user ON friendships(friend_id, user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_city ON users(city)")

        cursor.execute("ANALYZE users")
        cursor.execute("ANALYZE friendships")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL схема создана и оптимизирована")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка PostgreSQL: {e}")
        return False

def init_neo4j_schema():
    print("🕸️ Инициализация схемы Neo4j...")
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        
        with driver.session() as session:
            session.run("CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE")
            session.run("CREATE INDEX user_city_index IF NOT EXISTS FOR (u:User) ON (u.city)")
            
            session.run("CALL db.awaitIndexes(300)")
        
        driver.close()
        print("✅ Neo4j схема создана и оптимизирована")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка Neo4j: {e}")
        return False

def main():
    print("🎯 ИНИЦИАЛИЗАЦИЯ И ОПТИМИЗАЦИЯ СХЕМ БАЗ ДАННЫХ")
    
    success_pg = init_postgres_schema()
    success_neo4j = init_neo4j_schema()
    
    if success_pg and success_neo4j:
        print("\n🎉 Схемы баз данных успешно инициализированы и оптимизированы!")
    else:
        print("\n⚠️  Были ошибки при инициализации/оптимизации схем")

if __name__ == "__main__":
    main()