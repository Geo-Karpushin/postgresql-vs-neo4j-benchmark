#!/usr/bin/env python3
"""
Оптимизированная инициализация схемы баз данных PostgreSQL и Neo4j для графовых запросов
"""

import psycopg2
from neo4j import GraphDatabase

# ================== POSTGRESQL ==================
def init_postgres_schema():
    print("🗃️ Инициализация схемы PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="benchmark",
            user="postgres", password="password"
        )
        cursor = conn.cursor()
        
        # ===== Создаем таблицы =====
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
        
        # ===== Индексы для графовых запросов =====
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_user ON friendships(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_friend ON friendships(friend_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_user_friend ON friendships(user_id, friend_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_friend_user ON friendships(friend_id, user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_bidirectional ON friendships(user_id, friend_id) INCLUDE (since)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_friendships_reverse ON friendships(friend_id, user_id) INCLUDE (since)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_city ON users(city)")
        
        # ===== Материализованное представление для friends_of_friends =====
        cursor.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS friends_of_friends_mv AS
            WITH all_friends AS (
                SELECT user_id, friend_id FROM friendships
                UNION
                SELECT friend_id AS user_id, user_id AS friend_id FROM friendships
            )
            SELECT f1.user_id, f2.friend_id AS fof_id
            FROM all_friends f1
            JOIN all_friends f2 ON f1.friend_id = f2.user_id
            WHERE f2.friend_id <> f1.user_id
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fof_user ON friends_of_friends_mv(user_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fof_fof ON friends_of_friends_mv(fof_id)")
        
        # ===== Обновляем статистику =====
        cursor.execute("ANALYZE users")
        cursor.execute("ANALYZE friendships")
        cursor.execute("ANALYZE friends_of_friends_mv")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ PostgreSQL схема создана и оптимизирована")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка PostgreSQL: {e}")
        return False

# ================== NEO4J ==================
def init_neo4j_schema():
    print("🕸️ Инициализация схемы Neo4j...")
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))
        
        with driver.session() as session:
            # ===== Constraints и индексы узлов =====
            session.run("CREATE CONSTRAINT user_id_unique IF NOT EXISTS FOR (u:User) REQUIRE u.user_id IS UNIQUE")
            session.run("CREATE INDEX user_city_index IF NOT EXISTS FOR (u:User) ON (u.city)")
            session.run("CREATE INDEX user_properties_index IF NOT EXISTS FOR (u:User) ON (u.user_id, u.city, u.age)")
            
            # ===== Индексы для связей =====
            session.run("""
                CREATE INDEX friends_strength_index IF NOT EXISTS
                FOR ()-[r:FRIENDS_WITH]-() ON (r.strength)
            """)
            session.run("""
                CREATE INDEX friends_since_index IF NOT EXISTS
                FOR ()-[r:FRIENDS_WITH]-() ON (r.since)
            """)

            session.run("CALL db.awaitIndexes(300)")
            session.run("""
                CALL db.index.fulltext.createNodeIndex(
                    'user_search_index', 
                    ['User'], 
                    ['name', 'city']
                )
            """)
        
        driver.close()
        print("✅ Neo4j схема создана и оптимизирована")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка Neo4j: {e}")
        return False

# ================== MAIN ==================
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
