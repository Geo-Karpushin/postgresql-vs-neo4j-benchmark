#!/usr/bin/env python3
"""
Полная инициализация баз данных PostgreSQL и Neo4j
Включает очистку и создание оптимизированных схем
"""

import psycopg2
from neo4j import GraphDatabase

def cleanup_postgres():
    """Полная очистка PostgreSQL"""
    print("🧹 Очистка PostgreSQL...")
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            database="benchmark", user="postgres", password="password"
        )
        conn.autocommit = True
        cur = conn.cursor()

        print("   • Удаляем таблицы...")
        cur.execute("DROP TABLE IF EXISTS friendships CASCADE")
        cur.execute("DROP TABLE IF EXISTS users CASCADE")

        print("   • Очищаем оставшиеся объекты...")
        cur.execute("""
            DO $$
            DECLARE r RECORD;
            BEGIN
                FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname='public')
                LOOP
                    EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
                END LOOP;
            END $$;
        """)

        cur.close()
        conn.close()
        print("✅ PostgreSQL очищен")
        return True

    except Exception as e:
        print(f"❌ Ошибка очистки PostgreSQL: {e}")
        return False

def cleanup_neo4j():
    """Полная очистка Neo4j"""
    print("🧹 Очистка Neo4j...")
    try:
        driver = GraphDatabase.driver(
            "bolt://localhost:7687",
            auth=("neo4j", "password")
        )

        with driver.session() as session:
            print("   • Удаляем все узлы и связи...")
            delete_query = "MATCH (n) DETACH DELETE n"
            session.run(delete_query)

            print("   • Удаляем constraints...")
            constraints = session.run("SHOW CONSTRAINTS").data()
            for c in constraints:
                session.run(f"DROP CONSTRAINT {c['name']}")

            print("   • Удаляем индексы...")
            indexes = session.run("SHOW INDEXES").data()
            for idx in indexes:
                if idx.get("type") != "LOOKUP":
                    session.run(f"DROP INDEX {idx['name']}")

        driver.close()
        print("✅ Neo4j очищен")
        return True

    except Exception as e:
        print(f"❌ Ошибка очистки Neo4j: {e}")
        return False

def init_postgres_schema():
    """Создание оптимизированной схемы PostgreSQL"""
    print("🗃️ Создание схемы PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="benchmark",
            user="postgres", password="password"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Создание UNLOGGED таблиц для максимальной скорости загрузки
        cursor.execute("""
            CREATE UNLOGGED TABLE users (
                user_id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER,
                city VARCHAR(50),
                registration_date DATE
            )
        """)
        
        cursor.execute("""
            CREATE UNLOGGED TABLE friendships (
                user_id BIGINT,
                friend_id BIGINT,
                since DATE,
                strength VARCHAR(10),
                PRIMARY KEY (user_id, friend_id)
            )
        """)
        
        cursor.close()
        conn.close()
        print("✅ PostgreSQL: UNLOGGED таблицы созданы")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания схемы PostgreSQL: {e}")
        return False

def init_neo4j_schema():
    """Создание оптимизированной схемы Neo4j"""
    print("🕸️ Создание схемы Neo4j...")
    
    try:
        driver = GraphDatabase.driver("bolt://localhost:7687", 
                                    auth=("neo4j", "password"))
        
        with driver.session() as session:
            # Создаем ограничение уникальности (автоматически создаст индекс)
            print("   • Создаем constraints...")
            session.run("""
                CREATE CONSTRAINT user_id_unique 
                IF NOT EXISTS FOR (u:User) 
                REQUIRE u.user_id IS UNIQUE
            """)
            
            # Создаем отдельный индекс для города
            print("   • Создаем индексы...")
            session.run("""
                CREATE INDEX user_city_index 
                IF NOT EXISTS FOR (u:User) 
                ON (u.city)
            """)
            
            # Ждем создания индексов
            print("   • Ожидаем создания индексов...")
            session.run("CALL db.awaitIndexes()")
        
        driver.close()
        print("✅ Neo4j: схема создана")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка создания схемы Neo4j: {e}")
        return False

def enable_postgres_constraints():
    """Включение ограничений PostgreSQL после загрузки данных"""
    print("🔒 Включение ограничений PostgreSQL...")
    
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432, database="benchmark",
            user="postgres", password="password"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Включаем логирование таблиц
        print("   • Включаем логирование таблиц...")
        cursor.execute("ALTER TABLE users SET LOGGED")
        cursor.execute("ALTER TABLE friendships SET LOGGED")
        
        # Добавляем внешние ключи
        print("   • Добавляем внешние ключи...")
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
        
        # Создаем индексы
        print("   • Создаем индексы...")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_friendships_user_friend ON friendships(user_id, friend_id)")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_friendships_friend_user ON friendships(friend_id, user_id)")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_users_city ON users(city)")
        cursor.execute("CREATE INDEX CONCURRENTLY idx_users_age ON users(age)")

        # Обновляем статистику
        print("   • Обновляем статистику...")
        cursor.execute("ANALYZE users")
        cursor.execute("ANALYZE friendships")
        
        cursor.close()
        conn.close()
        
        print("✅ Ограничения PostgreSQL включены")
        return True
        
    except Exception as e:
        print(f"❌ Ошибка включения ограничений PostgreSQL: {e}")
        return False

def main():
    print("🎯 ПОЛНАЯ ИНИЦИАЛИЗАЦИЯ БАЗ ДАННЫХ")
    print("=" * 50)
    
    # Очистка баз
    print("\n1. ОЧИСТКА БАЗ ДАННЫХ")
    success_clean_pg = cleanup_postgres()
    success_clean_neo = cleanup_neo4j()
    
    if not (success_clean_pg and success_clean_neo):
        print("\n❌ Ошибка при очистке баз данных")
        return
    
    # Инициализация схем
    print("\n2. СОЗДАНИЕ СХЕМ")
    success_init_pg = init_postgres_schema()
    success_init_neo = init_neo4j_schema()
    
    if success_init_pg and success_init_neo:
        print("\n✅ Базы данных успешно инициализированы!")
    else:
        print("\n❌ Ошибка при создании схем")

if __name__ == "__main__":
    main()