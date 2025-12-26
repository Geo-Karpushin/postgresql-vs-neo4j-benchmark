#!/usr/bin/env python3
"""
Скрипт очистки баз данных PostgreSQL и Neo4j.
Проверяет, запущены ли БД, при необходимости запускает docker-compose.
"""
import subprocess
import time
import sys
import os
import argparse
from dataclasses import dataclass
from typing import Dict, Optional
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from neo4j import GraphDatabase, BoltDriver

# ------------------------- CONFIG -------------------------

@dataclass
class DatabaseConfig:
    """Конфигурация подключения к базам данных."""
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str = "postgres"
    postgres_password: str = "password"
    postgres_database: str = "benchmark"
    
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"


class DockerManager:
    """Управление docker-контейнерами."""
    
    def __init__(self, config_name: str = "medium"):
        self.config_name = config_name
        self.config_file = f"{config_name}.yaml"
        self.project_name = "database-benchmark"
        
    @property
    def container_names(self) -> Dict[str, str]:
        """Имена контейнеров и volumes."""
        return {
            "neo4j": f"{self.project_name}-neo4j-1",
            "neo4j_volume": f"{self.project_name}_neo4j_data",
            "postgres": f"{self.project_name}-postgres-1",
            "postgres_volume": f"{self.project_name}_postgres_data",
        }
    
    def run_command(self, cmd: str) -> None:
        """Выполнить команду с выводом."""
        print(f"$ {cmd}")
        subprocess.run(cmd, shell=True, check=True)
    
    def start(self) -> None:
        """Запустить docker-compose."""
        print(f"🚀 Запуск docker-compose: {self.config_file}")
        
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"Конфиг не найден: {self.config_file}")
        
        self.run_command(f"docker-compose -f {self.config_file} up -d")
    
    def stop(self) -> None:
        """Остановить docker-compose."""
        print("🛑 Остановка docker-compose...")
        self.run_command(f"docker-compose -f {self.config_file} down")
    
    def remove_neo4j_volume(self) -> None:
        """Удалить volume Neo4j."""
        containers = self.container_names
        print(f"🗑️  Удаление volume Neo4j: {containers['neo4j_volume']}")
        
        subprocess.run(f"docker rm {containers['neo4j']}", 
                      shell=True, check=False, capture_output=True)
        subprocess.run(f"docker volume rm {containers['neo4j_volume']}", 
                      shell=True, check=False, capture_output=True)


class PostgresManager:
    """Управление PostgreSQL."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection_params = {
            "host": config.postgres_host,
            "port": config.postgres_port,
            "user": config.postgres_user,
            "password": config.postgres_password,
            "database": "postgres",
        }
    
    def is_running(self, timeout: int = 2) -> bool:
        """Проверить, доступен ли PostgreSQL."""
        try:
            conn = psycopg2.connect(**self.connection_params, connect_timeout=timeout)
            conn.close()
            return True
        except Exception:
            return False
    
    def wait_for_availability(self, max_attempts: int = 90) -> None:
        """Дождаться доступности PostgreSQL."""
        print("⏳ Ожидание PostgreSQL...")
        
        for attempt in range(max_attempts):
            if self.is_running():
                print("✅ PostgreSQL доступен")
                return
            time.sleep(2)
        
        raise TimeoutError("PostgreSQL не стал доступен")
    
    def reset_database(self) -> None:
        """Сбросить базу данных benchmark."""
        print("🧹 PostgreSQL: очистка базы benchmark...")
        
        conn = psycopg2.connect(**self.connection_params)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        
        # Завершить все подключения к базе
        cur.execute("""
            SELECT pg_terminate_backend(pid) 
            FROM pg_stat_activity 
            WHERE datname = %s;
        """, (self.config.postgres_database,))
        
        # Удалить и создать базу заново
        cur.execute(f"DROP DATABASE IF EXISTS {self.config.postgres_database};")
        cur.execute(f"CREATE DATABASE {self.config.postgres_database};")
        
        conn.close()
        print("✅ PostgreSQL: база создана заново")
    
    def verify_empty(self) -> None:
        """Проверить, что база данных пуста."""
        print("🔍 Проверка PostgreSQL: таблиц быть не должно...")
        
        conn_params = self.connection_params.copy()
        conn_params["database"] = self.config.postgres_database
        
        try:
            conn = psycopg2.connect(**conn_params)
            cur = conn.cursor()
            cur.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public';")
            count = cur.fetchone()[0]
            conn.close()
            
            if count != 0:
                raise ValueError(f"В PostgreSQL остались {count} таблиц")
            
            print("✅ PostgreSQL пустая")
        except Exception as e:
            print(f"❌ Ошибка проверки PostgreSQL: {e}")
            sys.exit(1)


class Neo4jManager:
    """Управление Neo4j."""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.driver: Optional[BoltDriver] = None
    
    def is_running(self, timeout: int = 2) -> bool:
        """Проверить, доступен ли Neo4j."""
        try:
            self.driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=(self.config.neo4j_user, self.config.neo4j_password),
                connection_timeout=timeout
            )
            with self.driver.session() as session:
                session.run("RETURN 1")
            self.driver.close()
            return True
        except Exception:
            return False
    
    def wait_for_availability(self, max_attempts: int = 90) -> None:
        """Дождаться доступности Neo4j."""
        print("⏳ Ожидание Neo4j...")
        
        for attempt in range(max_attempts):
            if self.is_running():
                print("✅ Neo4j доступен")
                return
            time.sleep(2)
        
        raise TimeoutError("Neo4j не стал доступен")
    
    def get_node_count(self) -> int:
        """Получить количество узлов в графе."""
        try:
            self.driver = GraphDatabase.driver(
                self.config.neo4j_uri,
                auth=(self.config.neo4j_user, self.config.neo4j_password)
            )
            with self.driver.session() as session:
                result = session.run("MATCH (n) RETURN count(n) AS count")
                count = result.single()["count"]
            self.driver.close()
            return count
        except Exception as e:
            print(f"❌ Ошибка подключения к Neo4j: {e}")
            return -1
    
    def verify_empty(self) -> bool:
        """Проверить, что граф пуст."""
        print("🔍 Проверка Neo4j: граф должен быть пустым...")
        
        count = self.get_node_count()
        if count == 0:
            print("✅ Neo4j пустой")
            return True
        elif count > 0:
            print(f"⚠️  Neo4j содержит {count} узлов")
            return False
        else:
            print("❌ Не удалось проверить Neo4j")
            return False


class DatabaseCleaner:
    """Основной класс для очистки баз данных."""
    
    def __init__(self, config_name: str = "medium"):
        self.db_config = DatabaseConfig()
        self.docker = DockerManager(config_name)
        self.postgres = PostgresManager(self.db_config)
        self.neo4j = Neo4jManager(self.db_config)
    
    def ensure_databases_running(self) -> None:
        """Убедиться, что БД запущены."""
        print("🔍 Проверка состояния баз данных...")
        
        postgres_running = self.postgres.is_running()
        neo4j_running = self.neo4j.is_running()
        
        if not postgres_running or not neo4j_running:
            print("⚠️  Не все БД запущены. Запуск docker-compose...")
            self.docker.start()
            self.postgres.wait_for_availability()
            self.neo4j.wait_for_availability()
        else:
            print("✅ Все БД запущены")
    
    def cleanup_postgres(self) -> None:
        """Очистить PostgreSQL."""
        self.postgres.reset_database()
        self.postgres.verify_empty()
    
    def cleanup_neo4j(self) -> None:
        """Очистить Neo4j."""
        is_empty = self.neo4j.verify_empty()
        
        if not is_empty:
            print("♻️  Neo4j не пустой — выполняется очистка...")
            self.docker.stop()
            self.docker.remove_neo4j_volume()
            self.docker.start()
            self.neo4j.wait_for_availability()
            
            # Проверяем после очистки
            if not self.neo4j.verify_empty():
                raise RuntimeError("Neo4j не был очищен")
        else:
            print("⏭️  Neo4j уже пустой — очистка не требуется")
    
    def restart_containers(self) -> None:
        """Перезапустить контейнеры."""
        print("\n🔄 Перезапуск контейнеров...")
        self.docker.stop()
        self.docker.start()
        self.postgres.wait_for_availability()
        self.neo4j.wait_for_availability()
    
    def run(self) -> None:
        """Выполнить полный процесс очистки."""
        print("=" * 50)
        print(" 🔄 ПОЛНАЯ ОЧИСТКА PostgreSQL + Neo4j")
        print(f" 📁 Конфиг: {self.docker.config_file}")
        print("=" * 50 + "\n")
        
        try:
            # Шаг 1: Убедиться, что БД запущены
            self.ensure_databases_running()
            
            # Шаг 2: Очистить PostgreSQL
            self.cleanup_postgres()
            
            # Шаг 3: Очистить Neo4j
            self.cleanup_neo4j()
            
            # Шаг 4: Перезапустить контейнеры
            self.restart_containers()
            
            print("\n🎉 ВСЁ ГОТОВО: обе базы полностью очищены")
            
        except Exception as e:
            print(f"\n❌ Ошибка: {e}")
            sys.exit(1)


def main():
    """Точка входа."""
    parser = argparse.ArgumentParser(description='Очистка баз данных PostgreSQL и Neo4j')
    parser.add_argument('-c', '--config', default='medium', 
                       help='Имя конфига docker-compose (без расширения .yaml)')
    
    args = parser.parse_args()
    
    cleaner = DatabaseCleaner(args.config)
    cleaner.run()


if __name__ == "__main__":
    main()