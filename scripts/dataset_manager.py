#!/usr/bin/env python3
"""
Менеджер датасетов для корректного тестирования разных размеров.
Конфигурируемая версия с настройками для каждого размера датасета.
"""

import subprocess
import sys
import time
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any

DATA_DIR = Path("generated")
SCRIPTS_DIR = Path("scripts")
RESULTS_DIR = Path("results")
POSTGRES_CONTAINER = "database-benchmark-postgres-1"
NEO4J_CONTAINER = "database-benchmark-neo4j-1"
DOCKER_RETRIES = 4
DOCKER_BACKOFF = 2

# Конфигурация датасетов
DATASETS_CONFIG = {
    "small": {
        "users": 50_000,
        "avg_friends": 20,
        "iterations": 5,
        "query_runs": {
            "simple_friends": 50,
            "friends_of_friends": 400,
            "mutual_friends": 50,
            "friend_recommendations": 20,
            "shortest_path": 5
        }
    },
    "medium": {
        "users": 500_000,
        "avg_friends": 15,
        "iterations": 3,
        "query_runs": {
            "simple_friends": 30,
            "friends_of_friends": 300,
            "mutual_friends": 30,
            "friend_recommendations": 15,
            "shortest_path": 3
        }
    },
    "large": {
        "users": 2_000_000,
        "avg_friends": 12,
        "iterations": 2,
        "query_runs": {
            "simple_friends": 20,
            "friends_of_friends": 250,
            "mutual_friends": 20,
            "friend_recommendations": 10,
            "shortest_path": 2
        }
    },
    "x-large": {
        "users": 5_000_000,
        "avg_friends": 10,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 10,
            "friends_of_friends": 200,
            "mutual_friends": 10,
            "friend_recommendations": 5,
            "shortest_path": 1
        }
    },
    "xx-large": {
        "users": 10_000_000,
        "avg_friends": 8,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 5,
            "friends_of_friends": 100,
            "mutual_friends": 5,
            "friend_recommendations": 3,
            "shortest_path": 1
        }
    }
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("dataset_manager")

def run_cmd(cmd: List[str], capture: bool = True, check: bool = True) -> subprocess.CompletedProcess:
    """Унифицированный запуск команд (без shell=True)."""
    return subprocess.run(cmd, text=True, capture_output=capture, check=check)

def retry_cmd(cmd: List[str], retries: int = DOCKER_RETRIES, backoff: int = DOCKER_BACKOFF) -> bool:
    """Выполнить команду с retry/backoff. Возвращает True при успехе, False при неудаче."""
    attempt = 0
    delay = backoff
    while attempt < retries:
        try:
            run_cmd(cmd)
            return True
        except subprocess.CalledProcessError as e:
            attempt += 1
            log.warning("Команда не удалась (попытка %d/%d): %s — %s", attempt, retries, " ".join(cmd), e.stderr.strip()[:200])
            if attempt < retries:
                time.sleep(delay)
                delay *= 2
    return False

class DatasetManager:
    def __init__(self, dry_run: bool = False):
        self.base_path = DATA_DIR
        self.scripts_path = SCRIPTS_DIR
        self.results_path = RESULTS_DIR
        self.dry_run = dry_run
        self.results_path.mkdir(parents=True, exist_ok=True)
        self.config = DATASETS_CONFIG

    def _ensure_dataset_files(self, size: str) -> bool:
        users = self.base_path / size / "users.csv"
        friendships = self.base_path / size / "friendships.csv"
        if not users.exists():
            log.error("Отсутствует файл: %s", users)
            return False
        if not friendships.exists():
            log.error("Отсутствует файл: %s", friendships)
            return False
        return True

    def initialize_databases(self) -> bool:
        log.info("🗃️ Инициализация схем баз данных...")
        try:
            run_cmd([sys.executable, str(self.scripts_path / "init_database.py")])
            log.info("✅ Схемы баз данных инициализированы")
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка инициализации: %s", e.stderr.strip())
            return False
        
    def cleanup_databases(self) -> bool:
        log.info("🧹 Очистка баз данных...")
        try:
            subprocess.run(
                [sys.executable, str(self.scripts_path / "cleanup_databases.py")],
                check=True
            )
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка очистки: %s", e)
            return False

    def generate_dataset(self, size: str) -> bool:
        log.info("🎯 Генерация датасета %s...", size)
        try:
            config = self.config.get(size, {})
            subprocess.run(
                [sys.executable, str(self.scripts_path / "data_generator.py"), 
                str(config.get("users", 50000)), 
                str(config.get("avg_friends", 15)), 
                size],
                check=True
            )
            log.info("✅ Датасет %s сгенерирован", size)
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка генерации: %s", e)
            return False

    def copy_to_containers(self, size: str) -> bool:
        log.info("📦 Копирование %s датасета в контейнеры...", size)
        if not self._ensure_dataset_files(size):
            return False

        users_host = str(self.base_path / size / "users.csv")
        friends_host = str(self.base_path / size / "friendships.csv")

        cp_pg_users = ["docker", "cp", users_host, f"{POSTGRES_CONTAINER}:/tmp/users.csv"]
        cp_pg_friends = ["docker", "cp", friends_host, f"{POSTGRES_CONTAINER}:/tmp/friendships.csv"]

        neo4j_dir = f"/var/lib/neo4j/import/{size}"
        mkdir_neo = ["docker", "exec", NEO4J_CONTAINER, "mkdir", "-p", neo4j_dir]
        cp_neo_users = ["docker", "cp", users_host, f"{NEO4J_CONTAINER}:{neo4j_dir}/users.csv"]
        cp_neo_friends = ["docker", "cp", friends_host, f"{NEO4J_CONTAINER}:{neo4j_dir}/friendships.csv"]

        steps = [
            (mkdir_neo, "Создание папки Neo4j"),
            (cp_pg_users, "Копирование users -> Postgres"),
            (cp_pg_friends, "Копирование friendships -> Postgres"),
            (cp_neo_users, "Копирование users -> Neo4j"),
            (cp_neo_friends, "Копирование friendships -> Neo4j"),
        ]

        for cmd, desc in steps:
            log.info("  • %s: %s", desc, " ".join(cmd) if self.dry_run else "")
            if self.dry_run:
                continue
            ok = retry_cmd(cmd)
            if not ok:
                log.error("  ❌ Ошибка шага: %s", desc)
                return False

        log.info("✅ Датасет %s скопирован в контейнеры", size)
        return True

    def load_to_databases(self, size: str) -> bool:
        log.info("📥 Загрузка %s датасета в базы...", size)

        loader = self.scripts_path / "load_data.py"
        if not loader.exists():
            log.error("Скрипт загрузки не найден: %s", loader)
            return False

        try:
            subprocess.run(
                [sys.executable, str(loader), size],
                check=True
            )
            log.info("✅ Загрузка в базы завершена успешно")
            return True

        except subprocess.CalledProcessError:
            log.error("❌ Ошибка загрузки данных")
            return False

    def run_benchmarks(self, size: str) -> bool:
        log.info("🚀 Запуск бенчмарков для %s...", size)

        runner = self.scripts_path / "benchmark_runner.py"
        if not runner.exists():
            log.error("Скрипт бенчмарков не найден: %s", runner)
            return False

        try:
            config = self.config.get(size, {})
            
            # Создаем временный конфиг файл для query_runs
            config_file = self.results_path / f"benchmark_config_{size}.json"
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config.get("query_runs", {}), f, indent=2)
            
            subprocess.run(
                [sys.executable, str(runner), size,
                 "--config", str(config_file)],
                check=True
            )
            
            # Удаляем временный конфиг файл
            config_file.unlink(missing_ok=True)
            
            log.info("✅ Бенчмарки для %s завершены", size)
            return True

        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка выполнения бенчмарков: %s", e)
            return False

    def process_size(self, size: str) -> dict:
        """Обработка одного размера: выполняет все шаги и возвращает словарь с результатами."""
        result = {"size": size, "timestamps": {}, "durations": {}, "status": "unknown", "config": self.config.get(size, {})}

        ok = self.cleanup_databases()
        if not ok:
            result["status"] = "cleanup_failed"
            return result
        
        ok = self.initialize_databases()
        if not ok:
            log.error("Инициализация схем не удалась. Выход.")
            return result

        start = time.time()
        t0 = time.time()
        ok = self.generate_dataset(size)
        result["timestamps"]["generate_start"] = t0
        result["durations"]["generate"] = time.time() - t0
        if not ok:
            result["status"] = "generate_failed"
            return result

        t1 = time.time()
        ok = self.copy_to_containers(size)
        result["timestamps"]["copy_start"] = t1
        result["durations"]["copy"] = time.time() - t1
        if not ok:
            result["status"] = "copy_failed"
            return result

        t2 = time.time()
        ok = self.load_to_databases(size)
        result["timestamps"]["load_start"] = t2
        result["durations"]["load"] = time.time() - t2
        if not ok:
            result["status"] = "load_failed"
            return result

        t3 = time.time()
        ok = self.run_benchmarks(size)
        result["timestamps"]["benchmark_start"] = t3
        result["durations"]["benchmark"] = time.time() - t3
        if not ok:
            result["status"] = "bench_failed"
            return result
        
        ok = self.cleanup_databases()
        if not ok:
            result["status"] = "cleanup_failed"
            return result

        result["status"] = "ok"
        result["total_time"] = time.time() - start
        return result

def main():
    if len(sys.argv) < 2:
        print("Использование: python dataset_manager.py [small / medium / large / x-large / xx-large / all] [--dry-run]")
        return

    target = sys.argv[1]
    dry = "--dry-run" in sys.argv

    manager = DatasetManager(dry_run=dry)

    sizes = ["small", "medium", "large", "x-large", "xx-large"] if target == "all" else [target]

    for size in sizes:
        log.info("=" * 60)
        log.info("🎯 ОБРАБОТКА ДАТАСЕТА: %s", size.upper())
        log.info("📊 Конфигурация: %s пользователей, %s средних друзей", 
                manager.config.get(size, {}).get("users", "N/A"),
                manager.config.get(size, {}).get("avg_friends", "N/A"))
        
        oks = 0
        for i in range(1, DATASETS_CONFIG[size]["iterations"]+1):
            log.info("🔄 ИТЕРАЦИЯ %i/%i", i, DATASETS_CONFIG[size]["iterations"])
            res = manager.process_size(size)
            out_file = manager.results_path / f"{size}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(res, f, ensure_ascii=False, indent=2)
            log.info("Сохранён результат: %s", out_file)
            if res["status"] != "ok":
                oks += 1
                log.warning("Обработка %s завершилась с статусом: %s", size, res["status"])
            else:
                log.info("🎉 %s датасет полностью обработан! (время %.2fs)", size, res["total_time"])
        
        log.info("ОБРАБОТКА ДАТАСЕТА ЗАВЕРШЕНА, успешно: %i/%i", oks, DATASETS_CONFIG[size]["iterations"])

if __name__ == "__main__":
    main()