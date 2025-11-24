#!/usr/bin/env python3
"""
Менеджер датасетов для корректного тестирования разных размеров.
Улучшенная версия:
 - вынесён loader в scripts/load_data.py
 - проверка наличия файлов
 - retry для docker-команд
 - логирование и запись результатов в results/
"""

import subprocess
import sys
import time
import json
import logging
from pathlib import Path
from typing import List, Optional

DATA_DIR = Path("generated")
SCRIPTS_DIR = Path("scripts")
RESULTS_DIR = Path("results")
POSTGRES_CONTAINER = "database-benchmark-postgres-1"
NEO4J_CONTAINER = "database-benchmark-neo4j-1"
DOCKER_RETRIES = 4
DOCKER_BACKOFF = 2

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("dataset_manager")

def run_cmd(cmd: List[str], capture: bool = True, check: bool = True) -> subprocess.CompletedProcess:
    """Унифицированный запуск команд (без shell=True)."""
    return subprocess.run(cmd, text=True, capture_output=capture, check=check)

def stream_cmd(cmd: List[str]) -> int:
    """Запуск команды с realtime-выводом stdout/stderr."""
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    assert process.stdout is not None
    for line in process.stdout:
        print(line.rstrip())

    process.wait()
    return process.returncode

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

    def generate_dataset(self, size: str) -> bool:
        log.info("🎯 Генерация датасета %s...", size)
        try:
            subprocess.run(
                [sys.executable, str(self.scripts_path / "data_generator.py"), size],
                check=True
            )
            log.info("✅ Датасет %s сгенерирован", size)
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка генерации")
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
            subprocess.run(
                [sys.executable, str(runner), size],
                check=True
            )
            log.info("✅ Бенчмарки для %s завершены", size)
            return True

        except subprocess.CalledProcessError:
            log.error("❌ Ошибка выполнения бенчмарков")
            return False

    def process_size(self, size: str) -> dict:
        """Обработка одного размера: выполняет все шаги и возвращает словарь с результатами."""
        result = {"size": size, "timestamps": {}, "durations": {}, "status": "unknown"}

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

        result["status"] = "ok"
        result["total_time"] = time.time() - start
        return result

def main():
    if len(sys.argv) < 2:
        print("Использование: python dataset_manager.py [small / medium / large / x-large / xx-large] [--dry-run]")
        return

    target = sys.argv[1]
    dry = "--dry-run" in sys.argv

    manager = DatasetManager(dry_run=dry)

    if not manager.initialize_databases():
        log.error("Инициализация схем не удалась. Выход.")
        return

    sizes = ["small", "medium", "large", "x-large", "xx-large"] if target == "all" else [target]

    for size in sizes:
        log.info("=" * 60)
        log.info("🎯 ОБРАБОТКА ДАТАСЕТА: %s", size.upper())
        res = manager.process_size(size)
        out_file = manager.results_path / f"{size}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(res, f, ensure_ascii=False, indent=2)
        log.info("Сохранён результат: %s", out_file)
        if res["status"] != "ok":
            log.warning("Обработка %s завершилась с статусом: %s", size, res["status"])
        else:
            log.info("🎉 %s датасет полностью обработан! (время %.2fs)", size, res["total_time"])


if __name__ == "__main__":
    main()
