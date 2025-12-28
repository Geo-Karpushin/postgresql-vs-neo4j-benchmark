#!/usr/bin/env python3
"""
Менеджер датасетов для корректного тестирования разных размеров.
Конфигурируемая версия с настройками для каждого размера датасета.
С автоматической остановкой при явном преимуществе Neo4j.
"""

import subprocess
import sys
import time
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
import statistics

DATA_DIR = Path("generated")
SCRIPTS_DIR = Path("scripts")
RESULTS_DIR = Path("results")
POSTGRES_CONTAINER = "database-benchmark-postgres-1"
NEO4J_CONTAINER = "database-benchmark-neo4j-1"
DOCKER_RETRIES = 4
DOCKER_BACKOFF = 2

CONFIGS = [
    "test",
    "poor",
    "medium",
    "rich"
]

# Упорядоченный список размеров датасетов от меньшего к большему
ORDERED_SIZES = [
    "super-tiny",
    "tiny", 
    "very-small",
    "small",
    "medium",
    "large",
    "x-large",
    "xx-large"
]

DATASETS_CONFIG = {
    "super-tiny": {
        "users": 5_000,
        "avg_friends": 25,
        "iterations": 5,
        "query_runs": {
            "simple_friends": 150,
            "friends_of_friends": 300,
            "mutual_friends": 150,
            "friend_recommendations": 50,
            "shortest_path": 10,
            "cohort_analysis": 10,
            "social_cities": 8,
            "age_gap_analysis": 8,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "tiny": {
        "users": 10_000,
        "avg_friends": 22,
        "iterations": 5,
        "query_runs": {
            "simple_friends": 120,
            "friends_of_friends": 250,
            "mutual_friends": 120,
            "friend_recommendations": 40,
            "shortest_path": 8,
            "cohort_analysis": 8,
            "social_cities": 6,
            "age_gap_analysis": 6,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "very-small": {
        "users": 20_000,
        "avg_friends": 20,
        "iterations": 5,
        "query_runs": {
            "simple_friends": 100,
            "friends_of_friends": 200,
            "mutual_friends": 100,
            "friend_recommendations": 30,
            "shortest_path": 6,
            "cohort_analysis": 6,
            "social_cities": 5,
            "age_gap_analysis": 5,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "small": {
        "users": 50_000,
        "avg_friends": 20,
        "iterations": 5,
        "query_runs": {
            "simple_friends": 50,
            "friends_of_friends": 400,
            "mutual_friends": 50,
            "friend_recommendations": 20,
            "shortest_path": 5,
            "cohort_analysis": 5,
            "social_cities": 4,
            "age_gap_analysis": 4,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "medium": {
        "users": 500_000,
        "avg_friends": 18,
        "iterations": 3,
        "query_runs": {
            "simple_friends": 40,
            "friends_of_friends": 100,
            "mutual_friends": 40,
            "friend_recommendations": 20,
            "shortest_path": 5,
            "cohort_analysis": 4,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "large": {
        "users": 2_000_000,
        "avg_friends": 15,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 30,
            "friends_of_friends": 80,
            "mutual_friends": 30,
            "friend_recommendations": 15,
            "shortest_path": 3,
            "cohort_analysis": 3,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "x-large": {
        "users": 5_000_000,
        "avg_friends": 12,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 20,
            "friends_of_friends": 50,
            "mutual_friends": 20,
            "friend_recommendations": 10,
            "shortest_path": 3,
            "cohort_analysis": 3,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "xx-large": {
        "users": 10_000_000,
        "avg_friends": 10,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 10,
            "friends_of_friends": 30,
            "mutual_friends": 10,
            "friend_recommendations": 5,
            "shortest_path": 3,
            "cohort_analysis": 3,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 3,
            "age_clustering": 3
        }
    }
}

for config_name, config in DATASETS_CONFIG.items():
    users = config["users"]
    avg_friends = config["avg_friends"]
    friendships_count = int(users * avg_friends)
    config["expected_friendships"] = friendships_count
    
    if users <= 50_000:
        config["estimated_time_minutes"] = 5
    elif users <= 500_000:
        config["estimated_time_minutes"] = 15
    elif users <= 2_000_000:
        config["estimated_time_minutes"] = 30
    elif users <= 5_000_000:
        config["estimated_time_minutes"] = 60
    else:
        config["estimated_time_minutes"] = 120

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

class EfficiencyAnalyzer:
    """Анализатор эффективности для определения преимущества Neo4j"""
    
    @staticmethod
    def analyze_benchmark_result(result_file: Path) -> Optional[Dict[str, Any]]:
        """Анализирует результат бенчмарка и извлекает ключевые метрики"""
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            efficiency = data.get("efficiency", {})
            if not efficiency:
                return None
            
            summary = efficiency.get("_summary", {})
            if not summary:
                return None
            
            return {
                "average_efficiency": summary.get("average_efficiency", 1.0),
                "median_efficiency": summary.get("median_efficiency", 1.0),
                "neo4j_wins_count": summary.get("neo4j_wins_count", 0),
                "postgres_wins_count": summary.get("postgres_wins_count", 0),
                "total_comparisons": summary.get("total_comparisons", 0),
                "overall_winner": summary.get("overall_winner", "None"),
                "performance_advantage": summary.get("performance_advantage", "0%")
            }
        except Exception as e:
            log.error(f"Ошибка анализа файла {result_file}: {e}")
            return None
    
    @staticmethod
    def is_neo4j_clearly_faster(efficiency_data: Dict[str, Any]) -> bool:
        """
        Определяет, имеет ли Neo4j явное преимущество.
        Условия:
        1. Neo4j выиграл больше запросов чем PostgreSQL
        2. Средний коэффициент эффективности > 1.5
        3. Медианный коэффициент эффективности > 1.2
        """
        if not efficiency_data:
            return False
        
        neo_wins = efficiency_data.get("neo4j_wins_count", 0)
        pg_wins = efficiency_data.get("postgres_wins_count", 0)
        avg_eff = efficiency_data.get("average_efficiency", 1.0)
        median_eff = efficiency_data.get("median_efficiency", 1.0)
        
        # Neo4j выиграл больше запросов
        has_more_wins = neo_wins > pg_wins
        
        # Значительное преимущество по среднему коэффициенту
        has_high_avg_efficiency = avg_eff > 1.5
        
        # Стабильное преимущество по медианному коэффициенту
        has_high_median_efficiency = median_eff > 1.2
        
        return has_more_wins and has_high_avg_efficiency and has_high_median_efficiency
    
    @staticmethod
    def calculate_size_efficiency(size_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Рассчитывает суммарную эффективность для размера датасета"""
        if not size_results:
            return {"average_efficiency": 1.0, "neo4j_clearly_faster": False}
        
        avg_efficiencies = [r.get("average_efficiency", 1.0) for r in size_results]
        neo_clearly_faster_flags = [EfficiencyAnalyzer.is_neo4j_clearly_faster(r) for r in size_results]
        
        return {
            "average_efficiency": statistics.mean(avg_efficiencies),
            "median_efficiency": statistics.median(avg_efficiencies),
            "neo4j_clearly_faster_percentage": (sum(neo_clearly_faster_flags) / len(neo_clearly_faster_flags)) * 100,
            "is_neo4j_consistently_faster": all(neo_clearly_faster_flags),
            "iterations": len(size_results)
        }

class DatasetManager:
    def __init__(self, dry_run: bool = False):
        self.base_path = DATA_DIR
        self.scripts_path = SCRIPTS_DIR
        self.results_path = RESULTS_DIR
        self.dry_run = dry_run
        self.results_path.mkdir(parents=True, exist_ok=True)
        self.config = DATASETS_CONFIG
        self.efficiency_analyzer = EfficiencyAnalyzer()
        self.sizes_history = []  # История обработки размеров
        
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
            subprocess.run([sys.executable, str(self.scripts_path / "init_database.py"), "init"], check=True)
            log.info("✅ Схемы баз данных инициализированы")
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка инициализации: %s", e.stderr.strip())
            return False
        
    def cleanup_databases(self, config) -> bool:
        log.info("🧹 Очистка баз данных...")
        try:
            subprocess.run(
                [sys.executable, str(self.scripts_path / "cleanup_databases.py"),
                "--config", str(config)],
                check=True
            )
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка очистки: %s", e)
            return False
    
    def inspect_databases(self) -> bool:
        log.info("📝 Проверка датасетов в базах данных...")
        try:
            subprocess.run(
                [sys.executable, str(self.scripts_path / "inspect_databases.py")],
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
        cmd_chmod_users = ["docker", "exec", POSTGRES_CONTAINER, "chmod", "644", "/tmp/users.csv"]
        cmd_chmod_friends = ["docker", "exec", POSTGRES_CONTAINER, "chmod", "644", "/tmp/friendships.csv"]

        neo4j_dir = f"/var/lib/neo4j/import/{size}"
        mkdir_neo = ["docker", "exec", NEO4J_CONTAINER, "mkdir", "-p", neo4j_dir]
        cp_neo_users = ["docker", "cp", users_host, f"{NEO4J_CONTAINER}:{neo4j_dir}/users.csv"]
        cp_neo_friends = ["docker", "cp", friends_host, f"{NEO4J_CONTAINER}:{neo4j_dir}/friendships.csv"]

        steps = [
            (mkdir_neo, "Создание папки Neo4j"),
            (cp_pg_users, "Копирование users -> Postgres"),
            (cmd_chmod_users, "Выдача прав users.csv"),
            (cp_pg_friends, "Копирование friendships -> Postgres"),
            (cmd_chmod_friends, "Выдача прав friendships.csv"),
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
                [
                    sys.executable,
                    str(loader),
                    size,
                ],
                check=True
            )
            log.info("✅ Загрузка в базы завершена успешно")
            return True

        except subprocess.CalledProcessError:
            log.error("❌ Ошибка загрузки данных")
            return False

    def finalize_initialize_databases(self) -> bool:
        log.info("🗃️ Инициализация схем баз данных...")
        try:
            subprocess.run([sys.executable, str(self.scripts_path / "init_database.py"), "finalize"], check=True)
            log.info("✅ Схемы баз данных инициализированы")
            return True
        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка инициализации: %s", e.stderr.strip())
            return False

    def run_benchmarks(self, setup_config:str, size: str, iteration: int) -> Optional[Path]:
        """Запускает бенчмарки и возвращает путь к файлу с результатами"""
        log.info("🚀 Запуск бенчмарков для %s (итерация %d)...", size, iteration)

        runner = self.scripts_path / "benchmark_runner.py"
        if not runner.exists():
            log.error("Скрипт бенчмарков не найден: %s", runner)
            return None

        try:
            config = self.config.get(size, {})
            
            # Создаем временный конфиг файл для query_runs
            config_file = self.results_path / f"benchmark_config_{setup_config}_{size}_{iteration}.json"

            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config.get("query_runs", {}), f, indent=2)
            
            # Генерируем уникальное имя файла результатов
            result_filename = f"benchmark_results_{size}_{iteration}_{int(time.time())}.json"
            result_file = self.results_path / result_filename
            
            # Запускаем бенчмарк с явным указанием пути для сохранения
            subprocess.run(
                [
                    sys.executable, str(runner), setup_config, size,
                    "--config", str(config_file),
                    "--output", str(result_file)
                ],
                check=True
            )
            
            # Удаляем временный конфиг файл
            config_file.unlink(missing_ok=True)
            
            if result_file.exists():
                log.info("✅ Бенчмарки для %s завершены, результаты в %s", size, result_file)
                return result_file
            else:
                log.error("❌ Файл результатов не создан: %s", result_file)
                return None

        except subprocess.CalledProcessError as e:
            log.error("❌ Ошибка выполнения бенчмарков: %s", e)
            return None

    def process_iteration(self, config: str, size: str, iteration: int) -> Dict[str, Any]:
        """Обрабатывает одну итерацию тестирования для заданного размера"""
        result = {
            "size": size,
            "iteration": iteration,
            "timestamps": {},
            "durations": {},
            "status": "unknown",
            "config": self.config.get(size, {}),
            "start_time": time.time()
        }

        # 1. Очистка баз данных
        t0 = time.time()
        ok = self.cleanup_databases(config)
        result["timestamps"]["cleanup_start"] = t0
        result["durations"]["cleanup"] = time.time() - t0
        if not ok:
            result["status"] = "cleanup_failed"
            result["end_time"] = time.time()
            return result
        
        # 2. Инициализация схем
        t1 = time.time()
        ok = self.initialize_databases()
        result["timestamps"]["initialize_start"] = t1
        result["durations"]["initialize"] = time.time() - t1
        if not ok:
            result["status"] = "initialize_failed"
            result["end_time"] = time.time()
            return result

        # 3. Генерация датасета
        t2 = time.time()
        ok = self.generate_dataset(size)
        result["timestamps"]["generate_start"] = t2
        result["durations"]["generate"] = time.time() - t2
        if not ok:
            result["status"] = "generate_failed"
            result["end_time"] = time.time()
            return result

        # 4. Копирование в контейнеры
        t3 = time.time()
        ok = self.copy_to_containers(size)
        result["timestamps"]["copy_start"] = t3
        result["durations"]["copy"] = time.time() - t3
        if not ok:
            result["status"] = "copy_failed"
            result["end_time"] = time.time()
            return result

        # 5. Загрузка в базы данных
        t4 = time.time()
        ok = self.load_to_databases(size)
        result["timestamps"]["load_start"] = t4
        result["durations"]["load"] = time.time() - t4
        if not ok:
            result["status"] = "load_failed"
            result["end_time"] = time.time()
            return result
        
        # 6. Финализация инициализации баз данных
        t5 = time.time()
        ok = self.finalize_initialize_databases()
        result["timestamps"]["finalize_initialize_start"] = t5
        result["durations"]["finalize_initialize"] = time.time() - t5
        if not ok:
            result["status"] = "finalize_initialize_failed"
            result["end_time"] = time.time()
            return result

        # 6. Проверка данных
        ok = self.inspect_databases()
        if not ok:
            result["status"] = "inspection_failed"
            result["end_time"] = time.time()
            return result

        # 7. Запуск бенчмарков
        t6 = time.time()
        result_file = self.run_benchmarks(config, size, iteration)
        result["timestamps"]["benchmark_start"] = t6
        result["durations"]["benchmark"] = time.time() - t6
        if result_file is None:
            result["status"] = "bench_failed"
            result["end_time"] = time.time()
            return result
        
        result["benchmark_result_file"] = str(result_file)
        
        # 8. Анализ результатов
        efficiency_data = self.efficiency_analyzer.analyze_benchmark_result(result_file)
        if efficiency_data:
            result["efficiency_analysis"] = efficiency_data
            result["neo4j_clearly_faster"] = self.efficiency_analyzer.is_neo4j_clearly_faster(efficiency_data)

        result["status"] = "ok"
        result["end_time"] = time.time()
        result["total_time"] = result["end_time"] - result["start_time"]
        return result

    def should_stop_testing(self) -> bool:
        """
        Проверяет, нужно ли остановить тестирование.
        Останавливаем, если Neo4j показал явное преимущество
        на двух последовательных размерах датасета.
        """
        if len(self.sizes_history) < 2:
            return False
        
        # Берем последние два размера
        recent_sizes = self.sizes_history[-2:]
        
        # Проверяем, были ли оба размера успешно обработаны
        for size_info in recent_sizes:
            if not size_info.get("completed", False):
                return False
        
        # Проверяем, показал ли Neo4j явное преимущество на обоих размерах
        neo4j_faster_count = 0
        for size_info in recent_sizes:
            if size_info.get("neo4j_consistently_faster", False):
                neo4j_faster_count += 1
        
        if neo4j_faster_count >= 2:
            log.info("🚨 Neo4j показал явное преимущество на двух последовательных размерах датасета!")
            log.info("   Размеры: %s и %s", 
                    recent_sizes[0]["size"], recent_sizes[1]["size"])
            return True
        
        return False

def main():
    if len(sys.argv) < 2:
        print("Использование: python dataset_manager.py [size / all] [--dry-run]")
        print("Примеры:")
        print("  python dataset_manager.py small")
        print("  python dataset_manager.py all")
        print("  python dataset_manager.py all --dry-run")
        return

    target = sys.argv[1]
    dry = "--dry-run" in sys.argv

    manager = DatasetManager(dry_run=dry)
    
    # Определяем какие размеры обрабатывать
    if target == "all":
        sizes_to_process = ORDERED_SIZES
        log.info("🎯 Запуск тестирования всех размеров датасетов")
        log.info("📊 Порядок обработки: %s", " → ".join(sizes_to_process))
    elif target in ORDERED_SIZES:
        sizes_to_process = ORDERED_SIZES[ORDERED_SIZES.index(target):]
        log.info("🎯 Запуск тестирования с размера: %s", target)
        log.info("📊 Будут обработаны: %s", " → ".join(sizes_to_process))
    else:
        log.error("❌ Неизвестный размер датасета: %s", target)
        log.error("   Доступные размеры: %s", ", ".join(ORDERED_SIZES))
        return

    # Создаем общий файл результатов
    overall_results_file = manager.results_path / f"overall_results_{int(time.time())}.json"
    overall_results = {
        "start_time": datetime.now().isoformat(),
        "sizes_processed": [],
        "stopped_early": False,
        "stop_reason": None
    }

    # Обрабатываем каждый размер
    for config in CONFIGS:
        for size_idx, size in enumerate(sizes_to_process):
            log.info("=" * 80)
            log.info("🎯 ОБРАБОТКА ДАТАСЕТА: %s (%d/%d)", 
                    size.upper(), size_idx + 1, len(sizes_to_process))
            log.info("📊 Конфигурация: %s пользователей, %s средних друзей, %s итераций", 
                    manager.config.get(size, {}).get("users", "N/A"),
                    manager.config.get(size, {}).get("avg_friends", "N/A"),
                    manager.config.get(size, {}).get("iterations", "N/A"))
            
            size_results = []
            efficiency_results = []
            
            # Запускаем итерации для текущего размера
            iterations = manager.config.get(size, {}).get("iterations", 1)
            for iteration in range(1, iterations + 1):
                log.info("🔄 ИТЕРАЦИЯ %i/%i для размера %s", iteration, iterations, size)
                
                # Обрабатываем итерацию
                iteration_result = manager.process_iteration(config, size, iteration)
                
                # Сохраняем результат итерации
                iteration_file = manager.results_path / f"{size}_iteration_{iteration}.json"
                with open(iteration_file, "w", encoding="utf-8") as f:
                    json.dump(iteration_result, f, ensure_ascii=False, indent=2)
                log.info("📝 Результат итерации сохранен: %s", iteration_file)
                
                if iteration_result["status"] != "ok":
                    log.warning("⚠️ Итерация %i завершилась с статусом: %s", 
                            iteration, iteration_result["status"])
                    continue
                
                # Анализируем эффективность
                if "efficiency_analysis" in iteration_result:
                    efficiency_results.append(iteration_result["efficiency_analysis"])
                    neo4j_faster = iteration_result.get("neo4j_clearly_faster", False)
                    
                    if neo4j_faster:
                        log.info("✅ Neo4j показал явное преимущество в итерации %d", iteration)
                    else:
                        log.info("📊 Результаты итерации %d: Neo4j %s", iteration,
                                "быстрее" if neo4j_faster else "не показал явного преимущества")
                
                size_results.append(iteration_result)
            
            # Анализируем общую эффективность для размера
            size_efficiency = manager.efficiency_analyzer.calculate_size_efficiency(efficiency_results)
            
            # Сохраняем информацию о размере в историю
            size_info = {
                "size": size,
                "completed": len(size_results) > 0,
                "successful_iterations": len([r for r in size_results if r["status"] == "ok"]),
                "total_iterations": iterations,
                "neo4j_consistently_faster": size_efficiency.get("is_neo4j_consistently_faster", False),
                "average_efficiency": size_efficiency.get("average_efficiency", 1.0),
                "neo4j_faster_percentage": size_efficiency.get("neo4j_clearly_faster_percentage", 0.0)
            }
            manager.sizes_history.append(size_info)
            
            # Сохраняем сводку по размеру
            size_summary = {
                "size": size,
                "config": manager.config.get(size, {}),
                "efficiency_summary": size_efficiency,
                "iterations_completed": len(size_results),
                "successful_iterations": len([r for r in size_results if r["status"] == "ok"]),
                "timestamp": datetime.now().isoformat()
            }
            
            summary_file = manager.results_path / f"{size}_summary.json"
            with open(summary_file, "w", encoding="utf-8") as f:
                json.dump(size_summary, f, ensure_ascii=False, indent=2)
            
            log.info("📊 СВОДКА ПО РАЗМЕРУ %s:", size.upper())
            log.info("   Средний коэффициент эффективности: %.2fx", size_efficiency.get("average_efficiency", 1.0))
            log.info("   Neo4j показал преимущество в %.1f%% итераций", 
                    size_efficiency.get("neo4j_clearly_faster_percentage", 0.0))
            log.info("   Neo4j стабильно быстрее: %s", 
                    "ДА" if size_efficiency.get("is_neo4j_consistently_faster", False) else "НЕТ")
            
            overall_results["sizes_processed"].append(size_summary)
            
            # Проверяем, нужно ли остановить тестирование
            if manager.should_stop_testing():
                overall_results["stopped_early"] = True
                overall_results["stop_reason"] = "Neo4j показал явное преимущество на двух последовательных размерах"
                log.info("=" * 80)
                log.info("🚨 ТЕСТИРОВАНИЕ ОСТАНОВЛЕНО ПО УСЛОВИЮ")
                log.info("📊 Причина: %s", overall_results["stop_reason"])
                break
    
    # Сохраняем общие результаты
    overall_results["end_time"] = datetime.now().isoformat()
    overall_results["total_sizes_processed"] = len(overall_results["sizes_processed"])
    
    with open(overall_results_file, "w", encoding="utf-8") as f:
        json.dump(overall_results, f, ensure_ascii=False, indent=2)
    
    log.info("=" * 80)
    log.info("🏁 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    log.info("📊 Всего обработано размеров: %d", overall_results["total_sizes_processed"])
    log.info("📈 Тестирование остановлено досрочно: %s", 
            "ДА" if overall_results["stopped_early"] else "НЕТ")
    log.info("💾 Общие результаты сохранены: %s", overall_results_file)
    
    # Выводим финальную сводку
    log.info("\n📋 ФИНАЛЬНАЯ СВОДКА:")
    for i, size_info in enumerate(overall_results["sizes_processed"]):
        efficiency = size_info.get("efficiency_summary", {})
        log.info("  %d. %s: эффективность %.2fx, Neo4j стабильно быстрее: %s",
                i + 1,
                size_info["size"],
                efficiency.get("average_efficiency", 1.0),
                "✅" if efficiency.get("is_neo4j_consistently_faster", False) else "❌")


if __name__ == "__main__":
    main()