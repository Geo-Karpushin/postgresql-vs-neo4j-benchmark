#!/usr/bin/env python3
"""
Умный менеджер тестирования с адаптивными стратегиями, анализом трендов
и ранней остановкой при стабилизации результатов.
Поддерживает тестирование с разными конфигурациями ресурсов (poor, medium, rich).
"""

import subprocess
import sys
import time
import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
import statistics
from scipy import stats
import numpy as np

BASE_DIR = Path(__file__).parent.parent.resolve()  # Корень проекта
DATA_DIR = BASE_DIR / "generated"
SCRIPTS_DIR = BASE_DIR / "scripts"
RESULTS_DIR = BASE_DIR / "results"
POSTGRES_CONTAINER = "database-benchmark-postgres-1"
NEO4J_CONTAINER = "database-benchmark-neo4j-1"
DOCKER_RETRIES = 4
DOCKER_BACKOFF = 2

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

# Конфигурации инфраструктуры (от бедной к богатой)
CONFIGS = ["poor", "medium", "rich"]

# Базовые конфигурации тестирования (будут адаптироваться)
DATASETS_CONFIG = {
    "super-tiny": {
        "users": 5_000,
        "avg_friends": 5,
        "iterations": 3,
        "query_runs": {
            "simple_friends": 50,
            "friends_of_friends": 100,
            "mutual_friends": 50,
            "friend_recommendations": 30,
            "shortest_path": 10,
            "cohort_analysis": 10,
            "social_cities": 10,
            "age_gap_analysis": 10,
            "network_growth": 3,
            "age_clustering": 3
        }
    },
    "tiny": {
        "users": 10_000,
        "avg_friends": 22,
        "iterations": 3,
        "query_runs": {
            "simple_friends": 40,
            "friends_of_friends": 80,
            "mutual_friends": 40,
            "friend_recommendations": 25,
            "shortest_path": 8,
            "cohort_analysis": 8,
            "social_cities": 8,
            "age_gap_analysis": 8,
            "network_growth": 2,
            "age_clustering": 2
        }
    },
    "very-small": {
        "users": 20_000,
        "avg_friends": 500,
        "iterations": 3,
        "query_runs": {
            "simple_friends": 30,
            "friends_of_friends": 60,
            "mutual_friends": 30,
            "friend_recommendations": 20,
            "shortest_path": 6,
            "cohort_analysis": 6,
            "social_cities": 6,
            "age_gap_analysis": 6,
            "network_growth": 4,
            "age_clustering": 4
        }
    },
    "small": {
        "users": 50_000,
        "avg_friends": 20,
        "iterations": 2,
        "query_runs": {
            "simple_friends": 25,
            "friends_of_friends": 50,
            "mutual_friends": 25,
            "friend_recommendations": 15,
            "shortest_path": 5,
            "cohort_analysis": 5,
            "social_cities": 5,
            "age_gap_analysis": 5,
            "network_growth": 2,
            "age_clustering": 2
        }
    },
    "medium": {
        "users": 100_000,
        "avg_friends": 50,
        "iterations": 2,
        "query_runs": {
            "simple_friends": 20,
            "friends_of_friends": 40,
            "mutual_friends": 20,
            "friend_recommendations": 12,
            "shortest_path": 4,
            "cohort_analysis": 4,
            "social_cities": 4,
            "age_gap_analysis": 4,
            "network_growth": 2,
            "age_clustering": 2
        }
    },
    "large": {
        "users": 250_000,
        "avg_friends": 15,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 15,
            "friends_of_friends": 30,
            "mutual_friends": 15,
            "friend_recommendations": 10,
            "shortest_path": 3,
            "cohort_analysis": 3,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 2,
            "age_clustering": 2
        }
    },
    "x-large": {
        "users": 500_000,
        "avg_friends": 12,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 10,
            "friends_of_friends": 20,
            "mutual_friends": 10,
            "friend_recommendations": 8,
            "shortest_path": 3,
            "cohort_analysis": 3,
            "social_cities": 3,
            "age_gap_analysis": 3,
            "network_growth": 2,
            "age_clustering": 2
        }
    },
    "xx-large": {
        "users": 1_000_000,
        "avg_friends": 100,
        "iterations": 1,
        "query_runs": {
            "simple_friends": 5,
            "friends_of_friends": 5,
            "mutual_friends": 5,
            "friend_recommendations": 5,
            "shortest_path": 5,
            "cohort_analysis": 5,
            "social_cities": 5,
            "age_gap_analysis": 5,
            "network_growth": 5,
            "age_clustering": 5
        }
    }
}

# Настройка логирования
def setup_logging(config_name: str = "all"):
    """Настраивает логирование с учетом конфигурации"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f"testing_{config_name}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger()

class TrendAnalyzer:
    """Анализатор трендов производительности"""
    
    @staticmethod
    def calculate_efficiency_coefficient(pg_time: float, neo_time: float) -> float:
        """Рассчитывает коэффициент эффективности (больше 1 = Neo4j быстрее)"""
        if pg_time <= 0 or neo_time <= 0:
            return 1.0
        return neo_time / pg_time
    
    @staticmethod
    def analyze_benchmark_result(result_data: Dict[str, Any]) -> Dict[str, Any]:
        """Анализирует результат бенчмарка и извлекает ключевые метрики"""
        efficiency = result_data.get("efficiency", {})
        if not efficiency:
            return {}
        
        summary = efficiency.get("_summary", {})
        if not summary:
            return {}
        
        # Анализируем отдельные тесты
        tests_analysis = {}
        for test_name, test_data in efficiency.items():
            if test_name.startswith("_"):
                continue
            
            tests_analysis[test_name] = {
                "efficiency_coefficient": test_data.get("efficiency_coefficient", 1.0),
                "improvement_percentage": test_data.get("improvement_percentage", 0),
                "postgres_time_ms": test_data.get("postgres_time_ms", 0),
                "neo4j_time_ms": test_data.get("neo4j_time_ms", 0),
                "significance": test_data.get("significance", "средняя")
            }
        
        return {
            "summary": {
                "average_efficiency": summary.get("average_efficiency", 1.0),
                "median_efficiency": summary.get("median_efficiency", 1.0),
                "neo4j_wins_count": summary.get("neo4j_wins_count", 0),
                "postgres_wins_count": summary.get("postgres_wins_count", 0),
                "total_comparisons": summary.get("total_comparisons", 0),
                "overall_winner": summary.get("overall_winner", "None"),
                "performance_advantage": summary.get("performance_advantage", "0%")
            },
            "tests": tests_analysis
        }
    
    @staticmethod
    def analyze_trends(efficiency_history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Анализирует тренды изменения эффективности по размерам датасетов"""
        if len(efficiency_history) < 2:
            return {"has_trend": False, "trend": "insufficient_data"}
        
        # Извлекаем средние эффективности
        avg_efficiencies = []
        median_efficiencies = []
        neo_wins_counts = []
        pg_wins_counts = []
        
        for hist in efficiency_history:
            if "summary" in hist:
                summary = hist["summary"]
                avg_efficiencies.append(summary.get("average_efficiency", 1.0))
                median_efficiencies.append(summary.get("median_efficiency", 1.0))
                neo_wins_counts.append(summary.get("neo4j_wins_count", 0))
                pg_wins_counts.append(summary.get("postgres_wins_count", 0))
        
        if len(avg_efficiencies) < 2:
            return {"has_trend": False, "trend": "insufficient_data"}
        
        try:
            # Анализ тренда с линейной регрессией
            x = list(range(len(avg_efficiencies)))
            y = avg_efficiencies
            
            # Проверяем, можем ли использовать scipy
            try:
                slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
                has_significant_trend = p_value < 0.1
                
                # Определяем тип тренда
                if abs(slope) < 0.05:
                    trend = "stable"
                elif slope > 0.1:
                    trend = "neo4j_improving"
                elif slope > 0:
                    trend = "neo4j_slightly_improving"
                elif slope < -0.1:
                    trend = "postgres_improving"
                else:
                    trend = "postgres_slightly_improving"
                
                # Анализируем победы
                neo_wins_trend = "increasing" if neo_wins_counts[-1] > neo_wins_counts[0] else "decreasing"
                pg_wins_trend = "increasing" if pg_wins_counts[-1] > pg_wins_counts[0] else "decreasing"
                
                # Анализ волатильности
                volatility = np.std(y) / np.mean(y) if len(y) > 1 else 0
                
                return {
                    "has_trend": has_significant_trend,
                    "trend": trend,
                    "slope": float(slope),
                    "r_squared": float(r_value**2),
                    "p_value": float(p_value),
                    "volatility": float(volatility),
                    "efficiency_range": (float(min(y)), float(max(y))),
                    "current_efficiency": float(y[-1]) if y else 1.0,
                    "neo_wins_trend": neo_wins_trend,
                    "pg_wins_trend": pg_wins_trend,
                    "data_points": len(y)
                }
                
            except ImportError:
                # Fallback без scipy
                # Простой анализ: увеличивается или уменьшается эффективность
                if len(y) >= 3:
                    first_half = statistics.mean(y[:len(y)//2])
                    second_half = statistics.mean(y[len(y)//2:])
                    slope_est = (second_half - first_half) / (len(y) // 2)
                    
                    if slope_est > 0.05:
                        trend = "neo4j_improving"
                    elif slope_est < -0.05:
                        trend = "postgres_improving"
                    else:
                        trend = "stable"
                    
                    return {
                        "has_trend": True,
                        "trend": trend,
                        "slope": float(slope_est),
                        "volatility": float(np.std(y) / np.mean(y) if np.mean(y) > 0 else 0),
                        "efficiency_range": (float(min(y)), float(max(y))),
                        "current_efficiency": float(y[-1]) if y else 1.0,
                        "data_points": len(y)
                    }
                else:
                    return {"has_trend": False, "trend": "insufficient_data"}
                
        except Exception as e:
            print(f"Ошибка анализа трендов: {e}")
            return {"has_trend": False, "trend": "analysis_error"}
    
    @staticmethod
    def should_stop_based_on_trend(trend_analysis: Dict[str, Any], current_size: str) -> Tuple[bool, str]:
        """Определяет, нужно ли остановить тестирование на основе тренда"""
        if not trend_analysis.get("has_trend", False):
            return False, "Нет значимого тренда"
        
        trend = trend_analysis.get("trend", "stable")
        current_eff = trend_analysis.get("current_efficiency", 1.0)
        slope = trend_analysis.get("slope", 0)
        volatility = trend_analysis.get("volatility", 0)
        
        # Настраиваем пороги в зависимости от размера
        if "large" in current_size or "x-large" in current_size:
            stop_threshold = 0.1  # Более чувствительный для больших датасетов
            confidence_threshold = 0.8  # Высокая уверенность
        else:
            stop_threshold = 0.15
            confidence_threshold = 0.7
        
        # Проверяем различные условия остановки
        
        # 1. Neo4j стабильно проигрывает и тренд ухудшается
        if current_eff < 0.5 and slope < -stop_threshold and volatility < 0.2:
            return True, f"Neo4j сильно проигрывает (эффективность: {current_eff:.2f}) и тренд ухудшается"
        
        # 2. PostgreSQL стабильно выигрывает и улучшается
        if current_eff > 2.0 and slope > stop_threshold and volatility < 0.2:
            return True, f"PostgreSQL сильно выигрывает (эффективность: {current_eff:.2f}) и улучшается"
        
        # 3. Результаты стабилизировались с большим отрывом
        if volatility < 0.1 and abs(slope) < stop_threshold:
            if current_eff < 0.7:
                return True, f"Neo4j стабильно проигрывает (эффективность: {current_eff:.2f}, волатильность: {volatility:.2f})"
            elif current_eff > 1.5:
                return True, f"PostgreSQL стабильно выигрывает (эффективность: {current_eff:.2f}, волатильность: {volatility:.2f})"
        
        # 4. Разрыв увеличивается экспоненциально
        if abs(slope) > 0.3 and volatility > 0.3:
            direction = "в пользу Neo4j" if slope > 0 else "в пользу PostgreSQL"
            return True, f"Разрыв экспоненциально увеличивается {direction} (наклон: {slope:.2f})"
        
        return False, "Продолжаем тестирование"

class AdaptiveQueryManager:
    """Адаптивно управляет количеством прогонов тестов"""
    
    def __init__(self, base_config: Dict[str, Dict[str, Any]]):
        self.base_config = base_config
        self.results_history: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.test_performance_history: Dict[str, List[float]] = {}
        
    def update_from_results(self, size: str, result_data: Dict[str, Any]):
        """Обновляет историю на основе результатов бенчмарка"""
        if "efficiency" not in result_data:
            return
        
        efficiency_data = result_data["efficiency"]
        self.results_history[size] = efficiency_data
        
        # Обновляем историю производительности для каждого теста
        for test_name, test_data in efficiency_data.items():
            if test_name.startswith("_"):
                continue
            
            eff_coeff = test_data.get("efficiency_coefficient", 1.0)
            if test_name not in self.test_performance_history:
                self.test_performance_history[test_name] = []
            
            self.test_performance_history[test_name].append(eff_coeff)
    
    def get_adaptive_config(self, size: str, previous_size: str = None) -> Dict[str, int]:
        """Возвращает адаптивную конфигурацию прогонов на основе истории"""
        base_runs = self.base_config.get(size, {}).get("query_runs", {})
        
        # Если нет истории или первый размер, используем базовую конфигурацию
        if not self.results_history or previous_size not in self.results_history:
            print(f"Использую базовую конфигурацию для размера {size}")
            return base_runs.copy()
        
        # Получаем результаты предыдущего размера
        prev_results = self.results_history.get(previous_size, {})
        
        adaptive_runs = {}
        
        for test_name, base_run_count in base_runs.items():
            test_data = prev_results.get(test_name, {})
            eff_coeff = test_data.get("efficiency_coefficient", 1.0)
            significance = test_data.get("significance", "средняя")
            improvement = test_data.get("improvement_percentage", 0)
            
            # Определяем стратегию для теста
            if test_name in self.test_performance_history:
                history = self.test_performance_history[test_name]
                if len(history) >= 2:
                    # Анализируем тренд теста
                    trend = "improving" if history[-1] > history[-2] else "worsening"
                    volatility = np.std(history[-min(3, len(history)):]) / np.mean(history[-min(3, len(history)):]) if len(history) >= 2 else 0
                else:
                    trend = "unknown"
                    volatility = 0
            else:
                trend = "unknown"
                volatility = 0
            
            # Применяем адаптивные правила
            
            # Правило 1: Очень плохая производительность Neo4j
            if eff_coeff < 0.3 and significance == "высокая":
                # Сокращаем прогоны в 6 раз
                new_runs = max(2, base_run_count // 6)
                reason = "Очень плохая производительность Neo4j"
            
            # Правило 2: Плохая производительность Neo4j
            elif eff_coeff < 0.6:
                # Сокращаем прогоны в 3 раза
                new_runs = max(3, base_run_count // 3)
                reason = "Плохая производительность Neo4j"
            
            # Правило 3: Отличная производительность Neo4j
            elif eff_coeff > 2.0 and significance == "высокая":
                # Увеличиваем прогоны в 2 раза для детального анализа
                new_runs = min(100, base_run_count * 2)
                reason = "Отличная производительность Neo4j"
            
            # Правило 4: Хорошая производительность Neo4j
            elif eff_coeff > 1.3:
                # Увеличиваем прогоны
                new_runs = min(80, int(base_run_count * 1.5))
                reason = "Хорошая производительность Neo4j"
            
            # Правило 5: Большой процент изменения (нестабильность)
            elif abs(improvement) > 300 and volatility > 0.4:
                # Увеличиваем прогоны для получения статистической значимости
                new_runs = min(60, int(base_run_count * 1.3))
                reason = "Высокая волатильность результатов"
            
            # Правило 6: Тест с растущим преимуществом Neo4j
            elif trend == "improving" and eff_coeff > 0.8:
                # Увеличиваем прогоны для подтверждения тренда
                new_runs = min(70, int(base_run_count * 1.4))
                reason = "Растущее преимущество Neo4j"
            
            # Правило 7: Стабильные результаты
            elif volatility < 0.2 and abs(improvement) < 100:
                # Слегка уменьшаем прогоны
                new_runs = max(5, int(base_run_count * 0.8))
                reason = "Стабильные результаты"
            
            else:
                # Оставляем как есть
                new_runs = base_run_count
                reason = "Стандартная конфигурация"
            
            adaptive_runs[test_name] = new_runs
            
            if new_runs != base_run_count:
                print(f"  Тест {test_name}: {base_run_count} → {new_runs} прогонов ({reason})")
        
        return adaptive_runs

class AdaptiveTestingManager:
    """Умный менеджер тестирования с адаптивными стратегиями"""
    
    def __init__(self, config_name: str = "all", dry_run: bool = False):
        self.config_name = config_name
        self.base_path = DATA_DIR
        self.scripts_path = SCRIPTS_DIR
        self.results_path = RESULTS_DIR / config_name
        self.dry_run = dry_run
        self.results_path.mkdir(parents=True, exist_ok=True)
        
        self.config = DATASETS_CONFIG
        self.trend_analyzer = TrendAnalyzer()
        self.query_manager = AdaptiveQueryManager(DATASETS_CONFIG)
        
        # История тестирования
        self.efficiency_history: List[Dict[str, Any]] = []
        self.size_results: Dict[str, List[Dict[str, Any]]] = {}
        self.testing_log: List[Dict[str, Any]] = []
        
        # Статистика
        self.stats = {
            "total_iterations": 0,
            "successful_iterations": 0,
            "failed_iterations": 0,
            "total_time": 0,
            "sizes_completed": [],
            "adaptations_applied": 0
        }
        
        # Настройка логирования
        self.log = setup_logging(config_name)
    
    def run_cmd(self, cmd: List[str], capture: bool = False, check: bool = True) -> subprocess.CompletedProcess:
        """Запуск команд"""
        if self.dry_run:
            self.log.info(f"DRY RUN: {' '.join(cmd)}")
            return subprocess.CompletedProcess(cmd, 0, "", "")
        return subprocess.run(cmd, text=True, capture_output=capture, check=check)
    
    def retry_cmd(self, cmd: List[str], retries: int = DOCKER_RETRIES, backoff: int = DOCKER_BACKOFF) -> bool:
        """Повторный запуск команды с backoff"""
        for attempt in range(retries):
            try:
                self.run_cmd(cmd)
                return True
            except subprocess.CalledProcessError:
                if attempt < retries - 1:
                    time.sleep(backoff * (2 ** attempt))
        return False
    
    def initialize_databases(self, infrastructure_config: str) -> bool:
        """Инициализация схем баз данных"""
        self.log.info(f"🗃️ Инициализация схем баз данных (конфигурация: {infrastructure_config})...")
        try:
            self.run_cmd([sys.executable, str(self.scripts_path / "init_database.py"), "init", infrastructure_config])
            self.log.info("✅ Схемы баз данных инициализированы")
            return True
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка инициализации: %s", e.stderr.strip())
            return False
    
    def cleanup_databases(self, infrastructure_config: str) -> bool:
        """Очистка баз данных"""
        self.log.info(f"🧹 Очистка баз данных (конфигурация: {infrastructure_config})...")
        try:
            self.run_cmd([
                sys.executable, str(self.scripts_path / "cleanup_databases.py"),
                "--config", infrastructure_config
            ])
            return True
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка очистки: %s", e)
            return False
    
    def generate_dataset(self, size: str) -> bool:
        """Генерация датасета"""
        self.log.info("🎯 Генерация датасета %s...", size)
        try:
            config = self.config.get(size, {})
            self.run_cmd([
                sys.executable, str(self.scripts_path / "data_generator.py"),
                str(config.get("users", 50000)),
                str(config.get("avg_friends", 15)),
                size
            ])
            self.log.info("✅ Датасет %s сгенерирован", size)
            return True
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка генерации: %s", e)
            return False
    
    def copy_to_containers(self, size: str) -> bool:
        """Копирование датасета в контейнеры"""
        self.log.info("📦 Копирование %s датасета в контейнеры...", size)
        
        # Проверка файлов
        users_file = self.base_path / size / "users.csv"
        friends_file = self.base_path / size / "friendships.csv"
        
        if not users_file.exists() or not friends_file.exists():
            self.log.error("❌ Файлы датасета не найдены")
            return False
        
        # Команды копирования
        commands = [
            (["docker", "cp", str(users_file), f"{POSTGRES_CONTAINER}:/tmp/users.csv"], 
             "Копирование users -> Postgres"),
            (["docker", "exec", POSTGRES_CONTAINER, "chmod", "644", "/tmp/users.csv"],
             "Права users.csv"),
            (["docker", "cp", str(friends_file), f"{POSTGRES_CONTAINER}:/tmp/friendships.csv"],
             "Копирование friendships -> Postgres"),
            (["docker", "exec", POSTGRES_CONTAINER, "chmod", "644", "/tmp/friendships.csv"],
             "Права friendships.csv"),
            (["docker", "exec", NEO4J_CONTAINER, "mkdir", "-p", f"/var/lib/neo4j/import/{size}"],
             "Создание папки Neo4j"),
            (["docker", "cp", str(users_file), f"{NEO4J_CONTAINER}:/var/lib/neo4j/import/{size}/users.csv"],
             "Копирование users -> Neo4j"),
            (["docker", "cp", str(friends_file), f"{NEO4J_CONTAINER}:/var/lib/neo4j/import/{size}/friendships.csv"],
             "Копирование friendships -> Neo4j")
        ]
        
        for cmd, desc in commands:
            if not self.retry_cmd(cmd):
                self.log.error("❌ Ошибка шага: %s", desc)
                return False
        
        self.log.info("✅ Датасет %s скопирован в контейнеры", size)
        return True
    
    def load_to_databases(self, size: str) -> bool:
        """Загрузка данных в базы"""
        self.log.info("📥 Загрузка %s датасета в базы...", size)
        
        loader = self.scripts_path / "load_data.py"
        if not loader.exists():
            self.log.error("❌ Скрипт загрузки не найден")
            return False
        
        try:
            self.run_cmd([sys.executable, str(loader), size])
            self.log.info("✅ Загрузка в базы завершена")
            return True
        except subprocess.CalledProcessError:
            self.log.error("❌ Ошибка загрузки данных")
            return False
    
    def finalize_initialize_databases(self, infrastructure_config: str) -> bool:
        """Финализация инициализации"""
        self.log.info(f"🔧 Финализация инициализации баз данных (конфигурация: {infrastructure_config})...")
        try:
            self.run_cmd([sys.executable, str(self.scripts_path / "init_database.py"), "finalize", infrastructure_config])
            self.log.info("✅ Финализация завершена")
            return True
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка финализации: %s", e.stderr.strip())
            return False
    
    def inspect_databases(self) -> bool:
        """Проверка данных в базах"""
        self.log.info("🔍 Проверка датасетов в базах данных...")
        try:
            self.run_cmd([sys.executable, str(self.scripts_path / "inspect_databases.py")])
            return True
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка проверки: %s", e)
            return False
    
    def run_benchmarks(self, infrastructure_config: str, size: str, iteration: int, 
                       adaptive_runs: Dict[str, int]) -> Optional[Path]:
        """Запуск бенчмарков с адаптивной конфигурацией"""
        self.log.info(f"🚀 Запуск бенчмарков для {size} (итерация {iteration}, конфигурация: {infrastructure_config})...")
        
        runner = self.scripts_path / "benchmark_runner.py"
        if not runner.exists():
            self.log.error("❌ Скрипт бенчмарков не найден")
            return None
        
        # Создаем конфиг файл с адаптивными прогонами
        config_file = self.results_path / f"config_{infrastructure_config}_{size}_{iteration}_{int(time.time())}.json"
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(adaptive_runs, f, indent=2)
        
        # Файл результатов
        result_file = self.results_path / f"results_{infrastructure_config}_{size}_{iteration}_{int(time.time())}.json"
        
        try:
            self.run_cmd([
                sys.executable, str(runner), infrastructure_config, size,
                "--config", str(config_file),
                "--output", str(result_file)
            ])
            
            # Удаляем временный конфиг
            config_file.unlink(missing_ok=True)
            
            if result_file.exists():
                self.log.info("✅ Бенчмарки завершены, результаты в %s", result_file)
                return result_file
            else:
                self.log.error("❌ Файл результатов не создан")
                return None
                
        except subprocess.CalledProcessError as e:
            self.log.error("❌ Ошибка выполнения бенчмарков: %s", e)
            return None
    
    def process_iteration(self, infrastructure_config: str, size: str, iteration: int, 
                         previous_size: str = None) -> Dict[str, Any]:
        """Обработка одной итерации тестирования"""
        start_time = time.time()
        result = {
            "infrastructure_config": infrastructure_config,
            "size": size,
            "iteration": iteration,
            "start_time": start_time,
            "status": "started",
            "adaptations": {},
            "errors": []
        }
        
        # Получаем адаптивную конфигурацию
        adaptive_runs = self.query_manager.get_adaptive_config(size, previous_size)
        result["adaptations"]["query_runs"] = adaptive_runs
        
        # # Шаг 1: Очистка
        # if not self.cleanup_databases(infrastructure_config):
        #     result["status"] = "cleanup_failed"
        #     result["errors"].append("Ошибка очистки баз данных")
        #     return result
        
        # # Шаг 2: Инициализация
        # if not self.initialize_databases(infrastructure_config):
        #     result["status"] = "init_failed"
        #     result["errors"].append("Ошибка инициализации схем")
        #     return result
        
        # # Шаг 3: Генерация
        # if not self.generate_dataset(size):
        #     result["status"] = "generate_failed"
        #     result["errors"].append("Ошибка генерации датасета")
        #     return result
        
        # # Шаг 4: Копирование
        # if not self.copy_to_containers(size):
        #     result["status"] = "copy_failed"
        #     result["errors"].append("Ошибка копирования в контейнеры")
        #     return result
        
        # # Шаг 5: Загрузка
        # if not self.load_to_databases(size):
        #     result["status"] = "load_failed"
        #     result["errors"].append("Ошибка загрузки в базы данных")
        #     return result
        
        # # Шаг 6: Финализация
        # if not self.finalize_initialize_databases(infrastructure_config):
        #     result["status"] = "finalize_failed"
        #     result["errors"].append("Ошибка финализации")
        #     return result
        
        # Шаг 7: Проверка
        if not self.inspect_databases():
            result["status"] = "inspect_failed"
            result["errors"].append("Ошибка проверки данных")
            return result
        
        # Шаг 8: Бенчмарки
        result_file = self.run_benchmarks(infrastructure_config, size, iteration, adaptive_runs)
        if not result_file:
            result["status"] = "benchmark_failed"
            result["errors"].append("Ошибка выполнения бенчмарков")
            return result
        
        # Чтение и анализ результатов
        try:
            with open(result_file, 'r', encoding='utf-8') as f:
                benchmark_data = json.load(f)
            
            efficiency_analysis = self.trend_analyzer.analyze_benchmark_result(benchmark_data)
            
            result.update({
                "status": "completed",
                "result_file": str(result_file),
                "efficiency_analysis": efficiency_analysis,
                "benchmark_data": benchmark_data,
                "end_time": time.time(),
                "duration": time.time() - start_time
            })
            
            # Обновляем историю
            self.query_manager.update_from_results(size, benchmark_data)
            if efficiency_analysis:
                self.efficiency_history.append(efficiency_analysis)
            
            self.stats["successful_iterations"] += 1
            
        except Exception as e:
            result["status"] = "analysis_failed"
            result["errors"].append(f"Ошибка анализа результатов: {e}")
        
        self.stats["total_iterations"] += 1
        return result
    
    def analyze_current_trend(self) -> Tuple[bool, str, Dict[str, Any]]:
        """Анализирует текущий тренд и принимает решение о продолжении"""
        if len(self.efficiency_history) < 2:
            return False, "Недостаточно данных для анализа", {}
        
        trend_analysis = self.trend_analyzer.analyze_trends(self.efficiency_history)
        
        # Получаем последний обработанный размер
        last_size = self.stats["sizes_completed"][-1] if self.stats["sizes_completed"] else "unknown"
        
        should_stop, reason = self.trend_analyzer.should_stop_based_on_trend(
            trend_analysis, last_size
        )
        
        return should_stop, reason, trend_analysis
    
    def run_adaptive_testing_for_config(self, infrastructure_config: str, target: str):
        """Запуск адаптивного тестирования для конкретной конфигурации"""
        self.log.info("=" * 80)
        self.log.info(f"🚀 ЗАПУСК АДАПТИВНОГО ТЕСТИРОВАНИЯ: {infrastructure_config.upper()}")
        self.log.info("=" * 80)
        
        # Определяем размеры для тестирования
        if target == "all":
            sizes_to_process = ORDERED_SIZES
        elif target in ORDERED_SIZES:
            start_idx = ORDERED_SIZES.index(target)
            sizes_to_process = ORDERED_SIZES[start_idx:]
        else:
            self.log.error("❌ Неизвестный целевой размер: %s", target)
            return
        
        self.log.info("📋 Размеры для тестирования: %s", " → ".join(sizes_to_process))
        self.log.info("⚙️  Конфигурация инфраструктуры: %s", infrastructure_config)
        
        previous_size = None
        stop_reason = None
        trend_history = []
        
        # Основной цикл тестирования
        for size_idx, size in enumerate(sizes_to_process):
            self.log.info("\n" + "=" * 80)
            self.log.info("🎯 РАЗМЕР %s (%d/%d)", size.upper(), size_idx + 1, len(sizes_to_process))
            self.log.info("=" * 80)
            
            # Проверяем, нужно ли остановиться
            if size_idx > 0:
                should_stop, reason, trend_analysis = self.analyze_current_trend()
                trend_history.append(trend_analysis)
                
                if should_stop:
                    stop_reason = reason
                    self.log.info("🛑 ПРИНЯТО РЕШЕНИЕ ОБ ОСТАНОВКЕ: %s", reason)
                    break
            
            size_config = self.config.get(size, {})
            iterations = size_config.get("iterations", 1)
            
            self.log.info("📊 Конфигурация: %d пользователей, %d средних друзей, %d итераций",
                    size_config.get("users", 0),
                    size_config.get("avg_friends", 0),
                    iterations)
            
            size_start_time = time.time()
            size_results = []
            
            # Запуск итераций для текущего размера
            for iteration in range(1, iterations + 1):
                self.log.info("-" * 60)
                self.log.info("🔄 ИТЕРАЦИЯ %d/%d для %s", iteration, iterations, size)
                
                result = self.process_iteration(infrastructure_config, size, iteration, previous_size)
                size_results.append(result)
                
                # Логирование результата итерации
                if result["status"] == "completed":
                    self.log.info("✅ Итерация %d завершена за %.2f сек", 
                            iteration, result.get("duration", 0))
                    
                    if "efficiency_analysis" in result:
                        eff = result["efficiency_analysis"].get("summary", {})
                        avg_eff = eff.get("average_efficiency", 1.0)
                        winner = eff.get("overall_winner", "Unknown")
                        self.log.info("📈 Эффективность: %.2fx, Победитель: %s", avg_eff, winner)
                else:
                    self.log.warning("⚠️ Итерация %d завершилась с ошибкой: %s", 
                               iteration, result.get("status", "unknown"))
                    self.log.warning("   Ошибки: %s", result.get("errors", []))
            
            # Сохранение результатов размера
            size_duration = time.time() - size_start_time
            self.save_size_results(infrastructure_config, size, size_results, size_duration)
            self.stats["sizes_completed"].append(size)
            
            previous_size = size
            
            # Вывод сводки по размеру
            self.print_size_summary(size, size_results, size_duration)
        
        # Финальная сводка
        self.print_final_summary(infrastructure_config, stop_reason, trend_history)
        
        # Сохранение полного отчета
        self.save_full_report(infrastructure_config, stop_reason)
    
    def save_size_results(self, infrastructure_config: str, size: str, results: List[Dict[str, Any]], duration: float):
        """Сохранение результатов тестирования размера"""
        summary = {
            "infrastructure_config": infrastructure_config,
            "size": size,
            "config": self.config.get(size, {}),
            "iterations": len(results),
            "successful_iterations": sum(1 for r in results if r["status"] == "completed"),
            "duration": duration,
            "results": results,
            "timestamp": datetime.now().isoformat()
        }
        
        summary_file = self.results_path / f"{infrastructure_config}_{size}_summary.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        self.log.info("💾 Результаты размера сохранены: %s", summary_file)
    
    def print_size_summary(self, size: str, results: List[Dict[str, Any]], duration: float):
        """Вывод сводки по размеру"""
        successful = sum(1 for r in results if r["status"] == "completed")
        total = len(results)
        
        if successful == 0:
            self.log.warning("❌ Размер %s: 0 успешных итераций из %d", size, total)
            return
        
        # Анализ эффективности
        efficiencies = []
        for result in results:
            if result["status"] == "completed" and "efficiency_analysis" in result:
                eff = result["efficiency_analysis"].get("summary", {}).get("average_efficiency", 1.0)
                efficiencies.append(eff)
        
        if efficiencies:
            avg_eff = statistics.mean(efficiencies)
            median_eff = statistics.median(efficiencies)
            min_eff = min(efficiencies)
            max_eff = max(efficiencies)
            
            self.log.info("📊 СВОДКА ПО РАЗМЕРУ %s:", size.upper())
            self.log.info("   Итераций: %d/%d успешно", successful, total)
            self.log.info("   Время: %.2f минут", duration / 60)
            self.log.info("   Эффективность Neo4j/PostgreSQL:")
            self.log.info("     • Средняя: %.2fx", avg_eff)
            self.log.info("     • Медианная: %.2fx", median_eff)
            self.log.info("     • Диапазон: %.2fx - %.2fx", min_eff, max_eff)
            
            if avg_eff > 1.0:
                self.log.info("     📈 Neo4j быстрее в среднем на %.1f%%", (avg_eff - 1) * 100)
            else:
                self.log.info("     📉 PostgreSQL быстрее в среднем на %.1f%%", (1 - avg_eff) * 100)
    
    def print_final_summary(self, infrastructure_config: str, stop_reason: Optional[str], trend_history: List[Dict[str, Any]]):
        """Вывод финальной сводки"""
        self.log.info("\n" + "=" * 80)
        self.log.info(f"🏁 ТЕСТИРОВАНИЕ ЗАВЕРШЕНО: {infrastructure_config.upper()}")
        self.log.info("=" * 80)
        
        self.log.info("📈 СТАТИСТИКА:")
        self.log.info("   Всего итераций: %d", self.stats["total_iterations"])
        self.log.info("   Успешных итераций: %d", self.stats["successful_iterations"])
        self.log.info("   Проваленных итераций: %d", self.stats["failed_iterations"])
        self.log.info("   Обработано размеров: %d", len(self.stats["sizes_completed"]))
        self.log.info("   Применено адаптаций: %d", self.stats.get("adaptations_applied", 0))
        
        if self.efficiency_history:
            # Анализ итоговой эффективности
            final_eff = self.efficiency_history[-1].get("summary", {}).get("average_efficiency", 1.0)
            overall_winner = self.efficiency_history[-1].get("summary", {}).get("overall_winner", "Unknown")
            
            self.log.info("📊 ИТОГОВАЯ ЭФФЕКТИВНОСТЬ:")
            self.log.info("   Коэффициент: %.2fx", final_eff)
            self.log.info("   Общий победитель: %s", overall_winner)
            
            if final_eff > 1.0:
                self.log.info("   🎉 Neo4j показал преимущество %.1f%%", (final_eff - 1) * 100)
            else:
                self.log.info("   ⚡ PostgreSQL показал преимущество %.1f%%", (1 - final_eff) * 100)
        
        if stop_reason:
            self.log.info("🛑 ПРИЧИНА ОСТАНОВКИ:")
            self.log.info("   %s", stop_reason)
        
        if trend_history:
            self.log.info("📈 АНАЛИЗ ТРЕНДОВ:")
            for i, trend in enumerate(trend_history):
                if trend.get("has_trend"):
                    self.log.info("   Размер %d: %s (наклон: %.3f)", 
                            i + 1, trend.get("trend", "unknown"), trend.get("slope", 0))
    
    def save_full_report(self, infrastructure_config: str, stop_reason: Optional[str]):
        """Сохранение полного отчета"""
        report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "infrastructure_config": infrastructure_config,
                "stop_reason": stop_reason,
                "total_duration": self.stats.get("total_time", 0)
            },
            "statistics": self.stats,
            "efficiency_history": self.efficiency_history,
            "testing_log": self.testing_log,
            "adaptations": self.query_manager.get_test_recommendations(),
            "config_used": self.config
        }
        
        report_file = self.results_path / f"{infrastructure_config}_full_report_{int(time.time())}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        self.log.info("💾 Полный отчет сохранен: %s", report_file)

def main():
    """Основная функция"""
    if len(sys.argv) < 2:
        print("Использование: python adaptive_testing.py [size / all] [--config poor|medium|rich|all] [--dry-run]")
        print("\nПримеры:")
        print("  python adaptive_testing.py small --config medium")
        print("  python adaptive_testing.py all --config rich")
        print("  python adaptive_testing.py all --config all    # Тестировать все конфигурации")
        print("  python adaptive_testing.py super-tiny --dry-run")
        print("\nДоступные размеры:", " → ".join(ORDERED_SIZES))
        print("Доступные конфигурации ресурсов:", ", ".join(CONFIGS + ["all"]))
        return
    
    target = sys.argv[1]
    
    # Парсинг аргументов
    config_arg = "all"  # По умолчанию тестируем все конфигурации
    dry_run = False
    
    i = 2
    while i < len(sys.argv):
        if sys.argv[i] == "--config" and i + 1 < len(sys.argv):
            config_arg = sys.argv[i + 1]
            i += 2
        elif sys.argv[i] == "--dry-run":
            dry_run = True
            i += 1
        else:
            i += 1
    
    # Определяем какие конфигурации тестировать
    if config_arg == "all":
        configs_to_test = CONFIGS
    elif config_arg in CONFIGS:
        configs_to_test = [config_arg]
    else:
        print(f"❌ Неизвестная конфигурация инфраструктуры: {config_arg}")
        print(f"   Доступные конфигурации: {', '.join(CONFIGS + ['all'])}")
        return
    
    print("=" * 80)
    print("🚀 ЗАПУСК МНОГОКОНФИГУРАЦИОННОГО ТЕСТИРОВАНИЯ")
    print("=" * 80)
    print(f"📋 Целевой размер датасета: {target}")
    print(f"⚙️  Конфигурации для тестирования: {', '.join(configs_to_test)}")
    print(f"👁️  Режим dry-run: {'Да' if dry_run else 'Нет'}")
    print("=" * 80)
    
    overall_start_time = time.time()
    all_results = {}
    
    # Запуск тестирования для каждой конфигурации
    for config_idx, config_name in enumerate(configs_to_test):
        config_start_time = time.time()
        
        print(f"\n\n📊 КОНФИГУРАЦИЯ {config_name.upper()} ({config_idx + 1}/{len(configs_to_test)})")
        print("-" * 60)
        
        # Создаем менеджер для этой конфигурации
        manager = AdaptiveTestingManager(config_name=config_name, dry_run=dry_run)
        
        try:
            # Запускаем тестирование для этой конфигурации
            manager.run_adaptive_testing_for_config(config_name, target)
            
            # Сохраняем результаты
            all_results[config_name] = {
                "stats": manager.stats,
                "efficiency_history": manager.efficiency_history,
                "sizes_completed": manager.stats["sizes_completed"]
            }
            
            config_duration = time.time() - config_start_time
            print(f"⏱️  Время выполнения конфигурации {config_name}: {config_duration:.2f} сек")
            
        except KeyboardInterrupt:
            print(f"⚠️ Тестирование конфигурации {config_name} прервано пользователем")
            break
        except Exception as e:
            print(f"❌ Критическая ошибка в конфигурации {config_name}: {e}")
            import traceback
            traceback.print_exc()
    
    # Общая сводка по всем конфигурациям
    overall_duration = time.time() - overall_start_time
    print("\n" + "=" * 80)
    print("🏁 МНОГОКОНФИГУРАЦИОННОЕ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 80)
    
    print("📊 ОБЩАЯ СВОДКА ПО КОНФИГУРАЦИЯМ:")
    for config_name, results in all_results.items():
        stats = results.get("stats", {})
        print(f"\n  📈 {config_name.upper()}:")
        print(f"     Обработано размеров: {len(stats.get('sizes_completed', []))}")
        print(f"     Успешных итераций: {stats.get('successful_iterations', 0)}")
        print(f"     Проваленных итераций: {stats.get('failed_iterations', 0)}")
        
        if results.get("efficiency_history"):
            last_eff = results["efficiency_history"][-1].get("summary", {}).get("average_efficiency", 1.0)
            winner = results["efficiency_history"][-1].get("summary", {}).get("overall_winner", "Unknown")
            print(f"     Итоговая эффективность: {last_eff:.2f}x")
            print(f"     Победитель: {winner}")
    
    print(f"\n⏱️  Общее время выполнения: {overall_duration:.2f} сек ({overall_duration/60:.2f} мин)")
    print("💾 Результаты сохранены в папках:")
    for config_name in configs_to_test:
        config_dir = RESULTS_DIR / config_name
        if config_dir.exists():
            print(f"   • {config_name}: {config_dir}")
    
    # Создаем сравнительный отчет
    create_comparative_report(all_results, configs_to_test, overall_duration)

def create_comparative_report(all_results: Dict[str, Any], configs_tested: List[str], total_duration: float):
    """Создает сравнительный отчет по всем конфигурациям"""
    comparative_data = {
        "metadata": {
            "timestamp": datetime.now().isoformat(),
            "configs_tested": configs_tested,
            "total_duration": total_duration
        },
        "config_comparison": {},
        "summary": {
            "best_config_per_size": {},
            "overall_best_config": None,
            "performance_differences": {}
        }
    }
    
    # Собираем данные для сравнения
    for config_name, results in all_results.items():
        stats = results.get("stats", {})
        efficiency_history = results.get("efficiency_history", [])
        
        comparative_data["config_comparison"][config_name] = {
            "sizes_completed": stats.get("sizes_completed", []),
            "successful_iterations": stats.get("successful_iterations", 0),
            "failed_iterations": stats.get("failed_iterations", 0),
            "final_efficiency": efficiency_history[-1].get("summary", {}).get("average_efficiency", 1.0) if efficiency_history else 1.0,
            "final_winner": efficiency_history[-1].get("summary", {}).get("overall_winner", "Unknown") if efficiency_history else "Unknown"
        }
    
    # Сохраняем сравнительный отчет
    comp_report_file = RESULTS_DIR / f"comparative_report_{int(time.time())}.json"
    with open(comp_report_file, 'w', encoding='utf-8') as f:
        json.dump(comparative_data, f, ensure_ascii=False, indent=2)
    
    print(f"\n📊 Сравнительный отчет сохранен: {comp_report_file}")
    
    # Простой вывод в консоль
    print("\n📈 СРАВНИТЕЛЬНЫЙ АНАЛИЗ:")
    print("-" * 60)
    
    for config_name, data in comparative_data["config_comparison"].items():
        print(f"{config_name.upper():10} | Эффективность: {data['final_efficiency']:6.2f}x | "
              f"Победитель: {data['final_winner']:15} | Размеров: {len(data['sizes_completed'])}")

if __name__ == "__main__":
    main()