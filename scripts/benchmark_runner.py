import time
import statistics
import json
import psycopg2
from neo4j import GraphDatabase
from pathlib import Path
import random
import math
import argparse
import logging
from collections import deque
from tqdm import tqdm
from typing import Dict, List, Tuple, Optional, Any

# Импортируем базовые запросы и добавляем аналитические
from benchmark_queries import (
    POSTGRES_QUERIES, NEO4J_QUERIES,
    POSTGRES_ANALYTICAL_QUERIES, NEO4J_ANALYTICAL_QUERIES
)

BATCH_SIZE = 1000
ITER_PROGRESS_PRINT_EVERY = 1
MAX_BFS_NEIGHBORS_FETCH = 10000
WARMUP_ITERATIONS = 2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("bench")


class EfficiencyCalculator:
    """Класс для расчета коэффициентов эффективности"""
    
    @staticmethod
    def calculate_efficiency_coefficients(pg_results: Dict, neo_results: Dict) -> Dict:
        """
        Рассчитывает коэффициенты эффективности Neo4j по сравнению с PostgreSQL
        
        Args:
            pg_results: результаты тестов PostgreSQL
            neo_results: результаты тестов Neo4j
            
        Returns:
            Словарь с коэффициентами эффективности
        """
        efficiency_results = {}
        
        # Находим общие запросы
        common_queries = set(pg_results.keys()) & set(neo_results.keys())
        
        for query in common_queries:
            pg_avg = pg_results[query].get("avg_time")
            neo_avg = neo_results[query].get("avg_time")
            
            if pg_avg and neo_avg and pg_avg > 0 and neo_avg > 0:
                # Коэффициент эффективности: во сколько раз Neo4j быстрее
                efficiency = pg_avg / neo_avg
                
                # Процентное улучшение
                improvement_pct = ((pg_avg - neo_avg) / pg_avg) * 100
                
                # Статистическая значимость (простая проверка)
                pg_std = pg_results[query].get("std_time", 0)
                neo_std = neo_results[query].get("std_time", 0)
                significance = "высокая" if abs(pg_avg - neo_avg) > (pg_std + neo_std) else "средняя"
                
                efficiency_results[query] = {
                    "efficiency_coefficient": round(efficiency, 2),
                    "neo4j_faster_times": round(efficiency, 1),
                    "improvement_percentage": round(improvement_pct, 1),
                    "postgres_time_ms": round(pg_avg * 1000, 2),
                    "neo4j_time_ms": round(neo_avg * 1000, 2),
                    "significance": significance,
                    "result_count_pg": pg_results[query].get("results_count", 0),
                    "result_count_neo": neo_results[query].get("results_count", 0)
                }
        
        # Расчет общих коэффициентов
        if efficiency_results:
            avg_efficiency = statistics.mean([v["efficiency_coefficient"] for v in efficiency_results.values()])
            median_efficiency = statistics.median([v["efficiency_coefficient"] for v in efficiency_results.values()])
            max_efficiency = max([v["efficiency_coefficient"] for v in efficiency_results.values()])
            min_efficiency = min([v["efficiency_coefficient"] for v in efficiency_results.values()])
            
            # Подсчет запросов, где Neo4j быстрее
            neo_wins = sum(1 for v in efficiency_results.values() if v["efficiency_coefficient"] > 1)
            pg_wins = sum(1 for v in efficiency_results.values() if v["efficiency_coefficient"] < 1)
            
            efficiency_results["_summary"] = {
                "average_efficiency": round(avg_efficiency, 2),
                "median_efficiency": round(median_efficiency, 2),
                "max_efficiency": round(max_efficiency, 2),
                "min_efficiency": round(min_efficiency, 2),
                "neo4j_wins_count": neo_wins,
                "postgres_wins_count": pg_wins,
                "total_comparisons": len(efficiency_results),
                "overall_winner": "Neo4j" if avg_efficiency > 1 else "PostgreSQL",
                "performance_advantage": f"{abs(avg_efficiency - 1) * 100:.1f}%"
            }
        
        return efficiency_results
    
    @staticmethod
    def print_efficiency_report(efficiency_results: Dict, title: str = ""):
        """Выводит отчет по эффективности в консоль"""
        print("\n" + "="*80)
        if title:
            print(f"ОТЧЕТ ЭФФЕКТИВНОСТИ NEO4J ПО СРАВНЕНИЮ С POSTGRESQL - {title}")
        else:
            print("ОТЧЕТ ЭФФЕКТИВНОСТИ NEO4J ПО СРАВНЕНИЮ С POSTGRESQL")
        print("="*80)
        
        if "_summary" in efficiency_results:
            summary = efficiency_results["_summary"]
            print(f"\n📊 ОБЩИЙ РЕЗУЛЬТАТ:")
            print(f"   Средний коэффициент эффективности: {summary['average_efficiency']:.2f}x")
            print(f"   Neo4j быстрее в {summary['neo4j_wins_count']} из {summary['total_comparisons']} запросов")
            print(f"   PostgreSQL быстрее в {summary['postgres_wins_count']} из {summary['total_comparisons']} запросов")
            print(f"   Общий победитель: {summary['overall_winner']}")
            print(f"   Преимущество производительности: {summary['performance_advantage']}")
            print("-"*80)
        
        print("\n📈 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ ПО ЗАПРОСАМ:")
        print(f"{'Запрос':<30} {'Коэфф.':<10} {'Neo4j быстрее':<15} {'PG (мс)':<10} {'Neo4j (мс)':<12} {'Значимость':<12}")
        print("-"*80)
        
        for query, results in efficiency_results.items():
            if query.startswith("_"):
                continue
                
            coeff = results["efficiency_coefficient"]
            if coeff > 1:
                faster = f"в {coeff:.1f} раз"
                marker = "✅"
            elif coeff == 0:
                faster = "N/A"
                marker = "❌"
            else:
                faster = f"в {1/coeff:.1f} раз" if coeff > 0 else "N/A"
                marker = "⚠️"
            
            print(f"{marker} {query:<28} {coeff:<10.2f} {faster:<15} "
                  f"{results['postgres_time_ms']:<10.1f} {results['neo4j_time_ms']:<12.1f} "
                  f"{results['significance']:<12}")
        
        print("="*80)


class DatabaseMetricsCollector:
    """Класс для сбора метрик базы данных"""
    
    @staticmethod
    def collect_postgres_metrics(conn) -> Dict[str, Any]:
        """Сбор метрик PostgreSQL"""
        metrics = {}
        try:
            with conn.cursor() as cur:
                # Количество пользователей
                cur.execute("SELECT COUNT(*) FROM users")
                metrics["users_count"] = cur.fetchone()[0]
                
                # Количество связей
                cur.execute("SELECT COUNT(*) FROM friendships")
                metrics["friendships_count"] = cur.fetchone()[0]
                
                # Дополнительные метрики
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT user_id) as users_with_friends,
                        COUNT(DISTINCT friend_id) as unique_friends,
                        AVG(friend_count) as avg_friends_per_user
                    FROM (
                        SELECT 
                            user_id,
                            friend_id,
                            COUNT(*) as friend_count
                        FROM friendships 
                        GROUP BY user_id, friend_id
                    ) user_friend_counts
                """)
                row = cur.fetchone()
                if row:
                    metrics["users_with_friends"] = row[0]
                    metrics["unique_friends"] = row[1]
                    metrics["avg_friends_per_user"] = float(row[2]) if row[2] else 0.0
                
                # Распределение по возрастам
                cur.execute("""
                    SELECT 
                        MIN(age) as min_age,
                        MAX(age) as max_age,
                        AVG(age) as avg_age,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY age) as median_age
                    FROM users
                """)
                row = cur.fetchone()
                if row:
                    metrics["age_distribution"] = {
                        "min": row[0],
                        "max": row[1],
                        "avg": float(row[2]) if row[2] else 0.0,
                        "median": float(row[3]) if row[3] else 0.0
                    }
                
            log.info(f"📊 PostgreSQL метрики: {metrics['users_count']} пользователей, {metrics['friendships_count']} связей")
        except Exception as e:
            log.warning(f"Не удалось собрать метрики PostgreSQL: {e}")
            metrics = {"users_count": 0, "friendships_count": 0}
        
        return metrics
    
    @staticmethod
    def collect_neo4j_metrics(driver) -> Dict[str, Any]:
        """Сбор метрик Neo4j"""
        metrics = {}
        try:
            with driver.session() as session:
                # Количество узлов и отношений
                result = session.run("""
                    MATCH (u:User)
                    WITH count(u) as user_count
                    MATCH ()-[r:FRIENDS_WITH]->()
                    RETURN user_count, count(r) as friendship_count
                """)
                row = result.single()
                if row:
                    metrics["users_count"] = row["user_count"]
                    metrics["friendships_count"] = row["friendship_count"]
                
                # Средняя степень связности
                result = session.run("""
                    MATCH (u:User)-[r:FRIENDS_WITH]-()
                    WITH u, count(r) as degree
                    RETURN 
                        count(u) as users_with_friends,
                        avg(degree) as avg_degree,
                        min(degree) as min_degree,
                        max(degree) as max_degree
                """)
                row = result.single()
                if row:
                    metrics["avg_friends_per_user"] = float(row["avg_degree"]) if row["avg_degree"] else 0.0
                    metrics["min_friends"] = row["min_degree"]
                    metrics["max_friends"] = row["max_degree"]
                
            log.info(f"📊 Neo4j метрики: {metrics.get('users_count', 0)} пользователей, {metrics.get('friendships_count', 0)} связей")
        except Exception as e:
            log.warning(f"Не удалось собрать метрики Neo4j: {e}")
            metrics = {"users_count": 0, "friendships_count": 0}
        
        return metrics


class BenchmarkRunner:
    def __init__(self, dataset="unknown", config=None, docker_config="medium"):
        self.dataset = dataset
        self.docker_config = docker_config
        self.config = config or {}
        
        # ВАЖНО: теперь config может содержать ТОЛЬКО query_runs
        # dataset-size будем получать из метрик базы данных
        
        # Извлекаем настройки из конфига
        self.dataset_size_config = {
            "users": 0,  # Будет заполнено из метрик
            "avg_friends": 0,  # Будет заполнено из метрик
            "iterations": 1  # По умолчанию
        }
        
        # Все запросы теперь в едином конфиге query_runs
        self.query_runs_config = config or {}  # Теперь config = query_runs
        
        # Для обратной совместимости: если query_runs пуст, используем значения по умолчанию
        if not self.query_runs_config:
            log.warning("Конфигурация query_runs пуста, используются значения по умолчанию")
            self.query_runs_config = self._get_default_query_config()
        
        log.info(f"Конфигурация запуска запросов: {self.query_runs_config}")
        
        self.database_metrics = {}
        self.results = {
            "postgres": {},
            "neo4j": {},
            "efficiency": {},
            "metadata": {
                "dataset": dataset,
                "docker_config": docker_config,
                "timestamp": time.time(),
                "database_metrics": {}
            }
        }
        self.efficiency_calculator = EfficiencyCalculator()
        self.metrics_collector = DatabaseMetricsCollector()

    def _get_default_query_config(self):
        """Возвращает конфигурацию запросов по умолчанию"""
        default_config = {
            "simple_friends": 5,
            "friends_of_friends": 5,
            "mutual_friends": 5,
            "friend_recommendations": 5,
            "shortest_path": 3
        }
        
        # Добавляем аналитические запросы, если они определены
        if POSTGRES_ANALYTICAL_QUERIES:
            for qn in POSTGRES_ANALYTICAL_QUERIES.keys():
                default_config[qn] = 1
        
        return default_config

    def connect_postgres(self, connect_timeout=5):
        try:
            return psycopg2.connect(
                host="localhost", port=5432, database="benchmark",
                user="postgres", password="password",
                connect_timeout=connect_timeout
            )
        except Exception as e:
            log.error("❌ PG connect: %s", e)
            return None

    def connect_neo4j(self):
        try:
            driver = GraphDatabase.driver(
                "bolt://localhost:7687",
                auth=("neo4j", "password")
            )
            return driver
        except Exception as e:
            log.error("❌ Neo4j connect: %s", e)
            return None

    def collect_database_metrics(self):
        """Сбор метрик баз данных"""
        log.info("📊 Сбор метрик баз данных...")
        
        # Сбор метрик PostgreSQL
        pg_conn = self.connect_postgres()
        if pg_conn:
            self.database_metrics["postgres"] = self.metrics_collector.collect_postgres_metrics(pg_conn)
            pg_conn.close()
        
        # Сбор метрик Neo4j
        neo_driver = self.connect_neo4j()
        if neo_driver:
            self.database_metrics["neo4j"] = self.metrics_collector.collect_neo4j_metrics(neo_driver)
            neo_driver.close()
        
        # Сохраняем метрики в результаты
        self.results["metadata"]["database_metrics"] = self.database_metrics
        
        # Обновляем dataset_size_config из метрик
        if "postgres" in self.database_metrics:
            pg_metrics = self.database_metrics["postgres"]
            self.dataset_size_config["users"] = pg_metrics.get("users_count", 0)
            self.dataset_size_config["avg_friends"] = pg_metrics.get("avg_friends_per_user", 0)
            log.info(f"📈 PostgreSQL: {pg_metrics.get('users_count', 0):,} пользователей, "
                    f"{pg_metrics.get('friendships_count', 0):,} связей, "
                    f"в среднем {pg_metrics.get('avg_friends_per_user', 0):.1f} друзей на пользователя")
        
        elif "neo4j" in self.database_metrics:
            neo_metrics = self.database_metrics["neo4j"]
            self.dataset_size_config["users"] = neo_metrics.get("users_count", 0)
            self.dataset_size_config["avg_friends"] = neo_metrics.get("avg_friends_per_user", 0)
            log.info(f"📈 Neo4j: {neo_metrics.get('users_count', 0):,} пользователей, "
                    f"{neo_metrics.get('friendships_count', 0):,} связей")

    def _count_candidates(self, conn, sql):
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM ({sql}) t")
                r = cur.fetchone()
                try: conn.rollback()
                except: pass
                return int(r[0]) if r else 0
        except:
            try: conn.rollback()
            except: pass
            return 0

    def _select_candidate_by_offset(self, conn, sql, offset):
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT node FROM ({sql}) t LIMIT 1 OFFSET {offset}")
                r = cur.fetchone()
                try: conn.rollback()
                except: pass
                return r[0] if r else None
        except:
            try: conn.rollback()
            except: pass
            return None

    def _pick_two_users_from_pg(self, conn, seed=None, attempts=30):
        if seed is not None:
            random.seed(seed)

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT user_id FROM users 
                    WHERE user_id IN (SELECT user_id FROM friendships)
                    ORDER BY random() 
                    LIMIT 1
                """)
                result = cur.fetchone()
                userA = result[0] if result else 1
                
                cur.execute("""
                    SELECT user_id FROM users 
                    WHERE user_id != %s 
                    AND user_id NOT IN (
                        SELECT friend_id FROM friendships WHERE user_id = %s
                        UNION 
                        SELECT user_id FROM friendships WHERE friend_id = %s
                    )
                    ORDER BY random() 
                    LIMIT 1
                """, (userA, userA, userA))
                
                result = cur.fetchone()
                userB = result[0] if result else (userA + 1 if userA > 1 else 2)
                
                return userA, userB
                
        except Exception as e:
            log.warning("Ошибка выбора пользователей: %s, используем 1,2", e)
            return 1, 2

    def run_postgres_benchmarks(self, userA, userB):
        """Запуск всех запросов PostgreSQL (базовых и аналитических)"""
        conn = self.connect_postgres()
        if conn is None:
            log.error("PG недоступен")
            return False

        # Объединяем базовые и аналитические запросы
        all_postgres_queries = {**POSTGRES_QUERIES, **POSTGRES_ANALYTICAL_QUERIES}
        
        log.info(f"Доступно запросов PostgreSQL: {len(all_postgres_queries)}")
        log.info(f"Конфигурация query_runs: {self.query_runs_config}")

        for qn in self.query_runs_config:
            if qn not in all_postgres_queries:
                log.warning(f"Запрос {qn} из конфигурации не найден в PostgreSQL")
                continue
                
            qi = all_postgres_queries[qn]
            iterations = self.query_runs_config.get(qn, 1)
            desc = qi.get("description", "")
            sql = qi["query"]
            
            # Для аналитических запросов параметры не нужны
            if qn in POSTGRES_ANALYTICAL_QUERIES:
                params = []
            else:
                params = self._build_pg_params(qn, userA, userB)
            
            # Проверяем соответствие параметров и плейсхолдеров
            placeholder_count = sql.count('%s')
            if len(params) != placeholder_count:
                log.warning(f"⚠️ Несоответствие параметров для {qn}: "
                        f"ожидается {placeholder_count} плейсхолдеров, "
                        f"передано {len(params)} параметров")
                # Для безопасности пропускаем запрос, если параметры не совпадают
                self.results["postgres"][qn] = self._pack_result(
                    desc, [], 0, iterations
                )
                continue

            tqdm_desc = f"PG {qn} ({iterations} runs)"
            pbar = tqdm(total=iterations, desc=tqdm_desc, ncols=100)

            times = []
            results_count = 0

            for i in range(iterations):
                try:
                    # Перед каждым запросом убедимся, что нет активной транзакции
                    try:
                        conn.rollback()
                    except:
                        pass
                    
                    t0 = time.perf_counter()

                    with conn.cursor() as cur:
                        try:
                            cur.execute(sql, params)
                            cnt = 0
                            while True:
                                batch = cur.fetchmany(BATCH_SIZE)
                                if not batch:
                                    break
                                cnt += len(batch)
                            
                            if i == 0:
                                results_count = cnt
                                
                        except Exception as e:
                            log.error(f"PG {qn} SQL error (итерация {i+1}): {e}")
                            # Пытаемся восстановить соединение
                            try: 
                                conn.rollback()
                            except Exception as rollback_err:
                                log.warning(f"Не удалось выполнить rollback: {rollback_err}")
                                # Если rollback не помогает, переподключаемся
                                try:
                                    conn.close()
                                except:
                                    pass
                                conn = self.connect_postgres()
                                if conn is None:
                                    log.error("Не удалось восстановить соединение с PostgreSQL")
                                    pbar.close()
                                    conn.close()
                                    return False
                            
                            pbar.update(1)
                            continue
                    
                    t1 = time.perf_counter()
                    times.append(t1 - t0)
                    
                    # Явный commit после успешного запроса (опционально)
                    try:
                        conn.commit()
                    except:
                        pass
                        
                except Exception as e:
                    log.error(f"PG {qn} общая ошибка (итерация {i+1}): {e}")
                    # Пытаемся восстановить соединение
                    try:
                        conn.close()
                    except:
                        pass
                    conn = self.connect_postgres()
                    if conn is None:
                        log.error("Не удалось восстановить соединение с PostgreSQL")
                        pbar.close()
                        return False
                
                pbar.update(1)

            pbar.close()
            self.results["postgres"][qn] = self._pack_result(desc, times, results_count, iterations)

        try: 
            conn.close()
        except: 
            pass
        
        log.info(f"Завершено запросов PostgreSQL: {list(self.results['postgres'].keys())}")
        return True

    def run_neo4j_benchmarks(self, userA, userB):
        driver = self.connect_neo4j()
        if driver is None:
            log.error("Neo4j недоступен")
            return False
        
        all_neo4j_queries = {**NEO4J_QUERIES, **NEO4J_ANALYTICAL_QUERIES}
        
        for qn in self.query_runs_config:
            if qn not in all_neo4j_queries:
                continue
                
            qi = all_neo4j_queries[qn]
            iterations = self.query_runs_config.get(qn, 1)
            query = qi["query"]
            
            if qn in NEO4J_ANALYTICAL_QUERIES:
                params = {}
            else:
                params = self._build_neo_params(qn, userA, userB)
            
            pbar = tqdm(total=iterations, desc=f"Neo4j {qn}", ncols=100)
            times = []
            results_count = 0
            
            # Создаем сессию один раз для всех итераций (если возможно)
            session = driver.session()
            
            for i in range(iterations):
                try:
                    t0 = time.perf_counter()
                    
                    # Используем consume() для быстрого получения всех результатов
                    # без обработки в Python
                    result = session.run(query, params)
                    result.consume()  # Получаем результаты, но не обрабатываем
                    
                    t1 = time.perf_counter()
                    
                    # Для подсчета строк выполняем отдельный запрос (только для первой итерации)
                    if i == 0:
                        count_result = session.run(query, params)
                        results_count = sum(1 for _ in count_result)
                    
                    if i >= WARMUP_ITERATIONS:
                        times.append(t1 - t0)
                    
                except Exception as e:
                    log.error("Neo4j %s error: %s", qn, e)
                
                pbar.update(1)
            
            session.close()
            pbar.close()
            
            self.results["neo4j"][qn] = self._pack_result(
                qi.get("description", ""), times, results_count, iterations
            )
        
        driver.close()
        return True

    def _build_pg_params(self, qn, A, B):
        if qn == "simple_friends":
            return [A, A, A]
        if qn == "friends_of_friends":
            return [A, A, A, A]
        if qn == "mutual_friends":
            return [A, A, A, B, B, B]
        if qn == "friend_recommendations":
            return [A, A, A]
        if qn == "shortest_path":
            return [A, A, B, B]
        return []

    def _build_neo_params(self, qn, A, B):
        if qn == "simple_friends":
            return {"user_id": A}
        if qn == "friends_of_friends":
            return {"user_id": A}
        if qn == "mutual_friends":
            return {"userA": A, "userB": B}
        if qn == "friend_recommendations":
            return {"user_id": A}
        if qn == "shortest_path":
            return {"userA": A, "userB": B}
        return {}

    def _pack_result(self, desc, times, count, iterations):
        if not times:
            return {
                "description": desc,
                "iterations": iterations,
                "times": [],
                "min_time": None,
                "max_time": None,
                "avg_time": None,
                "std_time": None,
                "results_count": count
            }
        return {
            "description": desc,
            "iterations": iterations,
            "times": times,
            "min_time": min(times),
            "max_time": max(times),
            "avg_time": statistics.mean(times),
            "std_time": statistics.stdev(times) if len(times) > 1 else 0.0,
            "results_count": count
        }

    def calculate_efficiency(self):
        """Расчет коэффициентов эффективности для всех запросов"""
        # Выводим отладочную информацию
        log.info(f"Результаты PostgreSQL: {list(self.results['postgres'].keys())}")
        log.info(f"Результаты Neo4j: {list(self.results['neo4j'].keys())}")
        
        # Находим общие запросы
        common_queries = set(self.results["postgres"].keys()) & set(self.results["neo4j"].keys())
        
        if not common_queries:
            log.error("❌ Нет общих запросов для сравнения!")
            log.error(f"PostgreSQL выполнил: {list(self.results['postgres'].keys())}")
            log.error(f"Neo4j выполнил: {list(self.results['neo4j'].keys())}")
            return
        
        self.results["efficiency"] = self.efficiency_calculator.calculate_efficiency_coefficients(
            self.results["postgres"],
            self.results["neo4j"]
        )
        
        # Вывод отчета в консоль
        if self.results["efficiency"]:
            self.efficiency_calculator.print_efficiency_report(self.results["efficiency"], "ВСЕ ЗАПРОСЫ")
        else:
            print("\n⚠️  Не удалось рассчитать коэффициенты эффективности (нет общих запросов)")

    def save_results(self, output_path):
        """Сохранение результатов в JSON файл"""
        # Преобразуем в Path если это строка
        output_path = Path(output_path)
        
        # Создаем директорию если ее нет
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Добавляем информацию о размере выборки в метаданные из собранных метрик
        self._add_dataset_size_to_metadata()
        
        # Добавляем конфигурацию в метаданные
        self.results["metadata"]["config"] = {
            "query_runs": self.query_runs_config,
            "dataset_size_config": self.dataset_size_config
        }
        
        # Сохраняем JSON
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        log.info("💾 Результаты сохранены: %s", output_path)
        
        return output_path
    
    def _add_dataset_size_to_metadata(self):
        """Добавляет информацию о размере выборки в метаданные"""
        if "postgres" in self.database_metrics:
            pg_metrics = self.database_metrics["postgres"]
            self.results["metadata"]["dataset_size"] = {
                "users_count": pg_metrics.get("users_count", 0),
                "friendships_count": pg_metrics.get("friendships_count", 0),
                "avg_friends_per_user": pg_metrics.get("avg_friends_per_user", 0),
                "source": "postgres"
            }
        elif "neo4j" in self.database_metrics:
            neo_metrics = self.database_metrics["neo4j"]
            self.results["metadata"]["dataset_size"] = {
                "users_count": neo_metrics.get("users_count", 0),
                "friendships_count": neo_metrics.get("friendships_count", 0),
                "avg_friends_per_user": neo_metrics.get("avg_friends_per_user", 0),
                "source": "neo4j"
            }
        else:
            self.results["metadata"]["dataset_size"] = {
                "users_count": 0,
                "friendships_count": 0,
                "avg_friends_per_user": 0,
                "source": "unknown"
            }

    def print_summary_report(self):
        """Вывод сводного отчета по всем тестам"""
        print("\n" + "="*80)
        print("📊 СВОДНЫЙ ОТЧЕТ ПО РЕЗУЛЬТАТАМ ТЕСТИРОВАНИЯ")
        print("="*80)
        
        # Информация о наборе данных
        dataset_size = self.results["metadata"].get("dataset_size", {})
        print(f"\n📈 РАЗМЕР НАБОРА ДАННЫХ (фактический):")
        print(f"   • Пользователей: {dataset_size.get('users_count', 0):,}")
        print(f"   • Связей: {dataset_size.get('friendships_count', 0):,}")
        print(f"   • Среднее количество друзей: {dataset_size.get('avg_friends_per_user', 0):.1f}")
        
        # Информация о количестве итераций
        print(f"\n⚙️  КОНФИГУРАЦИЯ ТЕСТИРОВАНИЯ:")
        print(f"   • Конфигурация запросов (query_runs):")
        for query, iterations in self.query_runs_config.items():
            print(f"      - {query}: {iterations} итераций")
        
        # Сводка по всем запросам
        if self.results["efficiency"] and "_summary" in self.results["efficiency"]:
            summary = self.results["efficiency"]["_summary"]
            print(f"\n🎯 ОБЩАЯ СВОДКА:")
            print(f"   • Средний коэффициент: {summary['average_efficiency']:.2f}x")
            print(f"   • Neo4j быстрее в: {summary['neo4j_wins_count']}/{summary['total_comparisons']} запросов")
            print(f"   • PostgreSQL быстрее в: {summary['postgres_wins_count']}/{summary['total_comparisons']} запросов")
            print(f"   • Общий победитель: {summary['overall_winner']}")
            print(f"   • Преимущество: {summary['performance_advantage']}")
        
        # Разделение на графовые и аналитические запросы
        graph_queries = set(POSTGRES_QUERIES.keys()) & set(NEO4J_QUERIES.keys())
        analytical_queries = set(POSTGRES_ANALYTICAL_QUERIES.keys()) & set(NEO4J_ANALYTICAL_QUERIES.keys())
        
        # Группировка результатов по типам запросов
        graph_results = {k: v for k, v in self.results["efficiency"].items() 
                        if k in graph_queries and not k.startswith("_")}
        analytical_results = {k: v for k, v in self.results["efficiency"].items() 
                            if k in analytical_queries and not k.startswith("_")}
        
        if graph_results:
            avg_graph = statistics.mean([r["efficiency_coefficient"] for r in graph_results.values()])
            print(f"\n🔗 ГРАФОВЫЕ ЗАПРОСЫ ({len(graph_results)}):")
            print(f"   • Средний коэффициент: {avg_graph:.2f}x")
            neo_wins = sum(1 for r in graph_results.values() if r["efficiency_coefficient"] > 1)
            print(f"   • Neo4j быстрее в: {neo_wins}/{len(graph_results)} запросов")
        
        if analytical_results:
            avg_analytical = statistics.mean([r["efficiency_coefficient"] for r in analytical_results.values()])
            print(f"\n📊 АНАЛИТИЧЕСКИЕ ЗАПРОСЫ ({len(analytical_results)}):")
            print(f"   • Средний коэффициент: {avg_analytical:.2f}x")
            neo_wins = sum(1 for r in analytical_results.values() if r["efficiency_coefficient"] > 1)
            print(f"   • Neo4j быстрее в: {neo_wins}/{len(analytical_results)} запросов")
        
        # Вывод самых быстрых/медленных запросов
        if self.results["efficiency"]:
            print(f"\n⚡ САМЫЕ БЫСТРЫЕ ЗАПРОСЫ NEO4J:")
            fast_queries = sorted(
                [(k, v) for k, v in self.results["efficiency"].items() if not k.startswith("_")],
                key=lambda x: x[1].get("efficiency_coefficient", 0),
                reverse=True
            )[:5]
            
            for i, (query, data) in enumerate(fast_queries, 1):
                coeff = data.get("efficiency_coefficient", 0)
                if coeff > 1:
                    print(f"   {i}. {query}: Neo4j быстрее в {coeff:.1f} раз")
                elif coeff > 0:
                    print(f"   {i}. {query}: PostgreSQL быстрее в {1/max(coeff, 0.01):.1f} раз")
                else:
                    print(f"   {i}. {query}: N/A")
        else:
            print(f"\n⚠️  Нет результатов для анализа")
        
        print("="*80)


def main():
    parser = argparse.ArgumentParser(description="Тестирование производительности PostgreSQL vs Neo4j")
    parser.add_argument("setup_config", nargs="?", default="unknown", help="Конфигурация окружения")
    parser.add_argument("dataset", nargs="?", default="unknown", help="Название датасета")
    parser.add_argument("--seed", type=int, default=None, help="Seed для случайных чисел")
    parser.add_argument("--config", type=str, required=True, help="Путь к JSON конфигурации тестов (содержит только query_runs)")
    parser.add_argument("--output", type=str, help="Путь для сохранения результатов")
    args = parser.parse_args()

    log.info("🎯 Benchmark: PostgreSQL vs Neo4j")
    log.info("Датасет: %s", args.dataset)
    log.info("Конфигурация докера: %s", args.setup_config)
    log.info("Конфигурационный файл: %s", args.config)

    # Загружаем конфигурацию тестов (содержит только query_runs)
    config = {}
    if args.config and Path(args.config).exists():
        with open(args.config, 'r', encoding='utf-8') as f:
            config = json.load(f)
        log.info("📋 Загружена конфигурация запросов (query_runs)")
        log.info(f"Конфигурация запросов: {json.dumps(config, indent=2)}")
    else:
        log.error("❌ Конфигурационный файл не найден: %s", args.config)
        return 1

    # Теперь config = query_runs
    runner = BenchmarkRunner(
        dataset=args.dataset,
        config=config,  # Передаем только query_runs
        docker_config=args.setup_config
    )

    # Собираем метрики баз данных (здесь узнаем реальный размер данных)
    runner.collect_database_metrics()

    # Выбираем пользователей для тестирования (только для графовых запросов)
    conn = runner.connect_postgres()
    if conn:
        userA, userB = runner._pick_two_users_from_pg(conn, seed=args.seed)
        conn.close()
    else:
        userA, userB = 1, 2

    log.info(f"Пользователи для графовых запросов: A={userA}, B={userB}")

    # Запускаем все запросы PostgreSQL
    log.info("\n🚀 Запуск всех запросов PostgreSQL...")
    if not runner.run_postgres_benchmarks(userA, userB):
        log.error("❌ Не удалось запустить запросы PostgreSQL")
        return 1
    
    # Запускаем все запросы Neo4j
    log.info("\n🚀 Запуск всех запросов Neo4j...")
    if not runner.run_neo4j_benchmarks(userA, userB):
        log.error("❌ Не удалось запустить запросы Neo4j")
        return 1
    
    # Расчет и вывод коэффициентов эффективности
    runner.calculate_efficiency()
    
    # Вывод сводного отчета
    runner.print_summary_report()
    
    # Определяем путь для сохранения
    if args.output:
        output_path = args.output
    else:
        # Если путь не указан, создаем автоматический
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        # Используем информацию из метрик в имени файла
        dataset_size = runner.dataset_size_config
        users_count = dataset_size.get("users", 0)
        avg_friends = dataset_size.get("avg_friends", 0)
        
        timestamp = int(time.time())
        output_path = results_dir / f"benchmark_{args.setup_config}_{users_count}u_{avg_friends}af_{timestamp}.json"
    
    # Сохраняем результаты
    saved_path = runner.save_results(output_path)
    
    log.info("🏁 Готово! Результаты сохранены в: %s", saved_path)
    return 0


if __name__ == "__main__":
    exit(main())