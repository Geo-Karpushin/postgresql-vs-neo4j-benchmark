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

from benchmark_queries import POSTGRES_QUERIES, NEO4J_QUERIES

BATCH_SIZE = 1000
ITER_PROGRESS_PRINT_EVERY = 1
MAX_BFS_NEIGHBORS_FETCH = 10000

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
    def print_efficiency_report(efficiency_results: Dict):
        """Выводит отчет по эффективности в консоль"""
        print("\n" + "="*80)
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
        print(f"{'Запрос':<25} {'Коэфф.':<10} {'Neo4j быстрее':<15} {'PG (мс)':<10} {'Neo4j (мс)':<12} {'Значимость':<12}")
        print("-"*80)
        
        for query, results in efficiency_results.items():
            if query.startswith("_"):
                continue
                
            coeff = results["efficiency_coefficient"]
            if coeff > 1:
                faster = f"в {coeff:.1f} раз"
                marker = "✅"
            else:
                faster = f"в {1/coeff:.1f} раз" if coeff > 0 else "N/A"
                marker = "⚠️"
            
            print(f"{marker} {query:<23} {coeff:<10.2f} {faster:<15} "
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
                            COUNT(*) as friend_count
                        FROM friendships 
                        GROUP BY user_id
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
                    CALL db.schema.visualization() YIELD nodes, relationships
                    RETURN 
                        SIZE(nodes) as total_nodes,
                        SIZE(relationships) as total_relationships
                """)
                row = result.single()
                if row:
                    metrics["total_nodes"] = row["total_nodes"]
                    metrics["total_relationships"] = row["total_relationships"]
                
                # Количество пользователей и связей
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
    def __init__(self, dataset="unknown", query_runs_config=None, docker_config="medium"):
        self.dataset = dataset
        self.docker_config = docker_config
        self.query_runs_config = query_runs_config or {}
        self.database_metrics = {}
        self.results = {
            "postgres": {},
            "neo4j": {},
            "efficiency": {},
            "metadata": {
                "dataset": dataset,
                "docker_config": docker_config,
                "query_runs_config": query_runs_config,
                "timestamp": time.time(),
                "database_metrics": {}
            }
        }
        self.efficiency_calculator = EfficiencyCalculator()
        self.metrics_collector = DatabaseMetricsCollector()

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
        
        # Вывод сводки метрик
        if "postgres" in self.database_metrics:
            pg_metrics = self.database_metrics["postgres"]
            log.info(f"📈 PostgreSQL: {pg_metrics.get('users_count', 0)} пользователей, "
                    f"{pg_metrics.get('friendships_count', 0)} связей, "
                    f"в среднем {pg_metrics.get('avg_friends_per_user', 0):.1f} друзей на пользователя")
        
        if "neo4j" in self.database_metrics:
            neo_metrics = self.database_metrics["neo4j"]
            log.info(f"📈 Neo4j: {neo_metrics.get('users_count', 0)} пользователей, "
                    f"{neo_metrics.get('friendships_count', 0)} связей")

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
        conn = self.connect_postgres()
        if conn is None:
            log.error("PG недоступен")
            return False

        for qn, qi in POSTGRES_QUERIES.items():
            iterations = self.query_runs_config.get(qn, 5)
            desc = qi.get("description", "")
            sql = qi["query"]
            params = self._build_pg_params(qn, userA, userB)

            tqdm_desc = f"PG {qn} ({iterations} runs)"
            pbar = tqdm(total=iterations, desc=tqdm_desc, ncols=100)

            times = []
            results_count = 0

            for i in range(iterations):
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
                        log.error("PG %s SQL error: %s", qn, e)
                        try: 
                            conn.rollback()
                        except: 
                            pass
                        pbar.update(1)
                        continue

                t1 = time.perf_counter()
                times.append(t1 - t0)
                pbar.update(1)

            pbar.close()
            self.results["postgres"][qn] = self._pack_result(desc, times, results_count, iterations)

        try: 
            conn.close()
        except: 
            pass
        return True

    def _build_pg_params(self, qn, A, B):
        if qn == "simple_friends":
            return [A, A]
        if qn == "friends_of_friends":
            return [A, A, A]
        if qn == "mutual_friends":
            return [A, A, B, B]
        if qn == "friend_recommendations":
            return [A, A, A]
        if qn == "shortest_path":
            return [A, B]
        return []

    def run_neo4j_benchmarks(self, userA, userB):
        driver = self.connect_neo4j()
        if driver is None:
            log.error("Neo4j недоступен")
            return False

        for qn, qi in NEO4J_QUERIES.items():
            iterations = self.query_runs_config.get(qn, 5)
            desc = qi.get("description", "")
            query = qi["query"]
            params = self._build_neo_params(qn, userA, userB)

            tqdm_desc = f"Neo4j {qn} ({iterations} runs)"
            pbar = tqdm(total=iterations, desc=tqdm_desc, ncols=100)

            times = []
            results_count = 0

            for i in range(iterations):
                try:
                    with driver.session() as session:
                        t0 = time.perf_counter()
                        result = session.run(query, params)
                        cnt = sum(1 for _ in result)
                        t1 = time.perf_counter()

                    if i == 0:
                        results_count = cnt

                    times.append(t1 - t0)
                except Exception as e:
                    log.error("Neo4j %s error: %s", qn, e)

                pbar.update(1)

            pbar.close()
            self.results["neo4j"][qn] = self._pack_result(desc, times, results_count, iterations)

        try: driver.close()
        except: pass
        return True

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
        """Расчет коэффициентов эффективности"""
        self.results["efficiency"] = self.efficiency_calculator.calculate_efficiency_coefficients(
            self.results["postgres"],
            self.results["neo4j"]
        )
        
        # Вывод отчета в консоль
        if self.results["efficiency"]:
            self.efficiency_calculator.print_efficiency_report(self.results["efficiency"])
        else:
            print("\n⚠️  Не удалось рассчитать коэффициенты эффективности (нет общих запросов)")

    def save_results(self, output_path):
        """Сохранение результатов в JSON файл"""
        # Преобразуем в Path если это строка
        output_path = Path(output_path)
        
        # Создаем директорию если ее нет
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Добавляем информацию о размере выборки в метаданные
        self._add_dataset_size_to_metadata()
        
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("setup_config", nargs="?", default="unknown")
    parser.add_argument("dataset", nargs="?", default="unknown")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--config", type=str, help="Path to query runs config JSON file")
    parser.add_argument("--output", type=str, help="Path to output file")
    args = parser.parse_args()

    log.info("🎯 Benchmark: PG vs Neo4j")
    log.info("Датасет: %s", args.dataset)
    log.info("Конфигурация докера: %s", args.setup_config)

    # Загружаем конфигурацию query_runs
    query_runs_config = {}
    if args.config and Path(args.config).exists():
        with open(args.config, 'r', encoding='utf-8') as f:
            query_runs_config = json.load(f)
        log.info("📋 Загружена конфигурация query_runs: %s", query_runs_config)

    runner = BenchmarkRunner(
        dataset=args.dataset,
        query_runs_config=query_runs_config,
        docker_config=args.setup_config
    )

    # Собираем метрики баз данных
    runner.collect_database_metrics()

    # Выбираем пользователей для тестирования
    conn = runner.connect_postgres()
    if conn:
        userA, userB = runner._pick_two_users_from_pg(conn, seed=args.seed)
        conn.close()
    else:
        userA, userB = 1, 2

    log.info(f"Пользователи для тестирования: A={userA}, B={userB}")

    # Запускаем бенчмарки
    runner.run_postgres_benchmarks(userA, userB)
    runner.run_neo4j_benchmarks(userA, userB)
    
    # Расчет и вывод коэффициентов эффективности
    runner.calculate_efficiency()
    
    # Определяем путь для сохранения
    if args.output:
        output_path = args.output
    else:
        # Если путь не указан, создаем автоматический
        results_dir = Path("results")
        results_dir.mkdir(exist_ok=True)
        
        # Используем информацию о размере выборки в имени файла
        dataset_size = runner.results["metadata"].get("dataset_size", {})
        users_count = dataset_size.get("users_count", 0)
        friendships_count = dataset_size.get("friendships_count", 0)
        
        timestamp = int(time.time())
        output_path = results_dir / f"benchmark_{args.setup_config}_{users_count}users_{friendships_count}edges_{timestamp}.json"
    
    # Сохраняем результаты
    runner.save_results(output_path)

    log.info("🏁 Готово")


if __name__ == "__main__":
    exit(main())