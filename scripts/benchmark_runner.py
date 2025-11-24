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

from benchmark_queries import POSTGRES_QUERIES, NEO4J_QUERIES

BATCH_SIZE = 1000
ITER_PROGRESS_PRINT_EVERY = 1
MAX_BFS_NEIGHBORS_FETCH = 10000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("bench")

class BenchmarkRunner:
    def __init__(self, iterations=5, dataset="unknown"):
        self.iterations = iterations
        self.dataset = dataset
        self.results = {
            "postgres": {},
            "neo4j": {},
            "metadata": {
                "dataset": dataset,
                "iterations": iterations,
                "timestamp": time.time()
            }
        }

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
            desc = qi.get("description", "")
            sql = qi["query"]
            params = self._build_pg_params(qn, userA, userB)

            tqdm_desc = f"PG {qn}"
            pbar = tqdm(total=self.iterations, desc=tqdm_desc, ncols=100)

            times = []
            results_count = 0

            for i in range(self.iterations):
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
            self.results["postgres"][qn] = self._pack_result(desc, times, results_count)

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
            desc = qi.get("description", "")
            query = qi["query"]
            params = self._build_neo_params(qn, userA, userB)

            tqdm_desc = f"Neo4j {qn}"
            pbar = tqdm(total=self.iterations, desc=tqdm_desc, ncols=100)

            times = []
            results_count = 0

            for i in range(self.iterations):
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
            self.results["neo4j"][qn] = self._pack_result(desc, times, results_count)

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

    def _pack_result(self, desc, times, count):
        if not times:
            return {
                "description": desc,
                "times": [],
                "min_time": None,
                "max_time": None,
                "avg_time": None,
                "std_time": None,
                "results_count": count
            }
        return {
            "description": desc,
            "times": times,
            "min_time": min(times),
            "max_time": max(times),
            "avg_time": statistics.mean(times),
            "std_time": statistics.stdev(times) if len(times) > 1 else 0.0,
            "results_count": count
        }

    def save_results(self):
        d = Path("results")
        d.mkdir(exist_ok=True)
        fname = d / f"benchmark_results_{self.dataset}_{int(time.time())}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
        log.info("💾 Результаты сохранены: %s", fname)
        return fname


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset", nargs="?", default="unknown")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--iterations", type=int, default=5)
    args = parser.parse_args()

    log.info("🎯 Benchmark: PG vs Neo4j")
    log.info("Датасет: %s", args.dataset)

    runner = BenchmarkRunner(
        iterations=args.iterations,
        dataset=args.dataset
    )

    conn = runner.connect_postgres()
    if conn:
        userA, userB = runner._pick_two_users_from_pg(conn, seed=args.seed)
        conn.close()
    else:
        userA, userB = 1, 2

    log.info(f"users: A={userA}, B={userB}")

    runner.run_postgres_benchmarks(userA, userB)
    runner.run_neo4j_benchmarks(userA, userB)
    runner.save_results()

    log.info("🏁 Готово")


if __name__ == "__main__":
    exit(main())
