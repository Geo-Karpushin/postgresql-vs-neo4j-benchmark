#!/usr/bin/env python3
"""
Просмотр результатов бенчмарков — агрегирует N последних файлов.
Использование:
  python view_results.py        # последний файл (n=1)
  python view_results.py 3      # последние 3 файла
  python view_results.py 0      # все файлы
Агрегирование: times списки склеиваются (pool), затем считаются min/avg/max/std.
"""

import json
import sys
from pathlib import Path
import statistics

RESULTS_DIR = Path("results")


def safe_fmt(x):
    if x is None:
        return "—"
    return f"{x*1000:.2f}"


def aggregate_files(files):
    """
    Возвращает агрегированную структуру { 'postgres': {...}, 'neo4j': {...}, 'metadata': {...} }
    times списки объединяются по каждому запросу/движку.
    metadata: dataset — список уникальных датасетов (или 'mixed'), iterations — list/unique.
    """
    agg = {"postgres": {}, "neo4j": {}, "metadata": {"datasets": set(), "iterations": set(), "files": len(files)}}

    for f in files:
        try:
            d = json.load(open(f, "r", encoding="utf-8"))
        except Exception:
            continue

        meta = d.get("metadata") or d.get("meta") or d.get("metadata", {})
        ds = meta.get("dataset") if isinstance(meta, dict) else None
        iters = meta.get("iterations") if isinstance(meta, dict) else None
        if ds:
            agg["metadata"]["datasets"].add(str(ds))
        if iters:
            agg["metadata"]["iterations"].add(int(iters))

        # engines
        for engine in ("postgres", "neo4j"):
            src = d.get(engine, {})
            for qname, qv in src.items():
                target = agg[engine].setdefault(qname, {"description": qv.get("description"), "times": [], "results_counts": []})
                times = qv.get("times") or []
                # extend times if present
                target["times"].extend([t for t in times if t is not None])
                rc = qv.get("results_count")
                if rc is None:
                    rc = 0
                target["results_counts"].append(int(rc))

    # finalize metadata
    dslist = sorted(list(agg["metadata"]["datasets"]))
    agg["metadata"]["dataset"] = dslist[0] if len(dslist) == 1 else ("mixed" if dslist else "unknown")
    itlist = sorted(list(agg["metadata"]["iterations"]))
    agg["metadata"]["iterations"] = itlist[0] if len(itlist) == 1 else (min(itlist) if itlist else None)

    # compute aggregated metrics per query
    for engine in ("postgres", "neo4j"):
        for qname, info in agg[engine].items():
            times = info["times"]
            rc_list = info["results_counts"]
            if times:
                mn = min(times)
                mx = max(times)
                avg = statistics.mean(times)
                std = statistics.stdev(times) if len(times) > 1 else 0.0
            else:
                mn = mx = avg = std = None
            
            # ⚡ ИСПРАВЛЕНИЕ: берем ПЕРВЫЙ results_count, а не среднее
            # В бенчмарке results_count устанавливается только на первой итерации
            rc_final = rc_list[0] if rc_list else 0

            agg[engine][qname] = {
                "description": info.get("description"),
                "times": times,
                "min_time": mn,
                "max_time": mx,
                "avg_time": avg,
                "std_time": std,
                "results_count": rc_final  # ⬅️ ИСПРАВЛЕНО
            }

    return agg


def print_agg(agg):
    print("=" * 80)
    print(f"Файлов агрегировано: {agg['metadata'].get('files')}, dataset: {agg['metadata'].get('dataset')}, iterations: {agg['metadata'].get('iterations')}")
    print("=" * 80)
    print()

    # Postgres
    print("📈 POSTGRESQL")
    print("-" * 80)
    print(f"{'QUERY':<25} {'AVG(ms)':<12} {'MIN(ms)':<10} {'MAX(ms)':<10} {'COUNT':<8}")
    print("-" * 80)
    for q, m in agg["postgres"].items():
        print(f"{q:<25} {safe_fmt(m['avg_time']):<12} {safe_fmt(m['min_time']):<10} {safe_fmt(m['max_time']):<10} {m['results_count']:<8}")
    print()

    # Neo4j
    print("🕸️  NEO4J")
    print("-" * 80)
    print(f"{'QUERY':<25} {'AVG(ms)':<12} {'MIN(ms)':<10} {'MAX(ms)':<10} {'COUNT':<8}")
    print("-" * 80)
    for q, m in agg["neo4j"].items():
        print(f"{q:<25} {safe_fmt(m['avg_time']):<12} {safe_fmt(m['min_time']):<10} {safe_fmt(m['max_time']):<10} {m['results_count']:<8}")
    print()

    # Comparison
    print("⚡ СРАВНЕНИЕ (PG / NEO4J)")
    print("-" * 80)
    print(f"{'QUERY':<25} {'PG(ms)':<12} {'NEO(ms)':<12} {'RATIO':<10}")
    print("-" * 80)
    for q in agg["postgres"].keys():
        pg = agg["postgres"].get(q, {})
        neo = agg["neo4j"].get(q, {})
        pg_avg = pg.get("avg_time")
        neo_avg = neo.get("avg_time")
        if pg_avg is None or neo_avg is None or neo_avg == 0:
            ratio = "—"
        else:
            ratio = f"{(pg_avg*1000)/(neo_avg*1000):.2f}x"
        pg_ms = safe_fmt(pg_avg)
        neo_ms = safe_fmt(neo_avg)
        print(f"{q:<25} {pg_ms:<12} {neo_ms:<12} {ratio:<10}")
    print()


def main():
    n = 1
    if len(sys.argv) > 1:
        try:
            n = int(sys.argv[1])
        except Exception:
            print("Аргумент должен быть числом")
            return

    if not RESULTS_DIR.exists():
        print("Нет папки results/")
        return

    files = sorted(RESULTS_DIR.glob("benchmark_results_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        print("Нет файлов в results/")
        return

    if n == 0:
        chosen = files
    else:
        chosen = files[:n]

    agg = aggregate_files(chosen)
    print_agg(agg)


if __name__ == "__main__":
    main()
