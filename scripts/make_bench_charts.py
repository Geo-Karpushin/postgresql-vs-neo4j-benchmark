#!/usr/bin/env python3
import json
from pathlib import Path
from collections import defaultdict
import numpy as np
import matplotlib.pyplot as plt
from scipy.interpolate import PchipInterpolator

RESULTS_GLOB = "results/poor/results_*.json"
CHARTS_DIR = Path("charts")
CHARTS_DIR.mkdir(exist_ok=True)

DEFAULT_USER_COUNT = {
    "super-tiny": 5_000,
    "tiny": 10_000,
    "very-small": 20_000,
    "small": 50_000,
    "medium": 500_000,
    "large": 2_000_000,
    "x-large": 5_000_000,
}

QUERIES_ORDER = None
DBS = ["postgres", "neo4j"]

def format_users(x):
    if x is None:
        return "N/A"
    if x >= 1_000_000:
        return f"{x/1_000_000:.1f}M"
    if x >= 1000:
        return f"{x/1000:.0f}k"
    return str(x)

def load_benchmarks(folder):
    files = list(Path(folder).glob("results_*.json"))

    data = []
    user_counts = {}

    for f in files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                js = json.load(fp)

            dataset = js["metadata"]["dataset"]
            users = js["metadata"].get("users")

            data.append((dataset, js))

            if users is not None:
                user_counts[dataset] = users
            else:
                user_counts[dataset] = DEFAULT_USER_COUNT.get(dataset, 0)

        except Exception as e:
            print(f"Skipping broken file: {f} ({e})")

    return data, user_counts

def aggregate_by_dataset(data):
    """
    Возвращает:
      agg[dataset][query][db] = [avg_time, ...]
    """
    agg = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    for dataset, js in data:
        for db in DBS:
            db_obj = js.get(db, {})
            if not isinstance(db_obj, dict):
                continue
            for qname, qobj in db_obj.items():
                if isinstance(qobj, dict) and "avg_time" in qobj:
                    val = qobj.get("avg_time")
                    if val is not None:
                        agg[dataset][qname][db].append(float(val))
                elif isinstance(qobj, list):
                    vals = [float(x) for x in qobj if x is not None]
                    agg[dataset][qname][db].extend(vals)
    return agg

def compute_means(agg):
    mean_data = defaultdict(lambda: defaultdict(dict))
    for ds, queries in agg.items():
        for qname, dbs in queries.items():
            for db in DBS:
                vals = dbs.get(db, [])
                mean_data[ds][qname][db] = (sum(vals) / len(vals)) if vals else None
    return mean_data

def find_crossings_and_stats(points, dense=2000):
    """
    points: list of (users, pg_time, neo_time) sorted by users asc
    return: dict { crossings: [...], initial, final, max_rel_pct, winner_at_max, x_at_max }
    """
    out = {"crossings": [], "initial": None, "final": None, "max_rel_pct": None, "winner_at_max": None, "x_at_max": None}
    if len(points) < 2:
        return out

    xs = np.array([p[0] for p in points], dtype=float)
    pg = np.array([p[1] for p in points], dtype=float)
    neo = np.array([p[2] for p in points], dtype=float)

    mask = np.isfinite(xs) & np.isfinite(pg) & np.isfinite(neo)
    xs = xs[mask]; pg = pg[mask]; neo = neo[mask]
    if len(xs) < 2:
        return out

    try:
        pg_ip = PchipInterpolator(xs, pg)
        neo_ip = PchipInterpolator(xs, neo)
    except Exception:
        return out

    xs_dense = np.logspace(np.log10(xs.min()), np.log10(xs.max()), dense)
    pg_dense = pg_ip(xs_dense)
    neo_dense = neo_ip(xs_dense)

    diff = pg_dense - neo_dense
    s = np.sign(diff)
    changes = np.where(np.diff(s) != 0)[0]
    crossings = []
    for idx in changes:
        x0, x1 = xs_dense[idx], xs_dense[idx+1]
        d0, d1 = diff[idx], diff[idx+1]
        root = (x0 + x1) / 2.0 if (d1 - d0) == 0 else x0 - d0 * (x1 - x0) / (d1 - d0)
        crossings.append(root)
    nz = np.where(np.isclose(diff, 0.0, atol=1e-12))[0]
    for i in nz:
        crossings.append(xs_dense[i])
    crossings = sorted(set(crossings))

    out["crossings"] = crossings
    out["initial"] = "pg" if diff[0] < 0 else ("neo" if diff[0] > 0 else "equal")
    out["final"] = "pg" if diff[-1] < 0 else ("neo" if diff[-1] > 0 else "equal")

    rels = np.abs(pg_dense - neo_dense) / np.maximum(pg_dense, neo_dense) * 100.0
    if np.any(np.isfinite(rels)):
        mi = int(np.nanargmax(rels))
        out["max_rel_pct"] = float(rels[mi])
        out["winner_at_max"] = "pg" if pg_dense[mi] < neo_dense[mi] else "neo"
        out["x_at_max"] = float(xs_dense[mi])

    return out

def analyze_and_summarize(mean_data, user_counts):
    datasets_sorted = sorted(user_counts.keys(), key=lambda d: user_counts.get(d, 0))

    # Собираем названия всех запросов
    queries = set()
    for ds, qs in mean_data.items():
        queries.update(qs.keys())
    queries = sorted(queries)

    analysis = {}
    lines = []

    for q in queries:
        # Собираем точки (dataset_size, pg, neo)
        points = []
        for ds in datasets_sorted:
            pg = mean_data.get(ds, {}).get(q, {}).get("postgres")
            neo = mean_data.get(ds, {}).get(q, {}).get("neo4j")
            if pg is None or neo is None:
                continue
            points.append((user_counts.get(ds, 0), float(pg), float(neo)))

        if not points:
            lines.append(f"{q}: нет данных")
            analysis[q] = {}
            continue

        # Аналитика через интерполяцию
        stats = find_crossings_and_stats(points)
        crossings = stats["crossings"]
        critical = crossings[0] if crossings else None

        # Явные деградации в сырых точках (рост ×4)
        degrades = []
        for i in range(1, len(points)):
            prev = points[i - 1]
            cur = points[i]
            if prev[1] > 0 and cur[1] / prev[1] >= 4:
                degrades.append(("PG", cur[0]))
            if prev[2] > 0 and cur[2] / prev[2] >= 4:
                degrades.append(("NEO", cur[0]))

        # Победы по сырым точкам
        pg_wins = [u for (u, pg, neo) in points if pg < neo]
        neo_wins = [u for (u, pg, neo) in points if neo < pg]

        max_rel = stats.get("max_rel_pct")
        who_max = stats.get("winner_at_max")
        x_max = stats.get("x_at_max")

        analysis[q] = {
            "points": points,
            "pg_wins": pg_wins,
            "neo_wins": neo_wins,
            "critical": critical,
            "crossings": crossings,
            "max_rel_pct": max_rel,
            "winner_at_max": who_max,
            "x_at_max": x_max,
            "degradations": degrades,
            "initial": stats.get("initial"),
            "final": stats.get("final")
        }

        # Человеко-читаемые ярлыки
        init = {"pg": "PostgreSQL", "neo": "Neo4j", "equal": "равны"}.get(stats.get("initial"), "?")
        fin = {"pg": "PostgreSQL", "neo": "Neo4j", "equal": "равны"}.get(stats.get("final"), "?")

        crit_label = format_users(int(round(critical))) if critical else "нет"

        if max_rel is not None:
            max_label = (
                f"{max_rel:.1f}% — преимущество "
                f"{'PostgreSQL' if who_max=='pg' else 'Neo4j'} "
                f"на размере {format_users(int(round(x_max)))}"
            )
        else:
            max_label = "нет"

        if degrades:
            degr_label = ", ".join(
                f"{db} деградирует на {format_users(int(sz))}"
                for db, sz in degrades
            )
        else:
            degr_label = "нет резких деградаций"

        # Формируем итоговую строку
        line = (
            f"\n▶ {q}\n"
            f"   • Стартовое лидерство: {init}\n"
            f"   • Итоговое лидерство:  {fin}\n"
            f"   • Точка смены лидера:  {crit_label}\n"
            f"   • Максимальный разрыв: {max_label}\n"
            f"   • Деградации:          {degr_label}\n"
        )
        lines.append(line)

    # Глобальный порог
    criticals = []
    for q, v in analysis.items():
        c = v.get("critical")
        if c:
            criticals.append(c)

    global_threshold = int(np.median(criticals)) if criticals else None

    print("\n===============================")
    print("         АНАЛИЗ РЕЗУЛЬТАТОВ")
    print("===============================\n")

    for l in lines:
        print(l)

    print("\n===============================")
    if global_threshold:
        print(
            f"Рекомендация:\n"
            f"  • Neo4j предпочтителен для графов от {format_users(global_threshold)} пользователей и выше.\n"
            f"  • PostgreSQL имеет смысл только на очень маленьких графах (< {format_users(global_threshold)})."
        )
    else:
        print("Глобальной точки смены лидера не найдено — смотрите анализ по запросам выше.")
    print("===============================\n")

    return analysis

def print_runs_summary_from_agg(agg, user_counts):
    print("\n=== DATASET RUN COUNTS ===")
    for ds in sorted(agg.keys(), key=lambda d: user_counts.get(d, 0)):
        pg_total = sum(len(agg[ds][q].get("postgres", [])) for q in agg[ds])
        neo_total = sum(len(agg[ds][q].get("neo4j", [])) for q in agg[ds])
        print(f"{ds:<12} ({format_users(user_counts.get(ds,0))}) -> pg:{pg_total} neo:{neo_total}")
    print()

def plot_charts(mean_data, user_counts, out_dir=CHARTS_DIR):
    out_dir.mkdir(exist_ok=True)
    datasets_sorted = sorted(mean_data.keys(), key=lambda d: user_counts.get(d, 0))
    queries = set()
    for ds in mean_data:
        queries.update(mean_data[ds].keys())
    queries = sorted(queries)

    for q in queries:
        x = []
        pg_y = []
        neo_y = []
        for ds in datasets_sorted:
            pg = mean_data[ds][q].get("postgres")
            neo = mean_data[ds][q].get("neo4j")
            if pg is None or neo is None:
                continue
            x.append(user_counts.get(ds, 0))
            pg_y.append(pg)
            neo_y.append(neo)
        if len(x) < 2:
            continue

        x = np.array(x)
        pg_y = np.array(pg_y)
        neo_y = np.array(neo_y)

        try:
            pg_ip = PchipInterpolator(x, pg_y)
            neo_ip = PchipInterpolator(x, neo_y)
            xs = np.logspace(np.log10(x.min()), np.log10(x.max()), 600)
            pg_s = pg_ip(xs)
            neo_s = neo_ip(xs)
        except Exception:
            xs = x
            pg_s = pg_y
            neo_s = neo_y

        plt.figure(figsize=(9,5))
        plt.plot(xs, pg_s, label="Postgres", linewidth=2)
        plt.plot(xs, neo_s, label="Neo4j", linewidth=2)
        plt.scatter(x, pg_y, s=30)
        plt.scatter(x, neo_y, s=30)
        # plt.yscale("log")
        # plt.xscale("log")
        plt.xlabel("Users (log scale)")
        plt.ylabel("Time (sec)")
        plt.title(q)
        plt.grid(which="both", linestyle="--", alpha=0.4)
        plt.legend()
        plt.tight_layout()
        path = out_dir / f"{q}.png"
        plt.savefig(path, dpi=160)
        plt.close()

        print("Создан новый график:", out_dir / f"{q}.png")

def main():
    folder = "results/medium"
    print(f"📂 Загружаю benchmark-файлы из: {folder}")

    data, user_counts = load_benchmarks(folder)
    if not data:
        print("No benchmark files found.")
        return

    agg = aggregate_by_dataset(data)
    mean_data = compute_means(agg)

    print_runs_summary_from_agg(agg, user_counts)
    analysis = analyze_and_summarize(mean_data, user_counts)
    plot_charts(mean_data, user_counts)
    return analysis

if __name__ == "__main__":
    main()
