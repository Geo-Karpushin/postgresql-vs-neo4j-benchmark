#!/usr/bin/env python3
"""
Генератор датасетов для benchmark (исправленный)

Особенности:
- генерирует users.csv (user_id,name,age,city,registration_date)
- генерирует friendships.csv (user_id,friend_id,since)
  * каждая дружба записана ровно один раз в канонической форме (min, max)
  * нет self-loop
- избегает дубликатов (надёжная уникализация)
- потоковая запись friendships (chunked)
- совместим с PostgreSQL COPY и Neo4j LOAD CSV
"""
import argparse
import json
import logging
import os
from time import perf_counter

import numpy as np
import pandas as pd
from tqdm import tqdm

try:
    import polars as pl
    HAVE_POLARS = True
except Exception:
    HAVE_POLARS = False

# ----- config -----
CITIES = ["Moscow", "SPb", "Novosibirsk", "Ekaterinburg", "Kazan"]
CITY_PROBS = np.array([0.2, 0.15, 0.1, 0.1, 0.1], dtype=float)
CITY_PROBS /= CITY_PROBS.sum()

# ----- util -----
def ensure_writable(base_dir="generated"):
    os.makedirs(base_dir, exist_ok=True)
    test_file = os.path.join(base_dir, ".write_test")
    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
    except Exception:
        try:
            os.chmod(base_dir, 0o777)
        except Exception:
            pass
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)

def fast_ba_prealloc(n: int, m: int):
    """
    Быстрая реализация BA (Barabási–Albert). Возвращает ориентированные рёбра
    edges_u, edges_v — массивы одинаковой длины, где edge i = (edges_u[i], edges_v[i])
    """
    if n <= m:
        raise ValueError("n must be > m")

    init_nodes = m + 1
    init_edges = init_nodes * (init_nodes - 1) // 2
    est_edges = max(int(m * (n - (m + 1) / 2)), init_edges)

    edges_u = np.empty(est_edges, dtype=np.int64)
    edges_v = np.empty(est_edges, dtype=np.int64)
    e_ptr = 0
    deg = np.zeros(n, dtype=np.int64)

    # initial complete graph of size m+1
    for i in range(init_nodes):
        for j in range(i + 1, init_nodes):
            edges_u[e_ptr] = i
            edges_v[e_ptr] = j
            e_ptr += 1
            deg[i] += 1
            deg[j] += 1

    # reservoir (node repeated deg times)
    reservoir_size = int(2 * est_edges + n * m + 100)
    reservoir = np.empty(reservoir_size, dtype=np.int64)
    r_ptr = 0

    for node in range(init_nodes):
        d = deg[node]
        if d > 0:
            reservoir[r_ptr:r_ptr + d] = node
            r_ptr += d

    rng = np.random.default_rng()

    for new_node in range(init_nodes, n):
        # sample m targets preferentially from reservoir
        if r_ptr >= m:
            # sample without replacement if possible
            targets = rng.choice(reservoir[:r_ptr], size=m, replace=False)
        else:
            targets = rng.choice(reservoir[:r_ptr], size=m, replace=True)
        # add edges
        edges_u[e_ptr:e_ptr + m] = new_node
        edges_v[e_ptr:e_ptr + m] = targets
        e_ptr += m

        # update degrees and reservoir
        deg[new_node] = m
        for t in targets:
            deg[t] += 1

        # append to reservoir
        if r_ptr + 2 * m > reservoir_size:
            new_size = reservoir_size * 2 + m * 10
            reservoir = np.resize(reservoir, new_size)
            reservoir_size = new_size

        reservoir[r_ptr:r_ptr + m] = new_node
        r_ptr += m
        reservoir[r_ptr:r_ptr + m] = targets
        r_ptr += m

    return edges_u[:e_ptr].copy(), edges_v[:e_ptr].copy()

# ----- main writer -----
def generate_and_save(n, avg_friends, dataset_name, chunk_size=1_000_000, use_parquet=False, external_sort=False):
    """
    Генерация:
      n - users
      avg_friends - среднее число друзей (приблизительно)
      dataset_name - папка в generated/
      chunk_size - размер чанка при записи
      external_sort - если True, используем внешнюю сортировку для дедупа (для очень больших dataset)
    """
    t0 = perf_counter()
    m = max(1, avg_friends // 2)

    # 1) генерируем ориентированные рёбра BA
    edges_u, edges_v = fast_ba_prealloc(n, m)

    # 2) canonicalize pairs (min, max), remove self-loops
    a = edges_u
    b = edges_v
    u = np.minimum(a, b)
    v = np.maximum(a, b)
    mask = u != v
    u = u[mask]
    v = v[mask]

    # stack as 2-column array for fast unique
    pairs = np.vstack((u, v)).T  # shape (E,2)

    if external_sort:
        # For huge datasets: write unsorted chunks to disk, use system sort -u to dedupe.
        # Implementation left for very large cases; here we do in-memory unique.
        raise NotImplementedError("external_sort=True not implemented in this script")
    else:
        # lexsort + unique
        order = np.lexsort((pairs[:,1], pairs[:,0]))
        pairs = pairs[order]
        # now unique rows by comparing adjacent rows
        if pairs.shape[0] == 0:
            unique_pairs = pairs
        else:
            diffs = np.any(pairs[1:] != pairs[:-1], axis=1)
            keep = np.concatenate(([True], diffs))
            unique_pairs = pairs[keep]

    num_pairs = unique_pairs.shape[0]

    # 3) users df
    ids = np.arange(n, dtype=np.int64)
    names = np.array([f"User_{i}" for i in ids], dtype=object)
    rng = np.random.default_rng()
    ages = rng.integers(18, 70, size=n).astype(np.int64)
    cities = rng.choice(CITIES, size=n, p=CITY_PROBS)
    years = rng.integers(0, 4, size=n).astype(str)
    months = np.char.zfill(rng.integers(1, 13, size=n).astype(str), 2)
    days = np.char.zfill(rng.integers(1, 28, size=n).astype(str), 2)
    dates = ("202" + years + "-" + months + "-" + days).astype(object)

    users_df = pd.DataFrame({
        "user_id": ids,
        "name": names,
        "age": ages,
        "city": cities,
        "registration_date": dates
    })

    # 4) write files
    out_dir = f"generated/{dataset_name}"
    os.makedirs(out_dir, exist_ok=True)
    users_path = os.path.join(out_dir, "users.csv")
    friendships_path = os.path.join(out_dir, "friendships.csv")
    metadata_path = os.path.join(out_dir, "metadata.json")

    if use_parquet and HAVE_POLARS:
        pl.from_pandas(users_df).write_parquet(os.path.join(out_dir, "users.parquet"))
        users_df.to_csv(users_path, index=False)
    else:
        users_df.to_csv(users_path, index=False)

    # 5) stream write unique friendships in chunks (one row per undirected edge)
    total = num_pairs
    pbar = tqdm(total=total, desc="friendships.csv", unit="rows", dynamic_ncols=True)
    header = ["user_id", "friend_id", "since"]

    # open file and write in chunks
    with open(friendships_path, "w", encoding="utf-8", newline="") as out_f:
        # write header
        out_f.write(",".join(header) + "\n")
        # write by chunks
        chunk_st = 0
        while chunk_st < num_pairs:
            chunk_en = min(chunk_st + chunk_size, num_pairs)
            chunk = unique_pairs[chunk_st:chunk_en]
            k = chunk.shape[0]

            # generate dates for this chunk
            years = rng.integers(0, 4, size=k).astype(str)
            months = np.char.zfill(rng.integers(1, 13, size=k).astype(str), 2)
            days = np.char.zfill(rng.integers(1, 28, size=k).astype(str), 2)
            dates = ("202" + years + "-" + months + "-" + days)

            # build lines
            lines = []
            for i in range(k):
                u0 = int(chunk[i,0]); v0 = int(chunk[i,1])
                since = dates[i]
                lines.append(f"{u0},{v0},{since}\n")

            out_f.writelines(lines)
            pbar.update(k)
            chunk_st = chunk_en
    pbar.close()

    # 6) metadata
    metadata = {
        "num_users": int(n),
        "num_friendships": int(num_pairs),
        "avg_degree": float(2 * num_pairs / n)
    }
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    logging.info("Сохранено users: %s, friendships: %s (unique undirected edges)", users_path, friendships_path)
    logging.info("done in %.2fs", perf_counter() - t0)
    return users_path, friendships_path, metadata_path


# ----- CLI -----
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("users", type=int, help="кол-во пользователей")
    p.add_argument("avg_friends", type=int, help="среднее кол-во друзей (approx)")
    p.add_argument("dataset_name", help="имя папки в generated/")
    p.add_argument("--chunk-size", type=int, default=1_000_000)
    p.add_argument("--parquet", action="store_true")
    p.add_argument("--external-sort", action="store_true", help="использовать внешнюю сортировку (для очень больших наборов)")
    return p.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ensure_writable("generated")
    generate_and_save(args.users, args.avg_friends, args.dataset_name,
                      chunk_size=args.chunk_size,
                      use_parquet=args.parquet,
                      external_sort=args.external_sort)


if __name__ == "__main__":
    main()
