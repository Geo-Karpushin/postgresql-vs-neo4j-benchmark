#!/usr/bin/env python3
import numpy as np
import networkx as nx
import pandas as pd
import json
import sys
import os
import logging
from tqdm import tqdm

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

CITIES = ["Moscow", "SPb", "Novosibirsk", "Ekaterinburg", "Kazan"]
CITY_PROBS = np.array([0.2, 0.15, 0.1, 0.1, 0.1])
CITY_PROBS = CITY_PROBS / CITY_PROBS.sum()

STRENGTHS = ["weak", "medium", "strong"]
STRENGTH_PROBS = [0.5, 0.35, 0.15]

def generate_dataset(num_users, avg_friends):
    print(f"Генерация графа: {num_users} users, avg friends = {avg_friends}")

    m = max(1, avg_friends // 2)
    G = nx.barabasi_albert_graph(num_users, m)

    users = []
    for i in tqdm(range(num_users), desc="Генерация пользователей",
                  dynamic_ncols=True, leave=True, file=sys.stdout,
                  mininterval=0.1, smoothing=0.1):
        users.append({
            "user_id": i,
            "name": f"User_{i}",
            "age": int(np.random.randint(18, 70)),
            "city": str(np.random.choice(CITIES, p=CITY_PROBS)),
            "registration_date": f"202{np.random.randint(0,4)}-"
                                 f"{np.random.randint(1,13):02d}-"
                                 f"{np.random.randint(1,28):02d}"
        })

    edges = list(G.edges())
    num_edges = len(edges)

    years  = np.random.randint(0, 4,  num_edges)
    months = np.random.randint(1, 13, num_edges)
    days   = np.random.randint(1, 28, num_edges)

    dates = [
        f"202{y}-{m:02d}-{d:02d}"
        for y, m, d in zip(years, months, days)
    ]

    strength1 = np.random.choice(STRENGTHS, num_edges, p=STRENGTH_PROBS)
    strength2 = np.random.choice(STRENGTHS, num_edges, p=STRENGTH_PROBS)

    friendships = []
    for (u, v), date, s1, s2 in tqdm(
            zip(edges, dates, strength1, strength2),
            total=num_edges,
            desc="Генерация связей",
            dynamic_ncols=True, leave=True, file=sys.stdout,
            mininterval=0.1, smoothing=0.1):

        friendships.append({
            "user_id": int(u),
            "friend_id": int(v),
            "since": date,
            "strength": str(s1)
        })
        friendships.append({
            "user_id": int(v),
            "friend_id": int(u),
            "since": date,
            "strength": str(s2)
        })

    return users, friendships

def save_csv(users, friendships, dataset_name):
    out_dir = f"generated/{dataset_name}"
    os.makedirs(out_dir, exist_ok=True)

    users_path        = f"{out_dir}/users.csv"
    friendships_path  = f"{out_dir}/friendships.csv"
    metadata_path     = f"{out_dir}/metadata.json"

    with tqdm(total=3, desc="Сохранение CSV", leave=False, dynamic_ncols=True, mininterval=0.1, smoothing=0.1) as pbar:
        pd.DataFrame(users).to_csv(users_path, index=False)
        pbar.update(1)
        pd.DataFrame(friendships).to_csv(friendships_path, index=False)
        pbar.update(1)
        with open(metadata_path, "w") as f:
            json.dump({
                "num_users": len(users),
                "num_friendships": len(friendships),
                "avg_degree": len(friendships) / len(users)
            }, f, indent=2)
        pbar.update(1)

    print(f"CSV сохранены: {users_path}, {friendships_path}")
    return users_path, friendships_path

def ensure_writable(base_dir="generated"):
    """
    Проверяет, что каталог writable. Если нет — пытается исправить.
    Если не удалось — завершает программу с объяснением.
    """
    os.makedirs(base_dir, exist_ok=True)

    test_file = os.path.join(base_dir, ".write_test")

    try:
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
        print(f"Папка '{base_dir}' доступна для записи ✔")
        return
    except Exception as e:
        print(f"⚠ Нет прав на запись в '{base_dir}': {e}")
        print("Пробуем исправить...")
    
    try:
        os.chmod(base_dir, 0o777)
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
        print(f"Права исправлены, запись доступна ✔")
        return
    except Exception as e:
        print(f"❌ Не удалось исправить права в '{base_dir}'. Ошибка: {e}")
        print("Пожалуйста, запустите вручную:")
        print(f"  sudo chmod -R 777 {base_dir}")
        print(f"  sudo chown -R $USER:$USER {base_dir}")
        sys.exit(1)

def main():
    ensure_writable("generated")

    datasets = {
        "small":      {"users": 50_000,    "avg_friends": 20},
        "medium":     {"users": 500_000,   "avg_friends": 15},
        "large":      {"users": 2_000_000, "avg_friends": 12},
        "x-large":    {"users": 5_000_000, "avg_friends": 10},
        "xx-large":   {"users": 10_000_000, "avg_friends": 8}
    }

    dataset_name = sys.argv[1] if len(sys.argv) > 1 else "small"

    if dataset_name not in datasets:
        print("Допустимые датасеты: small / medium / large / x-large / xx-large")
        return

    cfg = datasets[dataset_name]

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    users, friendships = generate_dataset(cfg["users"], cfg["avg_friends"])
    save_csv(users, friendships, dataset_name)

if __name__ == "__main__":
    main()
