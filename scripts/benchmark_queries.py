#!/usr/bin/env python3
"""
Тестовые запросы для сравнительного анализа PostgreSQL и Neo4j.
"""
POSTGRES_QUERIES = {
    "simple_friends": {
        "query": """
            SELECT DISTINCT fid FROM (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ) AS all_friends
        """,
        "description": "Простой запрос: друзья пользователя (симметрично, DISTINCT)"
    },
    "friends_of_friends": {
        "query": """
            WITH all_friends AS (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ),
            fof AS (
                SELECT f2.friend_id AS fof_id
                FROM all_friends f1
                JOIN friendships f2 ON f1.fid = f2.user_id
                UNION ALL
                SELECT f2.user_id AS fof_id
                FROM all_friends f1
                JOIN friendships f2 ON f1.fid = f2.friend_id
            )
            SELECT DISTINCT fof_id
            FROM fof
            WHERE fof_id <> %s
        """,
        "description": "Друзья друзей (глубина 2, симметрично, DISTINCT)"
    },
    "mutual_friends": {
        "query": """
            SELECT DISTINCT f1.fid
            FROM (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ) AS f1
            JOIN (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ) AS f2
            ON f1.fid = f2.fid
        """,
        "description": "Общие друзья двух пользователей (симметрично, DISTINCT)"
    },
    "friend_recommendations": {
        "query": """
            WITH all_friends AS (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ),
            fof AS (
                SELECT f2.friend_id AS cand
                FROM all_friends f1
                JOIN friendships f2 ON f1.fid = f2.user_id
                UNION ALL
                SELECT f2.user_id AS cand
                FROM all_friends f1
                JOIN friendships f2 ON f1.fid = f2.friend_id
            )
            SELECT cand, COUNT(*) AS common_friends
            FROM fof
            WHERE cand <> %s
              AND cand NOT IN (SELECT fid FROM all_friends)
            GROUP BY cand
            ORDER BY common_friends DESC
            LIMIT 10
        """,
        "description": "Рекомендации друзей (симметрично, как Neo4j)"
    },
    "shortest_path": {
        "query": """
            WITH RECURSIVE search AS (
            SELECT 
                ARRAY[$1] AS path,
                $1        AS current,
                0         AS depth
            UNION ALL
            SELECT 
                path || f.friend_id,
                f.friend_id,
                depth + 1
            FROM search s
            JOIN friendships f ON f.user_id = s.current
            WHERE 
                depth < 4
                AND f.friend_id <> ALL(path)
        )
        SELECT path, depth
        FROM search
        WHERE current = $2 AND depth > 0
        ORDER BY depth
        LIMIT 1;
        """,
        "description": "Кратчайший путь между двумя пользователями (симметрично, depth<=4)"
    }
}


NEO4J_QUERIES = {
    "simple_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(friend)
            RETURN DISTINCT friend.user_id
        """,
        "description": "Простой запрос: друзья пользователя (симметрично)"
    },
    "friends_of_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-()-[:FRIENDS_WITH]-(fof)
            WHERE fof.user_id <> $user_id
            RETURN DISTINCT fof.user_id
        """,
        "description": "Друзья друзей (глубина 2, симметрично)"
    },
    "mutual_friends": {
        "query": """
            MATCH (a:User {user_id: $userA})-[:FRIENDS_WITH]-(mutual)-[:FRIENDS_WITH]-(b:User {user_id: $userB})
            RETURN DISTINCT mutual.user_id
        """,
        "description": "Общие друзья двух пользователей (симметрично)"
    },
    "friend_recommendations": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-()-[:FRIENDS_WITH]-(rec)
            WHERE rec.user_id <> $user_id AND NOT (u)-[:FRIENDS_WITH]-(rec)
            RETURN rec.user_id AS user_id, COUNT(*) AS common_friends
            ORDER BY common_friends DESC
            LIMIT 10
        """,
        "description": "Рекомендации друзей (симметрично)"
    },
    "shortest_path": {
        "query": """
            MATCH (start:User {id: $from}), (end:User {id: $to})
            CALL {
                WITH start, end
                MATCH path = shortestPath(
                    (start)-[:FRIENDS_WITH*..4]-(end)
                )
                RETURN path
            }
            RETURN [n IN nodes(path) | n.id] AS path,
                length(path) AS depth
            ORDER BY depth
            LIMIT 1;
        """,
        "description": "Кратчайший путь между двумя пользователями (симметрично, depth<=4)"
    }
}

