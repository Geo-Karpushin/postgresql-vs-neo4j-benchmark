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
        "description": "Простой запрос: друзья пользователя"
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
        "description": "Друзья друзей (depth == 2)"
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
        "description": "Общие друзья двух пользователей"
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
        "description": "Рекомендации друзей"
    },
    "shortest_path": {
        "query": """
            WITH RECURSIVE 
            forward(level, node) AS (
                SELECT 0, %s::bigint
                UNION ALL
                SELECT level + 1, f.friend_id
                FROM forward fw
                JOIN friendships f ON f.user_id = fw.node
                WHERE level < 2
            ),
            backward(level, node) AS (
                SELECT 0, %s::bigint
                UNION ALL
                SELECT level + 1, f.user_id
                FROM backward bw
                JOIN friendships f ON f.friend_id = bw.node
                WHERE level < 2
            )
            SELECT fw.node, bw.node, fw.level + bw.level AS depth
            FROM forward fw
            JOIN backward bw ON fw.node = bw.node
            ORDER BY depth
            LIMIT 1;
        """,
        "description": "Кратчайший путь между двумя пользователями (depth<=4)"
    }
}

NEO4J_QUERIES = {
    "simple_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f:User)
            RETURN DISTINCT f.user_id AS friend
        """,
        "description": "Простой запрос: друзья пользователя"
    },

    "friends_of_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f1:User)
            WITH u, COLLECT(f1) as direct_friends
            UNWIND direct_friends as friend
            MATCH (friend)-[:FRIENDS_WITH]-(f2:User)
            WHERE f2 <> u AND NOT f2 IN direct_friends
            RETURN DISTINCT f2.user_id AS fof
        """,
        "description": "Друзья друзей — depth=2 (оптимизированный)"
    },

    "mutual_friends": {
        "query": """
            MATCH (a:User {user_id: $userA})-[:FRIENDS_WITH]->(f:User)
            MATCH (b:User {user_id: $userB})-[:FRIENDS_WITH]->(f)
            RETURN DISTINCT f.user_id AS mutual
        """,
        "description": "Общие друзья двух пользователей"
    },

    "friend_recommendations": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-()-[:FRIENDS_WITH]-(rec:User)
            WHERE rec.user_id <> $user_id
              AND NOT (u)-[:FRIENDS_WITH]-(rec)
            RETURN rec.user_id AS user_id,
                   COUNT(*) AS common_friends
            ORDER BY common_friends DESC
            LIMIT 10
        """,
        "description": "Рекомендации друзей"
    },

    "shortest_path": {
        "query": """
            MATCH (start:User {user_id: $userA})
            MATCH (end:User {user_id: $userB})
            CALL apoc.path.expandConfig(start, {
                relationshipFilter: "FRIENDS_WITH>",
                minLevel: 1,
                maxLevel: 4,
                bfs: true,
                terminateNodes: [end],
                uniqueness: "NODE_GLOBAL"
            }) YIELD path
            WHERE last(nodes(path)) = end
            RETURN [n IN nodes(path) | n.user_id] AS path,
                length(path) AS depth
            ORDER BY depth ASC
            LIMIT 1
        """,
        "description": "Кратчайший путь через APOC expandConfig"
    }
}

