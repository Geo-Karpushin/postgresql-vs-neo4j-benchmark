POSTGRES_QUERIES = {
    "simple_friends": {
        "query": """
            SELECT DISTINCT 
                CASE WHEN user_id = %s THEN friend_id ELSE user_id END AS friend_id
            FROM friendships 
            WHERE user_id = %s OR friend_id = %s
        """,
        "description": "Получение всех прямых друзей пользователя (1-hop, ненаправленный граф)"
    },

    "friends_of_friends": {
        "query": """
            WITH direct_friends AS (
                SELECT DISTINCT 
                    CASE WHEN user_id = %s THEN friend_id ELSE user_id END AS friend
                FROM friendships 
                WHERE user_id = %s OR friend_id = %s
            )
            SELECT DISTINCT
                CASE 
                    WHEN f.user_id = df.friend THEN f.friend_id 
                    ELSE f.user_id 
                END AS fof
            FROM direct_friends df
            JOIN friendships f ON f.user_id = df.friend OR f.friend_id = df.friend
            WHERE CASE 
                    WHEN f.user_id = df.friend THEN f.friend_id 
                    ELSE f.user_id 
                END != %s
                AND CASE 
                    WHEN f.user_id = df.friend THEN f.friend_id 
                    ELSE f.user_id 
                END NOT IN (SELECT friend FROM direct_friends)
        """,
        "description": "Поиск друзей друзей пользователя (2-hop), исключая прямые связи"
    },

    "mutual_friends": {
        "query": """
            WITH a_friends AS (
                SELECT DISTINCT 
                    CASE WHEN user_id = %s THEN friend_id ELSE user_id END AS friend
                FROM friendships 
                WHERE user_id = %s OR friend_id = %s
            ),
            b_friends AS (
                SELECT DISTINCT 
                    CASE WHEN user_id = %s THEN friend_id ELSE user_id END AS friend
                FROM friendships 
                WHERE user_id = %s OR friend_id = %s
            )
            SELECT a.friend AS mutual
            FROM a_friends a
            INNER JOIN b_friends b ON a.friend = b.friend
        """,
        "description": "Поиск общих друзей для пары пользователей"
    },

    "friend_recommendations": {
        "query": """
            WITH all_friends AS (
                SELECT friend_id AS fid FROM friendships WHERE user_id = %s
                UNION ALL
                SELECT user_id AS fid FROM friendships WHERE friend_id = %s
            ),
            fof AS (
                SELECT 
                    CASE WHEN f2.user_id = f1.fid THEN f2.friend_id ELSE f2.user_id END AS cand
                FROM all_friends f1
                JOIN friendships f2 ON f1.fid = f2.user_id OR f1.fid = f2.friend_id
            )
            SELECT cand, COUNT(*) AS common_friends
            FROM fof
            WHERE cand <> %s
            AND cand NOT IN (SELECT fid FROM all_friends)
            GROUP BY cand
            ORDER BY common_friends DESC
            LIMIT 10
        """,
        "description": "Рекомендации новых друзей на основе числа общих друзей"
    },

    "shortest_path": {
        "query": """
            WITH RECURSIVE 
                forward(level, node, path) AS (
                    SELECT 0, %s, CAST(%s AS TEXT)
                    UNION ALL
                    SELECT level + 1, f.friend_id, fw.path || ',' || f.friend_id
                    FROM forward fw
                    JOIN friendships f ON f.user_id = fw.node
                    WHERE level < 4
                ),
                backward(level, node, path) AS (
                    SELECT 0, %s, CAST(%s AS TEXT)
                    UNION ALL
                    SELECT level + 1, f.user_id, bw.path || ',' || f.user_id
                    FROM backward bw
                    JOIN friendships f ON f.friend_id = bw.node
                    WHERE level < 4
                ),
                combined_paths AS (
                    SELECT 
                        fw.path || ',' || bw.path AS full_path,
                        fw.level + bw.level AS depth
                    FROM forward fw
                    JOIN backward bw ON fw.node = bw.node
                    WHERE fw.level + bw.level <= 4
                    ORDER BY fw.level + bw.level
                    LIMIT 1
                )
            SELECT 
                CASE 
                    WHEN full_path IS NOT NULL THEN
                        -- Разбиваем путь на массив ID
                        CASE 
                            WHEN depth = 0 THEN '[' || REPLACE(full_path, ',', ',') || ']'
                            ELSE '[' || full_path || ']'
                        END
                    ELSE '[]'
                END AS path,
                COALESCE(depth, -1) AS depth
            FROM combined_paths;
        """,
        "description": "Поиск кратчайшего пути между двумя пользователями (до глубины 4)"
    }
}

NEO4J_QUERIES = {
    "simple_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(friend:User)
            RETURN friend.user_id AS friend_id
        """,
        "description": "Получение всех прямых друзей пользователя (1-hop, ненаправленный граф)"
    },

    "friends_of_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f1:User)
            WITH u, collect(f1) AS direct_friends
            UNWIND direct_friends AS friend
            MATCH (friend)-[:FRIENDS_WITH]-(f2:User)
            WHERE f2 <> u AND NOT f2 IN direct_friends
            RETURN DISTINCT f2.user_id AS fof
        """,
        "description": "Поиск друзей друзей пользователя (2-hop), исключая прямые связи"
    },

    "mutual_friends": {
        "query": """
            MATCH (a:User {user_id: $userA})-[:FRIENDS_WITH]-(friend:User)-[:FRIENDS_WITH]-(b:User {user_id: $userB})
            RETURN friend.user_id AS mutual
        """,
        "description": "Поиск общих друзей для пары пользователей"
    },

    "friend_recommendations": {
        "query": """
            MATCH (me:User {user_id: $user_id})-[:FRIENDS_WITH]-(friend:User)
            WITH me, collect(friend) AS my_friends
            UNWIND my_friends AS friend
            MATCH (friend)-[:FRIENDS_WITH]-(recommendation:User)
            WHERE recommendation <> me AND NOT recommendation IN my_friends
            WITH recommendation, count(friend) AS common_friends
            ORDER BY common_friends DESC
            LIMIT 10
            RETURN recommendation.user_id AS candidate, common_friends
        """,
        "description": "Рекомендации новых друзей на основе количества общих соседей"
    },

    "shortest_path": {
        "query": """
            MATCH (start:User {user_id: $userA})
            MATCH (end:User {user_id: $userB})
            MATCH path = shortestPath((start)-[:FRIENDS_WITH*..4]-(end))
            RETURN [node IN nodes(path) | node.user_id] AS path, 
                   length(path) AS depth
            LIMIT 1
        """,
        "description": "Поиск кратчайшего пути между двумя пользователями в графе (глубина до 4)"
    }
}

POSTGRES_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            SELECT 
                EXTRACT(YEAR FROM u.registration_date) as cohort_year,
                EXTRACT(MONTH FROM f.since) as friendship_month,
                COUNT(DISTINCT u.user_id) as users_count,
                COUNT(*) as friendships_count
            FROM users u
            JOIN friendships f ON u.user_id = f.user_id
            WHERE EXTRACT(YEAR FROM f.since) = EXTRACT(YEAR FROM u.registration_date)
            GROUP BY EXTRACT(YEAR FROM u.registration_date), EXTRACT(MONTH FROM f.since)
            ORDER BY cohort_year, friendship_month
        """,
        "description": "Когортный анализ формирования дружеских связей в первый год"
    },

    "social_cities": {
        "query": """
            SELECT 
                u1.city as city,
                COUNT(*) as connections_count,
                COUNT(DISTINCT u2.city) as unique_cities
            FROM friendships f
            JOIN users u1 ON f.user_id = u1.user_id
            JOIN users u2 ON f.friend_id = u2.user_id
            WHERE u1.city <> u2.city
            GROUP BY u1.city
            ORDER BY connections_count DESC
            LIMIT 5
        """,
        "description": "Определение городов с наибольшим числом межгородских дружеских связей"
    },

    "age_gap_analysis": {
        "query": """
            SELECT 
                age_category,
                COUNT(*) as count
            FROM (
                SELECT 
                    CASE 
                        WHEN ABS(u1.age - u2.age) <= 1 THEN '0-1 год'
                        WHEN ABS(u1.age - u2.age) <= 5 THEN '2-5 лет'
                        WHEN ABS(u1.age - u2.age) <= 10 THEN '6-10 лет'
                        ELSE '>10 лет'
                    END as age_category
                FROM friendships f
                JOIN users u1 ON f.user_id = u1.user_id
                JOIN users u2 ON f.friend_id = u2.user_id
                WHERE u1.age IS NOT NULL AND u2.age IS NOT NULL
            ) t
            GROUP BY age_category
            ORDER BY 
                CASE age_category
                    WHEN '0-1 год' THEN 1
                    WHEN '2-5 лет' THEN 2
                    WHEN '6-10 лет' THEN 3
                    ELSE 4
                END
        """,
        "description": "Распределение дружеских связей по разнице возраста пользователей"
    },

    "network_growth": {
        "query": """
            SELECT 
                EXTRACT(YEAR FROM since) as year,
                EXTRACT(MONTH FROM since) as month,
                COUNT(*) as new_friendships
            FROM friendships
            WHERE since >= CURRENT_DATE - INTERVAL '2 years'
            GROUP BY EXTRACT(YEAR FROM since), EXTRACT(MONTH FROM since)
            ORDER BY year, month
            LIMIT 12
        """,
        "description": "Анализ динамики роста социальной сети по месяцам за 2 года"
    },

    "age_clustering": {
        "query": """
            WITH user_stats AS (
                SELECT 
                    u.user_id,
                    u.age,
                    COUNT(DISTINCT CASE WHEN f.user_id = u.user_id THEN f.friend_id END) as degree
                FROM users u
                LEFT JOIN friendships f ON u.user_id = f.user_id
                WHERE u.age IS NOT NULL
                GROUP BY u.user_id, u.age
            )
            SELECT 
                FLOOR(us.age / 10) * 10 as age_group,
                COUNT(DISTINCT us.user_id) as users_count,
                AVG(CASE WHEN us.degree >= 2 THEN 
                    1.0 * (us.degree - 1) / us.degree 
                    ELSE 0 END) as clustering_estimate
            FROM user_stats us
            GROUP BY FLOOR(us.age / 10) * 10
            HAVING COUNT(DISTINCT us.user_id) >= 2
            ORDER BY age_group
        """,
        "description": "Оценка коэффициента кластеризации пользователей по возрастным группам"
    }
}

NEO4J_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            MATCH (u:User)-[r:FRIENDS_WITH]-(:User)
            WHERE date(r.since).year = date(u.registration_date).year
            RETURN 
                date(u.registration_date).year AS cohort_year,
                date(r.since).month AS friendship_month,
                count(DISTINCT u) AS users_count,
                count(r) AS friendships_count
            ORDER BY cohort_year, friendship_month
        """,
        "description": "Когортный анализ формирования дружеских связей в первый год"
    },

    "social_cities": {
        "query": """
            MATCH (u1:User)-[:FRIENDS_WITH]-(u2:User)
            WHERE u1.city <> u2.city
            RETURN 
                u1.city AS city,
                count(*) AS connections_count,
                count(DISTINCT u2.city) AS unique_cities
            ORDER BY connections_count DESC
            LIMIT 5
        """,
        "description": "Поиск городов с наибольшей межгородской социальной активностью"
    },

    "age_gap_analysis": {
        "query": """
            MATCH (u1:User)-[:FRIENDS_WITH]-(u2:User)
            WHERE u1.age IS NOT NULL AND u2.age IS NOT NULL
            WITH 
                CASE 
                    WHEN abs(u1.age - u2.age) <= 1 THEN '0-1 год'
                    WHEN abs(u1.age - u2.age) <= 5 THEN '2-5 лет'
                    WHEN abs(u1.age - u2.age) <= 10 THEN '6-10 лет'
                    ELSE '>10 лет'
                END AS age_category
            RETURN 
                age_category,
                count(*) AS count
            ORDER BY 
                CASE age_category
                    WHEN '0-1 год' THEN 1
                    WHEN '2-5 лет' THEN 2
                    WHEN '6-10 лет' THEN 3
                    ELSE 4
                END
        """,
        "description": "Распределение дружеских связей по возрастной разнице пользователей"
    },

    "network_growth": {
        "query": """
            MATCH ()-[r:FRIENDS_WITH]-()
            WHERE r.since IS NOT NULL
            AND r.since.year >= date().year - 2
            RETURN 
                r.since.year AS year,
                r.since.month AS month,
                count(r) AS new_friendships
            ORDER BY year, month
            LIMIT 12
        """,
        "description": "Динамика роста количества дружеских связей по месяцам за 2 года"
    },

    "age_clustering": {
        "query": """
            MATCH (u:User)
            WHERE u.age IS NOT NULL
            WITH u, toInteger(floor(u.age / 10)) * 10 AS age_group
            OPTIONAL MATCH (u)-[:FRIENDS_WITH]-(friend:User)
            WITH age_group, u, count(friend) AS degree
            WHERE degree >= 2
            RETURN 
                age_group,
                count(DISTINCT u) AS users_count,
                avg(1.0 * (degree - 1) / degree) AS clustering_estimate
            ORDER BY age_group
        """,
        "description": "Коэффициент кластеризации пользователей по возрастным группам"
    }
}