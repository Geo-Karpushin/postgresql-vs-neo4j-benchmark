POSTGRES_QUERIES = {
    "simple_friends": {
        "query": """
            SELECT friend_id FROM friendships WHERE user_id = %s
            UNION
            SELECT user_id FROM friendships WHERE friend_id = %s
        """,
        "description": "Получение всех прямых друзей пользователя (1-hop, ненаправленный граф)"
    },

    "friends_of_friends": {
        "query": """
            WITH direct_friends AS (
                SELECT friend_id AS friend FROM friendships WHERE user_id = %s
                UNION
                SELECT user_id AS friend FROM friendships WHERE friend_id = %s
            )
            SELECT DISTINCT
                CASE 
                    WHEN fe.user_id IN (SELECT friend FROM direct_friends) 
                    THEN fe.friend_id 
                    ELSE fe.user_id 
                END AS fof
            FROM direct_friends df
            JOIN friendships fe ON 
                fe.user_id = df.friend OR fe.friend_id = df.friend
            WHERE CASE 
                    WHEN fe.user_id = df.friend THEN fe.friend_id 
                    ELSE fe.user_id 
                END != %s
            AND CASE 
                    WHEN fe.user_id = df.friend THEN fe.friend_id 
                    ELSE fe.user_id 
                END NOT IN (SELECT friend FROM direct_friends)
        """,
        "description": "Поиск друзей друзей пользователя (2-hop), исключая прямые связи"
    },

    "mutual_friends": {
        "query": """
            WITH a_friends AS (
                SELECT friend_id AS friend FROM friendships WHERE user_id = %s
                UNION
                SELECT user_id AS friend FROM friendships WHERE friend_id = %s
            ),
            b_friends AS (
                SELECT friend_id AS friend FROM friendships WHERE user_id = %s
                UNION
                SELECT user_id AS friend FROM friendships WHERE friend_id = %s
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
        "description": "Рекомендации новых друзей на основе числа общих друзей"
    },

    "shortest_path": {
        "query": """
            WITH RECURSIVE
            edges AS (
                SELECT user_id AS src, friend_id AS dst FROM friendships
                UNION ALL
                SELECT friend_id AS src, user_id AS dst FROM friendships
            ),
            forward AS (
                SELECT 0 AS level, %s::bigint AS node
                UNION ALL
                SELECT f.level + 1, e.dst
                FROM forward f
                JOIN edges e ON e.src = f.node
                WHERE f.level < 2
            ),
            backward AS (
                SELECT 0 AS level, %s::bigint AS node
                UNION ALL
                SELECT b.level + 1, e.dst
                FROM backward b
                JOIN edges e ON e.src = b.node
                WHERE b.level < 2
            )
            SELECT
                fw.node AS meeting_node,
                fw.level + bw.level AS depth
            FROM forward fw
            JOIN backward bw ON fw.node = bw.node
            ORDER BY depth
            LIMIT 1
        """,
        "description": "Поиск кратчайшего пути между двумя пользователями (ограничение по глубине)"
    }
}


NEO4J_QUERIES = {
    "simple_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f:User)
            RETURN f.user_id AS friend
        """,
        "description": "Получение всех прямых друзей пользователя (1-hop, ненаправленный граф)"
    },

    "friends_of_friends": {
        "query": """
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f1:User)
            WITH u, collect(DISTINCT f1) AS friends  
            UNWIND friends AS f1  
            MATCH (f1)-[:FRIENDS_WITH]-(f2:User)
            WHERE f2 <> u AND NOT (u)-[:FRIENDS_WITH]-(f2)
            RETURN DISTINCT f2.user_id AS fof
        """,
        "description": "Поиск друзей друзей пользователя (2-hop), исключая прямые связи"
    },

    "mutual_friends": {
        "query": """
            MATCH (a:User {user_id: $userA})-[:FRIENDS_WITH]-(f:User)-[:FRIENDS_WITH]-(b:User {user_id: $userB})
            WHERE f <> a AND f <> b
            RETURN f.user_id AS mutual
        """,
        "description": "Поиск общих друзей для пары пользователей"
    },

    "friend_recommendations": {
        "query": """
            MATCH (u:User {user_id: $user_id})
            WITH u, [(u)-[:FRIENDS_WITH]-(f) | f] AS friends
            UNWIND friends AS f
            MATCH (f)-[:FRIENDS_WITH]-(rec)
            WHERE rec <> u AND rec NOT IN friends
            WITH rec, COUNT(*) AS common_friends
            ORDER BY common_friends DESC
            LIMIT 10
            RETURN rec.user_id AS candidate, common_friends
        """,
        "description": "Рекомендации новых друзей на основе количества общих соседей"
    },

    "shortest_path": {
        "query": """
            MATCH (start:User {user_id: $userA})
            MATCH (end:User {user_id: $userB})
            CALL apoc.path.expandConfig(start, {
                relationshipFilter: 'FRIENDS_WITH',
                minLevel: 1,
                maxLevel: 4,
                terminatorNodes: [end],
                uniqueness: 'NODE_GLOBAL'
            }) YIELD path
            RETURN [n IN nodes(path) | n.user_id] as path, 
                length(path) as depth
            LIMIT 1
        """,
        "description": "Поиск кратчайшего пути между двумя пользователями в графе"
    }
}


POSTGRES_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            WITH cohort_data AS (
                SELECT 
                    user_id,
                    DATE_TRUNC('year', registration_date) as cohort,
                    registration_date as reg_date,
                    registration_date + INTERVAL '1 year' as cohort_end
                FROM users
            )
            SELECT 
                c.cohort,
                DATE_TRUNC('month', f.since) as month,
                COUNT(DISTINCT f.user_id) as active_users,
                COUNT(*) as friendships_formed
            FROM cohort_data c
            JOIN friendships f ON f.user_id = c.user_id 
                AND f.since BETWEEN c.reg_date AND c.cohort_end
            GROUP BY c.cohort, DATE_TRUNC('month', f.since)
        """,
        "description": "Когортный анализ формирования дружеских связей в первый год после регистрации"
    },

    "social_cities": {
        "query": """
            WITH intercity_friendships AS (
                SELECT DISTINCT
                    LEAST(u1.user_id, u2.user_id) AS user1,
                    GREATEST(u1.user_id, u2.user_id) AS user2,
                    u1.city AS user_city,
                    u2.city AS friend_city
                FROM friendships f
                JOIN users u1 ON f.user_id = u1.user_id
                JOIN users u2 ON f.friend_id = u2.user_id
                WHERE u1.city <> u2.city
            )
            SELECT
                user_city,
                COUNT(*) AS intercity_friendships_count,
                COUNT(DISTINCT friend_city) AS unique_cities_connected
            FROM intercity_friendships
            GROUP BY user_city
            ORDER BY intercity_friendships_count DESC
            LIMIT 5
        """,
        "description": "Определение городов с наибольшим числом межгородских дружеских связей"
    },

    "age_gap_analysis": {
        "query": """
            WITH age_gaps AS (
                SELECT
                    ABS(u1.age - u2.age) AS age_difference
                FROM friendships f
                JOIN users u1 ON f.user_id = u1.user_id
                JOIN users u2 ON f.friend_id = u2.user_id
                WHERE u1.user_id < u2.user_id
            ),
            categorized AS (
                SELECT
                    CASE
                        WHEN age_difference <= 1 THEN '0-1 год'
                        WHEN age_difference <= 5 THEN '2-5 лет'
                        WHEN age_difference <= 10 THEN '6-10 лет'
                        ELSE '>10 лет'
                    END AS age_gap_category,
                    CASE
                        WHEN age_difference <= 1 THEN 1
                        WHEN age_difference <= 5 THEN 2
                        WHEN age_difference <= 10 THEN 3
                        ELSE 4
                    END AS sort_order,
                    age_difference
                FROM age_gaps
            )
            SELECT
                age_gap_category,
                COUNT(*) AS friendships_count,
                ROUND(
                    100.0 * COUNT(*) / SUM(COUNT(*)) OVER (),
                    2
                ) AS percentage
            FROM categorized
            GROUP BY age_gap_category, sort_order
            ORDER BY sort_order
        """,
        "description": "Распределение дружеских связей по разнице возраста пользователей"
    },

    "network_growth": {
        "query": """
            WITH quarterly_dates AS (
                SELECT generate_series(
                    date_trunc('quarter', CURRENT_DATE - INTERVAL '5 years'),
                    date_trunc('quarter', CURRENT_DATE),
                    INTERVAL '3 months'
                ) AS quarter_end
            ),
            cumulative AS (
                SELECT
                    q.quarter_end,
                    COUNT(DISTINCT
                        LEAST(f.user_id, f.friend_id)::text || '-' ||
                        GREATEST(f.user_id, f.friend_id)::text
                    ) AS cumulative_friendships
                FROM quarterly_dates q
                LEFT JOIN friendships f
                ON f.since <= q.quarter_end
                GROUP BY q.quarter_end
            )
            SELECT
                to_char(quarter_end, 'YYYY-"Q"Q') AS quarter,
                cumulative_friendships,
                cumulative_friendships -
                LAG(cumulative_friendships, 1, 0)
                OVER (ORDER BY quarter_end) AS new_friendships_this_quarter
            FROM cumulative
            ORDER BY quarter_end
        """,
        "description": "Анализ динамики роста социальной сети по кварталам"
    },

    "age_clustering": {
        "query": """
            WITH edges AS (
                SELECT user_id AS a, friend_id AS b FROM friendships
                UNION ALL
                SELECT friend_id AS a, user_id AS b FROM friendships
            ),
            user_degrees AS (
                SELECT
                    u.user_id,
                    width_bucket(u.age, 10, 80, 7) AS age_bucket,
                    COUNT(DISTINCT e.b) AS degree
                FROM users u
                LEFT JOIN edges e ON e.a = u.user_id
                GROUP BY u.user_id, age_bucket
            ),
            triangles AS (
                SELECT
                    e1.a AS user_id,
                    COUNT(*) AS triangle_count
                FROM edges e1
                JOIN edges e2 ON e1.b = e2.a
                JOIN edges e3 ON e2.b = e3.b
                            AND e3.a = e1.a
                WHERE e1.a < e1.b
                GROUP BY e1.a
            )
            SELECT
                age_bucket * 10 + 10 AS age_group_start,
                COUNT(*) AS users_count,
                AVG(
                    COALESCE(t.triangle_count, 0) * 2.0 /
                    (d.degree * (d.degree - 1))
                ) AS avg_clustering_coefficient
            FROM user_degrees d
            LEFT JOIN triangles t ON d.user_id = t.user_id
            WHERE d.degree >= 2
            GROUP BY age_bucket
            ORDER BY age_bucket
        """,
        "description": "Оценка коэффициента кластеризации пользователей по возрастным группам"
    }
}


NEO4J_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            MATCH (u:User)-[r:FRIENDS_WITH]-(f:User)
            WHERE u.user_id < f.user_id
                AND r.since >= u.registration_date
                AND r.since <  u.registration_date + duration({years: 1})
            WITH
                u.registration_date.year AS cohortYear,
                r.since.month AS friendshipMonth
            RETURN
                cohortYear,
                friendshipMonth,
                count(*) AS friendshipsCount
            ORDER BY cohortYear, friendshipMonth
        """,
        "description": "Когортный анализ формирования дружеских связей в первый год"
    },

    "social_cities": {
        "query": """
            MATCH (u1:User)-[:FRIENDS_WITH]-(u2:User)
            WHERE u1.city <> u2.city
              AND u1.user_id < u2.user_id
            WITH u1.city AS userCity, u2.city AS friendCity
            RETURN
                userCity,
                COUNT(*) AS intercityFriendshipsCount,
                COUNT(DISTINCT friendCity) AS uniqueCitiesConnected
            ORDER BY intercityFriendshipsCount DESC
            LIMIT 5
        """,
        "description": "Поиск городов с наибольшей межгородской социальной активностью"
    },

    "age_gap_analysis": {
        "query": """
            MATCH (u1:User)-[:FRIENDS_WITH]-(u2:User)
            WHERE u1.user_id < u2.user_id
            WITH abs(u1.age - u2.age) AS ageDifference
            WITH
                CASE
                    WHEN ageDifference <= 1 THEN '0-1 год'
                    WHEN ageDifference <= 5 THEN '2-5 лет'
                    WHEN ageDifference <= 10 THEN '6-10 лет'
                    ELSE '>10 лет'
                END AS category
            WITH category, COUNT(*) AS friendshipsCount
            WITH collect({cat: category, cnt: friendshipsCount}) AS data,
                reduce(total = 0, x IN collect({cat: category, cnt: friendshipsCount}) | total + x.cnt) AS totalCount
            UNWIND data AS d
            RETURN
                d.cat AS category,
                d.cnt AS friendshipsCount,
                round(100.0 * d.cnt / totalCount, 2) AS percentage
            ORDER BY
                CASE d.cat
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
            WITH range(0, 19) as quarters
            UNWIND quarters as q
            WITH date() - duration({months: (19 - q) * 3}) as quarter_end
            MATCH (u1:User)-[r:FRIENDS_WITH]-(u2:User)
            WHERE r.since <= quarter_end
            AND u1.user_id < u2.user_id
            WITH quarter_end, count(*) as cumulative_friendships
            ORDER BY quarter_end
            WITH collect({q: quarter_end, c: cumulative_friendships}) as data
            UNWIND range(0, size(data)-1) as idx
            RETURN 
                toString(data[idx].q.year) + '-Q' + 
                toString(data[idx].q.quarter) as quarter,
                data[idx].c as cumulative_friendships,
                data[idx].c - coalesce(data[idx-1].c, 0) as new_friendships_this_quarter
            ORDER BY quarter
        """,
        "description": "Динамика роста количества дружеских связей во времени"
    },

    "age_clustering": {
        "query": """
            MATCH (u:User)
            WHERE u.age IS NOT NULL
            WITH u, toInteger(floor(u.age / 10) * 10) AS ageGroupStart
            MATCH (u)-[:FRIENDS_WITH]-(n:User)
            WITH u, ageGroupStart, collect(DISTINCT n) AS neighbors
            WHERE size(neighbors) >= 2
            WITH u, ageGroupStart, neighbors, size(neighbors) AS k
            CALL (neighbors) {
                UNWIND neighbors AS n1
                MATCH (n1)-[:FRIENDS_WITH]-(n2:User)
                WHERE n2 IN neighbors AND n1.user_id < n2.user_id
                RETURN COUNT(*) AS edgeCount
            }
            WITH ageGroupStart,
                CASE WHEN k >= 2 THEN 2.0 * edgeCount / (k * (k - 1)) ELSE 0 END AS clusteringCoefficient
            WITH ageGroupStart,
                toString(ageGroupStart) + '-' + toString(ageGroupStart + 9) AS ageGroup,
                clusteringCoefficient
            RETURN
                ageGroup,
                count(*) AS usersCount,
                avg(clusteringCoefficient) AS avgClusteringCoefficient
            ORDER BY ageGroup
        """,
        "description": "Коэффициент кластеризации пользователей, агрегированный по возрастным группам"
    }
}
