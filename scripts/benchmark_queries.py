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
            MATCH (u:User {user_id: $user_id})-[:FRIENDS_WITH]-(f1:User)-[:FRIENDS_WITH]-(f2:User)
            WHERE u <> f2 AND NOT (u)-[:FRIENDS_WITH]-(f2)
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

POSTGRES_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            WITH user_cohorts AS (
                SELECT 
                    user_id,
                    EXTRACT(YEAR FROM registration_date) as reg_year,
                    registration_date,
                    registration_date + INTERVAL '1 year' as first_year_end
                FROM users
            )
            SELECT 
                c.reg_year as cohort_year,
                EXTRACT(MONTH FROM f.since) as friendship_month,
                COUNT(DISTINCT f.user_id || '-' || f.friend_id) as friendships_count
            FROM user_cohorts c
            JOIN friendships f ON c.user_id = f.user_id 
                AND f.since BETWEEN c.registration_date AND c.first_year_end
            WHERE EXTRACT(YEAR FROM f.since) = c.reg_year
            GROUP BY c.reg_year, EXTRACT(MONTH FROM f.since)
            ORDER BY c.reg_year, friendship_month;
        """,
        "description": "Когортный анализ активности дружбы"
    },
    "social_cities": {
        "query": """
            WITH intercity_friendships AS (
                SELECT DISTINCT
                    LEAST(u1.user_id, u2.user_id) as user1,
                    GREATEST(u1.user_id, u2.user_id) as user2,
                    u1.city as user_city,
                    u2.city as friend_city
                FROM friendships f
                JOIN users u1 ON f.user_id = u1.user_id
                JOIN users u2 ON f.friend_id = u2.user_id
                WHERE u1.city <> u2.city
            )
            SELECT 
                user_city,
                COUNT(*) as intercity_friendships_count,
                COUNT(DISTINCT friend_city) as unique_cities_connected
            FROM intercity_friendships
            GROUP BY user_city
            ORDER BY intercity_friendships_count DESC
            LIMIT 5;
        """,
        "description": "Поиск «социально активных» городов"
    },
    "age_gap_analysis": {
        "query": """
            WITH age_gaps AS (
                SELECT 
                    ABS(u1.age - u2.age) as age_difference,
                    COUNT(*) as friendship_count
                FROM friendships f
                JOIN users u1 ON f.user_id = u1.user_id
                JOIN users u2 ON f.friend_id = u2.user_id
                GROUP BY ABS(u1.age - u2.age)
            )
            SELECT 
                CASE 
                    WHEN age_difference <= 1 THEN '0-1 год'
                    WHEN age_difference <= 5 THEN '2-5 лет'
                    WHEN age_difference <= 10 THEN '6-10 лет'
                    ELSE '>10 лет'
                END as age_gap_category,
                SUM(friendship_count) as friendships_count,
                ROUND(100.0 * SUM(friendship_count) / SUM(SUM(friendship_count)) OVER (), 2) as percentage
            FROM age_gaps
            GROUP BY 
                CASE 
                    WHEN age_difference <= 1 THEN '0-1 год'
                    WHEN age_difference <= 5 THEN '2-5 лет'
                    WHEN age_difference <= 10 THEN '6-10 лет'
                    ELSE '>10 лет'
                END
            ORDER BY MIN(age_difference);
        """,
        "description": "Анализ «возрастного разрыва» в дружбе"
    },
    "network_growth": {
        "query": """
            WITH quarterly_dates AS (
                SELECT generate_series(
                    date_trunc('quarter', CURRENT_DATE - INTERVAL '5 years'),
                    date_trunc('quarter', CURRENT_DATE),
                    '3 months'::interval
                ) as quarter_end
            ),
            quarterly_cumulative AS (
                SELECT 
                    q.quarter_end,
                    COUNT(DISTINCT 
                        LEAST(f.user_id, f.friend_id) || '-' || 
                        GREATEST(f.user_id, f.friend_id)
                    ) as cumulative_friendships
                FROM quarterly_dates q
                CROSS JOIN LATERAL (
                    SELECT f.user_id, f.friend_id
                    FROM friendships f
                    WHERE f.since <= q.quarter_end
                ) f
                GROUP BY q.quarter_end
            )
            SELECT 
                to_char(quarter_end, 'YYYY-Q') as quarter,
                cumulative_friendships,
                cumulative_friendships - LAG(cumulative_friendships, 1, 0) OVER (ORDER BY quarter_end) as new_friendships_this_quarter
            FROM quarterly_cumulative
            ORDER BY quarter_end;
        """,
        "description": "Динамика роста сети (ежеквартальный срез)"
    },
    "age_clustering": {
        "query": """
            WITH user_age_groups AS (
                SELECT 
                    user_id,
                    width_bucket(age, 10, 80, 7) as age_bucket
                FROM users
            ),
            clustering AS (
                SELECT 
                    uag.user_id,
                    uag.age_bucket,
                    COUNT(DISTINCT f1.friend_id) as degree,
                    COUNT(DISTINCT 
                        CASE WHEN EXISTS (
                            SELECT 1 FROM friendships f2 
                            WHERE f2.user_id = f1.friend_id 
                            AND f2.friend_id = f3.friend_id
                        ) THEN f1.friend_id || '-' || f3.friend_id END
                    ) as triangles
                FROM user_age_groups uag
                LEFT JOIN friendships f1 ON uag.user_id = f1.user_id
                LEFT JOIN friendships f3 ON uag.user_id = f3.user_id AND f1.friend_id < f3.friend_id
                GROUP BY uag.user_id, uag.age_bucket
            )
            SELECT 
                age_bucket * 10 + 10 as age_group_start,
                COUNT(*) as users_count,
                AVG(CASE WHEN degree >= 2 THEN triangles * 2.0 / (degree * (degree - 1)) ELSE 0 END) as avg_clustering_coefficient
            FROM clustering
            WHERE degree >= 2
            GROUP BY age_bucket
            ORDER BY age_bucket;
        """,
        "description": "Анализ «плотности» дружбы по возрасту (упрощенный)"
    }
}

NEO4J_ANALYTICAL_QUERIES = {
    "cohort_analysis": {
        "query": """
            MATCH (u:User)
            WITH u, 
                 u.registration_date.year as regYear,
                 u.registration_date as regDate,
                 u.registration_date + duration({years: 1}) as firstYearEnd
            MATCH (u)-[r:FRIENDS_WITH]->(friend)
            WHERE r.since >= regDate AND r.since <= firstYearEnd
              AND r.since.year = regYear
            RETURN regYear as cohortYear,
                   r.since.month as friendshipMonth,
                   count(r) as friendshipsCount
            ORDER BY cohortYear, friendshipMonth;
        """,
        "description": "Когортный анализ активности дружбы"
    },
    "social_cities": {
        "query": """
            MATCH (u1:User)-[r:FRIENDS_WITH]-(u2:User)
            WHERE u1.city <> u2.city
            WITH u1, u2, u1.city as userCity, u2.city as friendCity
            WITH apoc.coll.sort([u1.user_id, u2.user_id]) as sortedIds,
                 userCity, friendCity
            WITH DISTINCT sortedIds, userCity, friendCity
            RETURN userCity,
                   count(*) as intercityFriendshipsCount,
                   count(DISTINCT friendCity) as uniqueCitiesConnected
            ORDER BY intercityFriendshipsCount DESC
            LIMIT 5;
        """,
        "description": "Поиск «социально активных» городов"
    },
    "age_gap_analysis": {
        "query": """
            MATCH (u1:User)-[r:FRIENDS_WITH]-(u2:User)
            WITH abs(u1.age - u2.age) as ageDifference,
                 count(r) as friendshipCount
            WITH ageDifference, friendshipCount
            ORDER BY ageDifference
            WITH collect({ageDiff: ageDifference, count: friendshipCount}) as data
            UNWIND data as d
            WITH d,
                 CASE 
                    WHEN d.ageDiff <= 1 THEN '0-1 год'
                    WHEN d.ageDiff <= 5 THEN '2-5 лет'
                    WHEN d.ageDiff <= 10 THEN '6-10 лет'
                    ELSE '>10 лет'
                 END as category
            WITH category, d.count as cnt
            ORDER BY category
            WITH category, sum(cnt) as friendshipsCount
            WITH collect({cat: category, cnt: friendshipsCount}) as categoryData
            WITH categoryData, 
                 reduce(total = 0, x IN categoryData | total + x.cnt) as totalCount
            UNWIND categoryData as cd
            RETURN cd.cat as category,
                   cd.cnt as friendshipsCount,
                   round(100.0 * cd.cnt / totalCount, 2) as percentage
            ORDER BY 
              CASE cd.cat
                WHEN '0-1 год' THEN 1
                WHEN '2-5 лет' THEN 2
                WHEN '6-10 лет' THEN 3
                WHEN '>10 лет' THEN 4
              END;
        """,
        "description": "Анализ «возрастного разрыва» в дружбе"
    },
    "network_growth": {
        "query": """
            WITH range(0, 19) as quarters
            UNWIND quarters as q
            WITH date.truncate('quarter', date()) - duration({months: 3*q}) as quarterEnd
            MATCH (u1:User)-[r:FRIENDS_WITH]-(u2:User)
            WHERE r.since <= quarterEnd
            WITH quarterEnd, u1, u2
            WITH quarterEnd, 
                 apoc.coll.sort([u1.user_id, u2.user_id]) as pair
            WITH quarterEnd, count(DISTINCT pair) as cumulativeFriendships
            ORDER BY quarterEnd
            WITH collect({quarter: quarterEnd, count: cumulativeFriendships}) as data
            UNWIND range(0, size(data)-1) as idx
            WITH data[idx] as current,
                 CASE WHEN idx > 0 THEN data[idx-1].count ELSE 0 END as prevCount
            RETURN current.quarter.year + '-' + current.quarter.quarter as quarter,
                   current.count as cumulativeFriendships,
                   current.count - prevCount as newFriendshipsThisQuarter
            ORDER BY current.quarter;
        """,
        "description": "Динамика роста сети (ежеквартальный срез)"
    },
    "age_clustering": {
        "query": """
            MATCH (u:User)
            WITH u, 
                 toInteger(floor(u.age / 10) * 10) as ageGroupStart
            MATCH (u)-[:FRIENDS_WITH]-(friend)
            WITH u, ageGroupStart, collect(DISTINCT friend) as neighbors
            WHERE size(neighbors) >= 2
            WITH u, ageGroupStart, neighbors,
                 [n1 IN neighbors | 
                    [n2 IN neighbors WHERE n1 <> n2 AND EXISTS((n1)-[:FRIENDS_WITH]-(n2)) | 1]
                 ] as connections
            WITH u, ageGroupStart, 
                 size(neighbors) as k,
                 reduce(total = 0, x IN connections | total + size(x)) / 2 as triangles
            WITH u, ageGroupStart,
                 CASE 
                    WHEN k >= 2 THEN 2.0 * triangles / (k * (k - 1))
                    ELSE 0 
                 END as clusteringCoefficient
            WITH ageGroupStart,
                 collect(clusteringCoefficient) as coeffs,
                 count(u) as usersCount
            WITH ageGroupStart,
                 usersCount,
                 reduce(s = 0, c in coeffs | s + c) / size(coeffs) as avgClusteringCoefficient,
                 coeffs,
                 size(coeffs) as n
            WITH ageGroupStart,
                 usersCount,
                 avgClusteringCoefficient,
                 apoc.coll.sort(coeffs) as sortedCoeffs,
                 n
            RETURN toString(ageGroupStart) + '-' + toString(ageGroupStart + 9) as ageGroup,
                   usersCount,
                   avgClusteringCoefficient,
                   CASE 
                     WHEN n % 2 = 0 
                     THEN (sortedCoeffs[n/2 - 1] + sortedCoeffs[n/2]) / 2.0
                     ELSE sortedCoeffs[toInteger(floor(n/2))]
                   END as medianClustering
            ORDER BY ageGroupStart;
        """,
        "description": "Анализ «плотности» дружбы по возрасту"
    }
}