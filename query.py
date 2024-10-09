class Query:
    def __init__(self, id, query, description):
        self.id = id
        self.query = query
        self.description = description


# Dictionary of queries
queries = {
    "query1": Query(
        id="1",
        query="""SELECT 
                    (SELECT COUNT(*) FROM User) AS total_users,
                    (SELECT COUNT(*) FROM Activity) AS total_activities,
                    (SELECT COUNT(*) FROM TrackPoint) AS total_trackpoints;
                """,
        description="How many users, activities, and trackpoints are in the dataset."
    ),
    "query2": Query(
        id="2",
        query="""SELECT 
                    AVG(activity_count) AS average_activities_per_user
                    FROM (
                        SELECT user_id, COUNT(*) AS activity_count
                        FROM Activity
                        GROUP BY user_id
                    ) AS user_activity_counts;
                """,
        description="Find the average number of activities per user."
    ),
    "query3": Query(
        id="3",
        query="""SELECT 
                    user_id, COUNT(*) AS activity_count
                FROM 
                    Activity
                GROUP BY 
                    user_id
                ORDER BY 
                    activity_count DESC
                LIMIT 20;
                """,
        description="Find the top 20 users with the highest number of activities."
    ),
    "query4": Query(
        id="4",
        query="""SELECT DISTINCT 
                    user_id
                FROM 
                    Activity
                WHERE 
                    transportation_mode = 'taxi';
                """,
        description="Find all users who have taken a taxi."
    ),
    "query5": Query(
        id="5",
        query="""SELECT 
                    transportation_mode, COUNT(*) AS activity_count
                FROM 
                    Activity
                WHERE 
                    transportation_mode IS NOT NULL
                GROUP BY 
                    transportation_mode;
                """,
        description="Find all types of transportation modes and count how many activities are tagged with these modes."
    ),
    "query6a": Query(
        id="6a",
        query="""SELECT 
                    YEAR(start_date_time) AS activity_year, COUNT(*) AS activity_count
                FROM 
                    Activity
                GROUP BY 
                    activity_year
                ORDER BY 
                    activity_count DESC
                LIMIT 3;
                """,
        description="Find the year with the most activities."
    ),
    "query6b": Query(
        id="6b",
        query="""SELECT 
                    YEAR(start_date_time) AS activity_year, 
                    SUM(TIMESTAMPDIFF(SECOND, start_date_time, end_date_time)) / 3600 AS total_recorded_hours
                FROM 
                    Activity
                GROUP BY 
                    activity_year
                ORDER BY 
                    total_recorded_hours DESC
                LIMIT 3;
                """,
        description="Check if the year with the most activities is also the year with the most recorded hours."
    ),
    "query8": Query(
        id="8",
        query="""SELECT user_id, SUM(altitude_gained) AS total_altitude
                FROM (
                    SELECT tp1.activity_id, 
                        tp1.altitude - tp2.altitude AS altitude_gained,
                        a.user_id
                    FROM TrackPoint tp1
                    JOIN TrackPoint tp2 ON tp1.activity_id = tp2.activity_id AND tp1.id = tp2.id + 1
                    JOIN Activity a ON tp1.activity_id = a.id
                    WHERE tp1.altitude > tp2.altitude
                ) AS gained_altitudes
                GROUP BY user_id
                ORDER BY total_altitude DESC
                LIMIT 20;
                """,
        description="Find the top 20 users who have gained the most altitude meters."
    ),
    "query9": Query(
        id="9",
        query="""SELECT u.id AS user_id, COUNT(DISTINCT a.id) AS invalid_activity_count
                FROM User u
                JOIN Activity a ON u.id = a.user_id
                JOIN TrackPoint tp1 ON a.id = tp1.activity_id
                JOIN TrackPoint tp2 ON a.id = tp2.activity_id
                WHERE tp1.id < tp2.id 
                AND tp2.id = tp1.id + 1
                AND TIMESTAMPDIFF(SECOND, tp1.date_time, tp2.date_time) >= 300
                GROUP BY u.id
                ORDER BY invalid_activity_count DESC;
                """,
        description="Find all users who have invalid activities (with consecutive trackpoints where the timestamps deviate by at least 5 minutes)."
    ),
    "query10": Query(
        id="10",
        query="""SELECT DISTINCT a.user_id
                FROM TrackPoint tp
                JOIN Activity a ON tp.activity_id = a.id
                WHERE ABS(tp.lat - 39.916) < 0.01 AND ABS(tp.lon - 116.397) < 0.01;
                """,
        description="Find users who have tracked an activity in the Forbidden City of Beijing."
    ),
    "query11": Query(
        id="11",
        query="""SELECT user_id, transportation_mode
                FROM (
                    SELECT user_id, transportation_mode,
                        COUNT(*) AS mode_count
                    FROM Activity
                    WHERE transportation_mode IS NOT NULL
                    GROUP BY user_id, transportation_mode
                ) AS mode_counts
                WHERE (user_id, mode_count) IN (
                    SELECT user_id, MAX(mode_count)
                    FROM (
                        SELECT user_id, transportation_mode,
                            COUNT(*) AS mode_count
                        FROM Activity
                        WHERE transportation_mode IS NOT NULL
                        GROUP BY user_id, transportation_mode
                    ) AS inner_counts
                    GROUP BY user_id
                )
                ORDER BY user_id;
                """,
        description="Find all users who have registered a transportation mode and their most used transportation mode."
    ),
}

