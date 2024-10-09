from DbConnector import DbConnector
from tabulate import tabulate
from haversine import haversine
from query import queries

class Task2:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_query(self, query):
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        print(tabulate(result, headers=self.cursor.column_names))
        print()
        self.db_connection.commit()
    
    def execute_queries(self, queries):
        for key, q in queries.items():
            print(f"Question {q.id}: {q.description}")
            self.execute_query(q.query)


    def calculate_distance_walked_by_user_query_7(self, user_id):
        # Fetch trackpoints for the specific user in 2008
        query = """
        SELECT lat, lon, date_time 
        FROM TrackPoint 
        JOIN Activity ON TrackPoint.activity_id = Activity.id 
        WHERE Activity.user_id = %s AND YEAR(TrackPoint.date_time) = 2008 
        ORDER BY TrackPoint.date_time
        """
        self.cursor.execute(query, (user_id,))
        trackpoints = self.cursor.fetchall()
        
        total_distance = 0.0
        
        # Loop through the trackpoints and calculate distances
        for i in range(1, len(trackpoints)):
            point1 = (trackpoints[i-1][0], trackpoints[i-1][1])  # (lat, lon)
            point2 = (trackpoints[i][0], trackpoints[i][1])
            # Calculate distance between point1 and point2
            distance = haversine(point1, point2)
            total_distance += distance
            
        return total_distance


def main():
    program = None
    try:
        program = Task2()
        distance_walked = program.calculate_distance_walked_by_user_query_7(user_id='112')
        print(f'Question 7: Total distance walked by user 112 in 2008: {distance_walked:.2f} km')
        print()
        program.execute_queries(queries)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
