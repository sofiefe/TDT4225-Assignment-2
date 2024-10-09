from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
import os

class Task1:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    def create_table(self, table_name, query):
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    # def insert_data(self, table_name, data):
    #     for dp in data:
    #         # Take note that the name is wrapped in '' --> '%s' because it is a string,
    #         # while an int would be %s etc
    #         query = "INSERT INTO %s (data) VALUES ('%s')"
    #         self.cursor.execute(query % (table_name, data))
    #     self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        self.db_connection.commit()
        print(tabulate(rows, headers=self.cursor.column_names))

#------------------------------------------------------------------------------------------------------

    def describe_table(self, table_name):
        query = "DESCRIBE %s"
        self.cursor.execute(query % table_name)
        content = self.cursor.fetchall()
        print(tabulate(content))

    def fill_user_table(self):
        for user in range(182):
            has_labels = 'false'
            user_id = "00"+str(user)
            user_id = user_id[-3:]
            query_create_user = """INSERT INTO User (id, has_labels) VALUES ('%s', %s);"""
            data = (user_id, has_labels)
            self.cursor.execute(query_create_user % data)
            self.db_connection.commit()
        
        with open(r"dataset/dataset/labeled_ids.txt", 'r') as f:
            lines = f.readlines()
            for row in lines:
                # check if string present on a current line
                query = """UPDATE User SET 
                        has_labels = true
                        WHERE 
                            id = %s"""
                self.cursor.execute(query % row)
                self.db_connection.commit()

    def insert_into_activity(self, user_id, transportation_mode, start_date_time, end_date_time):
        query = """
        INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
        VALUES ('%s', '%s', '%s', '%s')
        """
        data = (user_id, transportation_mode, start_date_time, end_date_time)
        self.cursor.execute(query % data)
        self.db_connection.commit()
        return self.cursor.lastrowid 
    

    def insert_trackpoints_batch(self, trackpoints):
        query = """
        INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        self.cursor.executemany(query, trackpoints)
        self.db_connection.commit()


    def read_labels_file(self, labels_file):
        labels = []
        with open(labels_file, 'r') as file:
            next(file)  # Skip the header line
            for line in file:
                start_time, end_time, mode = line.strip().split('\t')
                start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')  
                end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')  
                labels.append((start_time, end_time, mode))
        return labels
    
    # Function to process each .plt file (read data and insert into the database)
    def process_plt_file(self, file_path, user_id, labels):
        with open(file_path, 'r') as file:
            lines = file.readlines()[6:]  # Skip the first 6 lines (header)
            if not lines or len(lines) > 2500:  # Skip if trackpoints > 2500
                return

            # Extract start and end times from the first and last rows
            start_line = lines[0].strip().split(',')
            end_line = lines[-1].strip().split(',')

            start_date_time = datetime.strptime(f"{start_line[5]} {start_line[6]}", '%Y-%m-%d %H:%M:%S')
            end_date_time = datetime.strptime(f"{end_line[5]} {end_line[6]}", '%Y-%m-%d %H:%M:%S')

            # Try to match with the transportation mode in labels.txt
            matched_label = None
            for label in labels:
                if label[0] == start_date_time and label[1] == end_date_time:
                    matched_label = label[2]
                    break

            if matched_label is None:
                return  # No match found, skip this file

            # Insert into the activity table
            activity_id = self.insert_into_activity(user_id, matched_label, start_date_time, end_date_time)

            # Collect trackpoints
            trackpoints = []
            for line in lines:
                lat, lon, _, altitude, date_days, date, time = line.strip().split(',')
                date_time = datetime.strptime(f"{date} {time}", '%Y-%m-%d %H:%M:%S')
                trackpoints.append((activity_id, float(lat), float(lon), int(float(altitude)), float(date_days), date_time))

            # Insert trackpoints in batches
            self.insert_trackpoints_batch(trackpoints)
    
    def process_geolife_dataset(self, dataset_path):
        for user_id in os.listdir(dataset_path):
            user_folder = os.path.join(dataset_path, user_id, 'Trajectory')
            labels_file = os.path.join(dataset_path, user_id, 'labels.txt')

            # Read labels.txt file 
            labels = self.read_labels_file(labels_file) if os.path.exists(labels_file) else []

            # Process all .plt files for this user
            if os.path.isdir(user_folder):
                for plt_file in os.listdir(user_folder):
                    if plt_file.endswith('.plt'):
                        file_path = os.path.join(user_folder, plt_file)
                        self.process_plt_file(file_path, user_id, labels)
        

    def show_top_rows(self):
        tables = ['User', 'Activity', 'TrackPoint']
        for table in tables:
            query = f"SELECT * FROM {table} LIMIT 10;"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            print(f"Top 10 rows from {table}:")
            print(tabulate(rows, headers=self.cursor.column_names))
            print()  # Add a newline for better readability

query_trackpoint = """CREATE TABLE IF NOT EXISTS %s 
                    (id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    activity_id INT,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_days DOUBLE,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE)
                    """
    
query_activity = """CREATE TABLE IF NOT EXISTS %s (
                    id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(3),
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE)
                    """

query_user = """CREATE TABLE IF NOT EXISTS %s (
                id VARCHAR(3) NOT NULL PRIMARY KEY,
                has_labels BOOLEAN)
                """

def main():
    program = None
    try:
        program = Task1()
        program.create_table(table_name="User", query=query_user)
        program.create_table(table_name="Activity", query=query_activity)
        program.create_table(table_name="TrackPoint", query=query_trackpoint)
        program.show_tables()
        program.describe_table(table_name="User")
        program.describe_table(table_name="Activity")
        program.describe_table(table_name="TrackPoint")
        program.fill_user_table()
        program.fetch_data(table_name="User")
        program.process_geolife_dataset(dataset_path="dataset/dataset/Data")
        program.show_top_rows()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
