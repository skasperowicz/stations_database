import sqlite3
from sqlite3 import Error
import csv

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
       return conn
   except Error as e:
       print(e)

   return conn



def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)
if __name__ == "__main__":

   create_station_sql = """
   -- station table
   CREATE TABLE IF NOT EXISTS stations (
      id integer PRIMARY KEY,
      station_number text NOT NULL,
      latitude FLOAT,
      longitude FLOAT,
      elevation FLOAT,
      name TEXT,
      country TEXT,
      state TEXT
   );
   """

   create_measure_sql = """
   -- zadanie table
   CREATE TABLE IF NOT EXISTS measure (
      id integer PRIMARY KEY,
      stations_id integer,
      date DATE,
      precip FLOAT,
      tobs FLOAT,
      FOREIGN KEY (stations_id) REFERENCES stations (id)
   );
   """

   db_file = "clean.db"

   conn = create_connection(db_file)
   
   if conn is not None:
        execute_sql(conn, create_station_sql)
        execute_sql(conn, create_measure_sql)
        
def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

cur = conn.cursor()
with open("clean_stations.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    for row in csv_reader:
        if row[0] == "station":
            continue
        station_number = row[0]
        latitude = row[1]
        longitude = row[2]
        elevation = row[3]
        name = row[4]
        country = row[5]
        state = row[6]
        cur.execute('''INSERT INTO stations(station_number, latitude, longitude, elevation, name, country, state)
             VALUES(?,?,?,?,?,?,?)''',(station_number, latitude, longitude, elevation, name, country, state))
        conn.commit()



with open("clean_measure.csv") as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=",")
    for row in csv_reader:
        if row[0] == "station":
            continue
        station_id_rows = select_where(conn,"stations", station_number = row[0])
        station_id = station_id_rows[0][0]
        date = row[1]
        precip = row[2]
        tobs = row[3]
        cur.execute('''INSERT INTO measure(stations_id, date, precip, tobs)
             VALUES(?,?,?,?)''',(station_id, date, precip, tobs))
        conn.commit()

#tworzy bazę danych złożoną z dwóch powiązanych tabel

print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())