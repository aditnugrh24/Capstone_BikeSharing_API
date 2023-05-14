import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/statistics/')
def route_all_statistics():
    conn = make_connection()
    statistics = get_all_stations_statistics(conn)
    return statistics.to_json()

@app.route('/most_used_bike/<bikeid>')
def route_bikes_id(bikeid):
    conn = make_connection()
    bike = get_bike_id(bikeid, conn)
    return bike.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    tripd = get_all_trips(conn)
    return tripd.to_json()


@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result


@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/date', methods=['POST']) 
def route_trips_date():
    input_data = request.get_json(force=True) 
    specified_date = input_data["period"]
    conn = make_connection()
  
    query = f"""SELECT start_station_id as Start_Station, AVG(duration_minutes) as Average_of_Trips, COUNT(id) as Number_of_Trips
    FROM trips  
    WHERE start_time LIKE '{specified_date}%'
    group by start_station_id"""
    selected_data = pd.read_sql_query(query, conn)
    return selected_data.to_json()
 


@app.route('/json', methods = ['POST']) 
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')
    
########Functions dari get station
def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

# Make a connection
conn = make_connection()

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query_trip_1 = """select * from trips"""
    result_1 = pd.read_sql_query(query_trip_1,conn)
    return result_1

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def insert_into_trips(data, conn):
    query_trips = f""" insert into trips values {data}"""
    try:
        conn.execute(query_trips)
    except:
        return 'Trip eror'
    conn.commit()
    return 'Trip ok'


def get_all_stations_statistics(conn):
    query = f""" SELECT station_id, COUNT(start_station_id) as StationStartCount, AVG(duration_minutes) AS Average_Duration
    FROM stations
    LEFT JOIN trips
    ON stations.station_id = trips.start_station_id
    GROUP BY station_id
    ORDER BY StationStartCount DESC
    """
    result = pd.read_sql_query(query, conn)
    return result

def get_bike_id(bikeid, conn):
    query = f""" SELECT  bikeid, COUNT(bikeid) as JumlahPemakaian, SUM(duration_minutes) as Total_Waktu_Pemakaian
    FROM trips 
    WHERE bikeid = {bikeid}"""
    result = pd.read_sql_query(query, conn)
    return result 

if __name__ == '__main__':
    app.run(debug=True, port=5000)