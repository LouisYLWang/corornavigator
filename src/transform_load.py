import pandas as pd
import sys
import logging
import rds_config
import pymysql
import json
from pathlib import Path
import os

# const 
RDS_HOST  = rds_config.db_host
DB_USERNAME = rds_config.db_username
PASSWORD = rds_config.db_password
DB_NAME = rds_config.db_name
PORT = 3306
DATASET_DIR = os.path.join(Path(__file__).parent.parent, "datasets")
COUNTRIES = ["Australia", "Brazil", "China", "France", "Germany", "India", "Japan", "Mexico", "Singapore", "South Africa", "Korea, Republic of", "Spain", "United Kingdom", "United States", "United Arab Emirates"]
COUNTRIE_CODES = ['AU', 'BR', 'CN', 'FR', 'DE', 'IN', 'JP', 'KR', 'MX', 'SG', 'ZA', 'ES', 'AE', 'GB', 'US']


# load country 
def load_country():
    country = pd.read_csv(os.path.join(DATASET_DIR, "country.csv"),  keep_default_na=False)
    country = country[country['Name'].isin(COUNTRIES)]
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in country.iterrows():
            cur.execute("INSERT INTO country (country_name, country_code) values(%s,%s)", 
            [row.Name, row.Code])
        conn.commit()
    return country

# load states 
def load_us_state():
    state = pd.read_csv(os.path.join(DATASET_DIR,"us_state.csv"),  keep_default_na=False)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in state.iterrows():
            cur.execute("INSERT INTO us_state (us_state_name, us_state_code) values(%s,%s)", 
            [row.State, row.Code])
        conn.commit()
    return state

# load airport 
def load_airport():
    airport = pd.read_csv(os.path.join(DATASET_DIR, "airport.csv"),  keep_default_na=False)    
    airport["state"] = airport["iso_region"].apply(lambda x: x[3:] if x[:2] == "US" else pd.NA)
    airport = airport[airport["iso_country"].isin(COUNTRIE_CODES)]
    airport = airport[~airport["type"].isin(["heliport", "closed"])]
    airport = airport[["ident", "name", "coordinates", "state", "iso_country"]]
    airport.reset_index(drop=True, inplace=True)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in airport.iterrows():
            if index%1000 == 0: print(f'load finished {index/len(airport)*100: .2f}%')
            cur.execute("INSERT INTO airport " + 
            "SET airport_name = %s," + 
            "airport_code = %s," + 
            "country_id = (SELECT country_id FROM country WHERE country_code = %s)," + 
            "state_id = (SELECT us_state_id FROM us_state WHERE us_state_code = %s)",
            [row[1], row.ident, row.iso_country, row.state])
        conn.commit()
    print("Load airports finished!")
    return airport

def load_flight():
    flight = pd.read_csv(os.path.join(DATASET_DIR,'flightlist_20210101_20210131.csv.gz'), compression='gzip', header=0, sep=',', error_bad_lines=False)
    flight['date'] = flight['day'].map(lambda x: x.split()[0])
    flight = flight[["origin", "destination", "date"]]
    flight = flight.dropna(subset=['origin', 'destination'])
    flight = flight[flight["origin"].isin(list(airport["ident"]))]
    flight = flight[flight["destination"].isin(list(airport["ident"]))]
    airport_index = pd.Index(airport["ident"])
    flight["airport_origin_id"] = flight["origin"].map(lambda x: airport_index.get_loc(x))
    flight["airport_destin_id"] = flight["destination"].map(lambda x: airport_index.get_loc(x))
    flight.reset_index(drop=True, inplace=True)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in flight.iterrows():
            if index%1000 == 0: print(f'load finished {index/len(flight)*100: .2f}%')
            cur.execute("INSERT INTO flight_international " + 
            "SET departure_airport_id = %s," + 
            "destination_airport_id = %s," + 
            "date = %s", 
            [row.airport_origin_id, row.airport_destin_id, row.date])
        conn.commit()
    print("Load interational flight finished!")


if __name__ == "__main__": 
    country = load_country()
    state = load_us_state()
    airport = load_airport()
    load_flight()