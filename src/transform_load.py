import pandas as pd
import sys
import logging
import rds_config
import pymysql
import json
from pathlib import Path
import os
import numpy as np
import datetime 

# const 
RDS_HOST  = rds_config.db_host
DB_USERNAME = rds_config.db_username
PASSWORD = rds_config.db_password
DB_NAME = rds_config.db_name
PORT = 3306
DATASET_DIR = os.path.join(Path(__file__).parent.parent, "datasets")
COUNTRIES = ["Australia", "Brazil", "China", "France", "Germany", "India", "Japan", "Mexico", "Singapore", "South Africa", "Korea, Republic of", "Spain", "United Kingdom", "United States", "United Arab Emirates"]
COUNTRIE_CODES = ['AU', 'BR', 'CN', 'FR', 'DE', 'IN', 'JP', 'KR', 'MX', 'SG', 'ZA', 'ES', 'AE', 'GB', 'US']


# static dataset preprocessing
# transform country 
def transform_country():
    country = pd.read_csv(os.path.join(DATASET_DIR, "country.csv"),  keep_default_na=False)
    country = country[country['Name'].isin(COUNTRIES)]
    return country

# transform state  
def transform_state():
    state = pd.read_csv(os.path.join(DATASET_DIR,"us_state.csv"),  keep_default_na=False)
    return state

# transform airport 
def transform_airport(country, state):
    airport = pd.read_csv(os.path.join(DATASET_DIR, "airport.csv"),  keep_default_na=False)    
    airport["state"] = airport["iso_region"].apply(lambda x: x[3:] if x[:2] == "US" else pd.NA)
    airport = airport[airport["iso_country"].isin(COUNTRIE_CODES)]
    airport = airport[~airport["type"].isin(["heliport", "closed"])]
    airport = airport[["ident", "name", "coordinates", "state", "iso_country"]]
    airport = airport[~airport['state'].isin(['U-A'])]
    county_index = pd.Index(country["Code"])
    state_index = pd.Index(state["Code"])
    airport["country_id"] = airport["iso_country"].map(lambda x: county_index.get_loc(x) + 1)
    airport["state_id"] = airport["state"].map(lambda x: state_index.get_loc(x) + 1 if x is not pd.NA else pd.NA)
    airport.reset_index(drop=True, inplace=True)
    return airport

def transform_flight(airport):
    # extract data from zenodo, TODO: make extraction automated on lambda function
    flight = pd.read_csv("https://zenodo.org/record/4485741/files/flightlist_20210101_20210131.csv.gz", compression='gzip', header=0, sep=',', error_bad_lines=False)
    flight['date'] = flight['day'].map(lambda x: x.split()[0])
    flight = flight[["origin", "destination", "date"]]
    flight = flight.dropna(subset=['origin', 'destination'])
    flight = flight[flight["origin"].isin(list(airport["ident"]))]
    flight = flight[flight["destination"].isin(list(airport["ident"]))]
    airport_index = pd.Index(airport["ident"])
    flight["airport_origin_id"] = flight["origin"].map(lambda x: airport_index.get_loc(x) + 1)
    flight["airport_destin_id"] = flight["destination"].map(lambda x: airport_index.get_loc(x) + 1)
    flight.reset_index(drop=True, inplace=True)
    return flight

# load country 
def load_country():
    country = transform_country()
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in country.iterrows():
            cur.execute("INSERT INTO country (country_name, country_code) values(%s,%s)", 
            [row.Name, row.Code])
        conn.commit()
    print("Load country finished!") 

# load states 
def load_us_state():
    state = transform_state()
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        for index, row in state.iterrows():
            cur.execute("INSERT INTO us_state (us_state_name, us_state_code) values(%s,%s)", 
            [row.State, row.Code])
        conn.commit()
    print("Load us_state finished!") 

# load airport 
def load_airport(country, state):
    airport = transform_airport(country, state)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        foreign_airport = airport[["ident", "name", "country_id"]][airport.state_id.isna()].values.tolist()
        us_airport = airport[["ident", "name", "country_id", "state_id"]][~airport.state_id.isna()].values.tolist()
        cur.executemany("INSERT INTO airport (airport_code, airport_name, country_id) values (%s,%s,%s)", foreign_airport)
        cur.executemany("INSERT INTO airport (airport_code, airport_name, country_id, state_id) values (%s,%s,%s,%s)", us_airport)
        conn.commit()
    print("Load airport finished!") 

def load_flight(airport):
    flight = transform_flight(airport)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        flight_list = flight[["airport_origin_id", "airport_destin_id", "date"]].values.tolist()
        cur.executemany("INSERT INTO flight_international (departure_airport_id, destination_airport_id, date) values (%s, %s, %s)", flight_list)
        conn.commit()
    print("Load interational flight finished!")

def load_us_state_vaccination(state):
    data = pd.read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv")
    data = data[data["location"].isin(state["State"])]
    state_name = pd.Index(state["State"])
    data["state_id"] = data["location"].map(lambda x: state_name.get_loc(x) + 1)
    today_data = data
    #next line is used for daily update
    #today_data = data[data["date"] == datetime.datetime.now().strftime('%Y-%m-%d')]
    today_data = today_data.where(today_data.notnull(), None)
    if len(today_data) > 1:
        today_data = today_data[["state_id", "people_fully_vaccinated", "people_vaccinated_per_hundred", "date"]]
        us_vac_list = today_data.values.tolist()
        try:
            conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
        except:
            print("ERROR: Unexpected error: Could not connect to MySQL instance.")
            
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO vaccine_us (us_state_id, number_people_vaccinated, percentage_people_vaccinated, date) values (%s, %s, %s, %s)", us_vac_list)
            conn.commit()
    print("Load us state vaccination finished!")

def load_world_vaccination(country):
    data = pd.read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv")
    data = data[data["location"].isin(country["Name"])]
    country_name = pd.Index(country["Name"])
    data["country_id"] = data["location"].map(lambda x: country_name.get_loc(x) + 1)
    today_data = data
    #next line is used for daily update
    #today_data = data[data["date"] == datetime.datetime.now().strftime('%Y-%m-%d')]
    today_data = today_data.where(today_data.notnull(), None)
    if len(today_data) > 1:
        today_data = today_data[["country_id", "people_fully_vaccinated", "people_vaccinated_per_hundred", "date"]]
        world_vac_list = today_data.values.tolist()
        try:
            conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
        except:
            print("ERROR: Unexpected error: Could not connect to MySQL instance.")
            
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO vaccine_world (country_id, number_people_vaccinated, percentage_people_vaccinated, date) values (%s, %s, %s, %s)", world_vac_list)
            conn.commit()
    print("Load world vaccination finished!")

SEAT_MONTH = 'Actual Global Scheduled Seats by Month - Last 12 months'
FLIGHT_MONTH = 'Actual Global Scheduled Flights by Month - Last 12 months'
#SEAT_WEEK = 'Actual Global Scheduled Seats by week - Last 8 weeks'
#FLIGHT_WEEK= 'Actual Global Scheduled Flights by Week - Last 8 weeks'

def transform_flight_and_seat(datasource):
    if datasource == "seat":
        title = SEAT_MONTH
        sheet_name = " Seats"
    elif datasource == "flight":
        title = FLIGHT_MONTH
        sheet_name = "Flights"
    data = pd.read_excel(
        "https://f.hubspotusercontent30.net/hubfs/490937/Coronavirus%20Web%20Page%202020/coronavirus-tracking-charts-220221/OAG-WEEKLY-TRACKER-22-February-2021.xlsx",
        sheet_name=sheet_name
    )        
    start = data[data.iloc[:, 0] == title].index[0] + 1
    data = data.iloc[start: start + 17, :]
    df = pd.DataFrame(data.iloc[2:, 1:13])
    df.columns = list(data.iloc[0, :13][1:13])
    df.index = list(data.iloc[:, 0])[2:]
    df = df.stack().reset_index()
    df.columns = ["country", "date", datasource + "_number"]
    df.replace({"USA": "United States",
                "South Korea": "Korea, Republic of" ,
                "UAE": "United Arab Emirates"})  
    return df

# datasource[string], can be "seat"/"flight"
def load_internation_flight_and_seat(datasource, country):
    col_name = datasource + "_number"
    data = transform_flight_and_seat(datasource)
    data = data[data["country"].isin(country["Name"])]
    country_name = pd.Index(country["Name"])
    data["country_id"] = data["country"].map(lambda x: country_name.get_loc(x) + 1)
    today_data = data
    today_data = today_data[["country_id", col_name, "date"]]
    data_ls = today_data.values.tolist()
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO oag_{0}_international (country_id, {1}, date) values (%s, %s, %s)".format(datasource, col_name), data_ls)
        conn.commit()
    print("Load internation {0} finished!".format(datasource))

if __name__ == "__main__": 
    country = transform_country()
    state = transform_state()
    airport = transform_airport(country, state)
    load_airport(country, state)
    load_flight(airport)
    load_us_state_vaccination(state)
    load_world_vaccination(country)
    load_internation_flight_and_seat("seat", country)
    load_internation_flight_and_seat("flight", country)