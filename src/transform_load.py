import pandas as pd
import sys
import logging
import rds_config
import pymysql
import json
from pathlib import Path
from extract import get_opensky_urls
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
    airport = airport[airport["iso_country"].isin(["US"])]
    airport["state"] = airport["iso_region"].apply(lambda x: x[3:])
    airport = airport[~airport["type"].isin(["heliport", "closed"])]
    airport = airport[~airport['state'].isin(['U-A'])]
    airport = airport[["ident", "name", "coordinates", "state", "iso_country"]]
    state_index = pd.Index(state["Code"])
    airport["state_id"] = airport["state"].map(lambda x: state_index.get_loc(x) + 1 if x is not pd.NA else pd.NA)
    airport.reset_index(drop=True, inplace=True)
    # flight["state_id"] = flight["airport_origin_id"].apply(lambda x: airport["state_id"][x])
    # flight.groupby(["state_id", "date"])
    # flight["month"] = flight["date"].apply()
    return airport

def get_month(date_str):
    dt = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    return datetime.date.strftime(dt, "%Y-%m")

def transform_flight(airport, url):
    # extract data from zenodo, TODO: make extraction automated on lambda function
    # latest url
    # flight = pd.read_csv("https://zenodo.org/record/4485741/files/flightlist_20210101_20210131.csv.gz", compression='gzip', header=0, sep=',', error_bad_lines=False)
    if url[-7:] != '.csv.gz':
        return 
    flight = pd.read_csv(url, compression='gzip', header=0, sep=',', error_bad_lines=False)
    flight['date'] = flight['day'].map(lambda x: x.split()[0])
    flight = flight[["origin", "destination", "date"]]
    flight = flight.dropna(subset=['origin', 'destination'])
    flight = flight[flight["origin"].isin(list(airport["ident"]))]
    flight = flight[flight["destination"].isin(list(airport["ident"]))]
    airport_index = pd.Index(airport["ident"])
    flight["airport_origin_state"] = flight["origin"].map(lambda x: airport['state_id'][airport_index.get_loc(x)])
    flight["airport_destin_state"] = flight["destination"].map(lambda x: airport['state_id'][airport_index.get_loc(x)])
    flight.reset_index(drop=True, inplace=True)
    # flight["state_id"] = flight["airport_destin_id"].apply(lambda x: airport["state_id"][x])
    return flight

def load_us_flight(airport):
    for url in get_opensky_urls():
        if url[-7:] != '.csv.gz':
            return 
        flight = transform_flight(airport, url)
        try:
            conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
        except:
            print("ERROR: Unexpected error: Could not connect to MySQL instance.")

        for col in ["airport_destin_state", "airport_origin_state"]:
            #month = get_month(flight['date'][0])
            aggregate_flight = flight.groupby([col]).count()
            aggregate_flight["month"] = flight['date'][0]
            aggregate_flight["is_origin"] = col == 'airport_origin_state'
            aggregate_flight["is_destination"] = col == 'airport_destin_state'
            aggregate_flight.reset_index(level=0, inplace=True) 
            aggregate_flight = aggregate_flight[[col, 'origin', 'is_origin', 'is_destination', 'month']]
        
            with conn.cursor() as cur:
                aggregate_flight = aggregate_flight.values.tolist()
                cur.executemany("INSERT INTO opensky_flight_state (state_id, flight_number, is_origin, is_destination, date) values (%s, %s, %s, %s, %s)", aggregate_flight)
                conn.commit()

        print("Load {0} finished!".format(url))


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

def load_opensky_flight(airport, url):
    flight = transform_flight(airport, url)
    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")

    with conn.cursor() as cur:
        flight_list = flight[["airport_origin_id", "airport_destin_id", "date"]].values.tolist()
        cur.executemany("INSERT INTO test_opensky_flight_international (departure_airport_id, destination_airport_id, date) values (%s, %s, %s)", flight_list)
        conn.commit()
    print("Load {0} finished!".format(url))

# region[string]: "country"/"state"
def load_vaccination(state, country, region):
    region_id = "{0}_id".format(region)
    table_name = "vaccine_{0}".format(region)

    if region == "state":
        data = pd.read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/us_state_vaccinations.csv")
        data = data[data["location"].isin(state["State"])]
        region_index = pd.Index(state["State"])    
    elif region == "country":
        data = pd.read_csv("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.csv")
        data = data[data["location"].isin(country["Name"])]
        region_index = pd.Index(country["Name"])
    
    data[region_id] = data["location"].map(lambda x: region_index.get_loc(x) + 1)
    today_data = data
    #next line is used for daily update
    #today_data = data[data["date"] == datetime.datetime.now().strftime('%Y-%m-%d')]
    today_data = today_data.where(today_data.notnull(), None)

    if len(today_data) > 1:
        today_data = today_data[[region_id, "people_fully_vaccinated", "people_vaccinated_per_hundred", "date"]]
        value_list = today_data.values.tolist()
        try:
            conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
        except:
            print("ERROR: Unexpected error: Could not connect to MySQL instance.")
            
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO {0} ({1}, number_people_vaccinated, percentage_people_vaccinated, date) values (%s, %s, %s, %s)".format(table_name, region_id), value_list)
            conn.commit()
    print("Load world vaccination finished!")


# dataset[string]: "confirmed"/"deaths"
# region[string]: "US"/"global"
def transform_covid_death_and_confirm(state, country, dataset, region):
    us_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{0}_US.csv".format(dataset)
    global_data_url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_{0}_global.csv".format(dataset)
    us_data = pd.read_csv(us_data_url)
    global_data = pd.read_csv(global_data_url)
    global_drop_columns = ["UID", "code3", "FIPS", "Lat", "Long_"]
    us_drop_columns = ['UID', 'iso2', 'iso3', 'code3', 'FIPS', 'Admin2', 'Country_Region', 'Lat', 'Long_', 'Combined_Key']
    if dataset == "deaths":
        global_drop_columns.append("Population")
        us_drop_columns.append('Population')
    if region == "US":
        us_data = us_data.drop(columns=us_drop_columns).groupby(['Province_State']).sum()
        return us_data
    elif region == "global":
        global_data = global_data.drop(columns= ["Province/State", "Lat", "Long"])
        global_data = global_data.replace({
                "US": "United States",
                "Korea, South": "Korea, Republic of"})  
        global_data = global_data[global_data['Country/Region'].isin(country["Name"])]
        global_data = global_data.groupby(['Country/Region']).sum()
        return global_data
    # data = data[data['Country/Region'].isin(country["Name"])]
    # data.groupby(['Country/Region']).sum()

def change_date_format(date_str):
    dt = datetime.datetime.strptime(date_str, '%m/%d/%y')
    return datetime.date.strftime(dt, "%Y-%m-%d")

def load_covid_death_and_confirm(state, country, dataset, region):
    df = transform_covid_death_and_confirm(state, country, dataset, region)
    df = df.stack().reset_index()
    df.columns = ["country/state", 
                  "date",
                  "number"]
    df["date"] = df["date"].map(lambda x: change_date_format(x))

    if region == "global":
        index = pd.Index(country["Name"]) 
        df = df[df["country/state"].isin(country["Name"])]
    elif region == "US":
        index = pd.Index(state["State"])
        df = df[df["country/state"].isin(state["State"])]

    df["country/state"] = df["country/state"].map(lambda x: index.get_loc(x) + 1)
    value_ls = df.values.tolist()
    # map datasource keywork to sql table name
    keywords_map = {
        "confirmed" : "case",
        "deaths": "death",
        "US": "state",
        "global": "country"
    }

    table_name = "covid_{0}_{1}".format(keywords_map[dataset], keywords_map[region])
    region_name = "{0}_id".format(keywords_map[region])
    value_name = "number_of_{0}s".format(keywords_map[dataset])

    try:
        conn = pymysql.connect(host=RDS_HOST, user=DB_USERNAME, passwd=PASSWORD, database=DB_NAME)
    except:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")
        
    with conn.cursor() as cur:
        cur.executemany("INSERT INTO {0} ({1}, Date, {2}) values (%s, %s, %s)".format(
            table_name,
            region_name,
            value_name
            ), value_ls)
        conn.commit()
    print("Load {0} covid {1} finished!".format(region, dataset))


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
    df = df.replace({"USA": "United States",
                "South Korea": "Korea, Republic of" ,
                "UAE": "United Arab Emirates"})  
    return df

# datasource[string]: "seat"/"flight"
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
    #load_opensky_flight(airport)
    load_us_flight(airport)

    # load_us_state_vaccination(state)
    # load_global_vaccination(country)
    load_internation_flight_and_seat("seat", country)
    load_internation_flight_and_seat("flight", country)

    load_vaccination(state, country, "country")
    load_vaccination(state, country, "state")

    load_covid_death_and_confirm(state, country, "deaths", "US")
    load_covid_death_and_confirm(state, country, "deaths", "global")
    load_covid_death_and_confirm(state, country, "confirmed", "US")
    load_covid_death_and_confirm(state, country, "confirmed", "global")