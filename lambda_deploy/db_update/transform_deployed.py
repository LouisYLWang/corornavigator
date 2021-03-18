import pandas as pd
import logging
import rds_config
import pymysql
import datetime 

# const & util
RDS_HOST  = rds_config.db_host
DB_USERNAME = rds_config.db_username
PASSWORD = rds_config.db_password
DB_NAME = rds_config.db_name
COUNTRIES = ["Australia", "Brazil", "China", "France", "Germany", "India", "Japan", "Mexico", "Singapore", "South Africa", "Korea, Republic of", "Spain", "United Kingdom", "United States", "United Arab Emirates"]
COUNTRIE_CODES = ['AU', 'BR', 'CN', 'FR', 'DE', 'IN', 'JP', 'KR', 'MX', 'SG', 'ZA', 'ES', 'AE', 'GB', 'US']
SEAT_MONTH = 'Actual Global Scheduled Seats by Month - Last 12 months'
FLIGHT_MONTH = 'Actual Global Scheduled Flights by Month - Last 12 months'

def change_date_format(date_str):
    dt = datetime.datetime.strptime(date_str, '%m/%d/%y')
    return datetime.date.strftime(dt, "%Y-%m-%d")

# get transformed data
def transform_country():
    country = pd.read_csv("https://pkgstore.datahub.io/core/country-list/data_csv/data/d7c9d7cfb42cb69f4422dec222dbbaa8/data_csv.csv",  keep_default_na=False)
    country = country[country['Name'].isin(COUNTRIES)]
    country.reset_index(drop=True, inplace=True)
    return country

def transform_state():
    state = pd.read_csv("https://raw.githubusercontent.com/jasonong/List-of-US-States/master/states.csv",  keep_default_na=False)
    return state

# region[string]: "country"/"state"
def get_transformed_vaccination(state, country, region):
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
    data = data[[region_id, "people_fully_vaccinated", "people_vaccinated_per_hundred", "date"]]
    data = data.where(data.notnull(), None)
    data['date']= pd.to_datetime(data['date'], format='%Y-%m-%d', errors='ignore')
    return data 

# dataset[string]: "confirmed"/"deaths"
# region[string]: "US"/"global"
def get_transformed_covid_death_and_confirm(state, country, dataset, region):
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
        data = us_data.drop(columns=us_drop_columns).groupby(['Province_State']).sum()
    elif region == "global":
        global_data = global_data.drop(columns= ["Province/State", "Lat", "Long"])
        global_data = global_data.replace({
                "US": "United States",
                "Korea, South": "Korea, Republic of"})  
        global_data = global_data[global_data['Country/Region'].isin(country["Name"])]
        data = global_data.groupby(['Country/Region']).sum()
    data = data.stack().reset_index()
    data.columns = ["country/state", 
                  "date",
                  "number"]
    data["date"] = data["date"].map(lambda x: change_date_format(x))

    if region == "global":
        index = pd.Index(country["Name"]) 
        data = data[data["country/state"].isin(country["Name"])]
    elif region == "US":
        index = pd.Index(state["State"])
        data = data[data["country/state"].isin(state["State"])]

    data["country/state"] = data["country/state"].map(lambda x: index.get_loc(x) + 1)
    data['date']= pd.to_datetime(data['date'], format='%Y-%m-%d', errors='ignore')
    data[["date", "number"]] = data[["number", "date"]] 
    data.columns = ['country/state', 'number', 'date']
    return data

# datasource[string]: "seat"/"flight"
def get_transformed_flight_and_seat(datasource):
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
    col_name = datasource + "_number"
    df = df[df["country"].isin(country["Name"])]
    country_name = pd.Index(country["Name"])
    df["country_id"] = df["country"].map(lambda x: country_name.get_loc(x) + 1)
    df = df[["country_id", col_name, "date"]]
    df['date']= df.to_datetime(df['date'], format='%Y-%m-%d', errors='ignore')
    return df

def get_transformed_flight_and_seat(datasource, country, url):
    if datasource == "seat":
        title = SEAT_MONTH
        sheet_name = "Output - Seats"
    elif datasource == "flight":
        title = FLIGHT_MONTH
        sheet_name = "Output - Flights"
    data = pd.read_excel(url, sheet_name=sheet_name)        
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
    col_name = datasource + "_number"
    df = df[df["country"].isin(country["Name"])]
    country_name = pd.Index(country["Name"])
    df["country_id"] = df["country"].map(lambda x: country_name.get_loc(x) + 1)
    df = df[["country_id", col_name, "date"]]
    return df

def get_weekday():
    return datetime.datetime.today().weekday()

def get_oag_url():
    url = "https://www.oag.com/hubfs/Coronavirus%20Web%20Page%202020/"
    url += 'coronavirus-tracking-charts-{0}/OAG-WEEKLY-TRACKER-{1}.xlsx'.format(
        datetime.datetime.today().strftime('%d%m%y'),
        datetime.datetime.today().strftime('%d-%b-%Y')
    )
    return url
 
state = transform_state()
country = transform_country()
schema_map = {
    "vaccine_state": get_transformed_vaccination(state, country, "state"),
    "vaccine_country": get_transformed_vaccination(state, country, "country"),
    "covid_death_state": get_transformed_covid_death_and_confirm(state, country, "deaths", "US"),
    "covid_death_country": get_transformed_covid_death_and_confirm(state, country, "deaths", "global"),
    "covid_case_state": get_transformed_covid_death_and_confirm(state, country, "confirmed", "US"),
    "covid_case_country": get_transformed_covid_death_and_confirm(state, country, "confirmed", "global")
}

# on each Monday update OAG data
# if get_weekday() == 0:
#     url = get_oag_url()
#     schema_map['oag_flight_international'] = get_transformed_flight_and_seat("flight", country, url)
#     schema_map['oag_seat_international'] = get_transformed_flight_and_seat("seat", country, url)
    
