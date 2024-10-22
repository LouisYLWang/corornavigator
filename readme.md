# Coronavigator

Coronavigator is an end-to-end ETL pipline and a PowerBI dashboard to visualize present-day information on the state of air travels, COVID-19 case and death tolls, and COVID-19 vaccination progresses across the world.

![title pic](./resource/img/coronavigator.png)
![architecture](./resource/img/implementation.png)

## Data Source

### staic date 

[country code](https://datahub.io/core/country-list#data)

[airport code](https://datahub.io/core/airport-codes)

[state code](https://worldpopulationreview.com/states/state-abbreviations)

### covid data sources

[confirmed cases by country](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv)

[confirmed cases by state (US Only)](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_US.csv)

[deaths by country](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv)

[deaths cases by state (US Only)](https://github.com/CSSEGISandData/COVID-19/blob/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_US.csv)

###  vaccination data sources

[vaccination data sources by country](https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/vaccinations.csv)

[vaccination data sources by states (US Only)](https://github.com/owid/covid-19-data/blob/master/public/data/vaccinations/us_state_vaccinations.csv)

### flight data sources 

[OAG - seats by country](https://www.oag.com/coronavirus-airline-schedules-data)

[OAG - flights by country](https://www.oag.com/coronavirus-airline-schedules-data)

[OpenSky - flights by airport](https://zenodo.org/record/4485741) (Highly biased data, may only be used for flight data of US)

## Demo


<iframe width="100%" height="500" src="https://app.powerbi.com/view?r=eyJrIjoiMGYwY2RjODMtNmFiOS00Mzg2LWEwNGEtN2Y1ZjIwZDZmMWZmIiwidCI6ImY2YjZkZDViLWYwMmYtNDQxYS05OWEwLTE2MmFjNTA2MGJkMiIsImMiOjZ9&pageName=ReportSection826307d1c03c8628139b" frameborder="0" allowFullScreen="true"></iframe>
