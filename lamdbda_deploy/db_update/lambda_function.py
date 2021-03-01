import sys
import logging
import rds_config
import pymysql
import json
import pandas as pd
from transform_deployed import schema_map
#rds settings
#test
rds_host  = rds_config.db_host
name = rds_config.db_username
password = rds_config.db_password
db_name = rds_config.db_name
port = 3306
logger = logging.getLogger()
logger.setLevel(logging.INFO)

try:
    conn = pymysql.connect(host=rds_host, user=name, password=password, database=db_name)
except:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")
def lambda_handler(event, context):
    """
    This function fetches content from MySQL RDS instance
    """
    item_count = 0
    update_info = ""
    counter = 0
    for schema in schema_map:
        update_count = update_schema(schema)
        if update_count != 0:
            counter += 1
        update_info += "schema {0} update {1} queries\n".format(schema, update_count)
    
    return {
        'statusCode': 200,
        'body': json.dumps("Updated {0} schemas: \n{1}".format(counter, update_info))
    }

def get_update_date(schema):
    with conn.cursor() as cur:
        cur.execute("SELECT Date FROM {0} ORDER BY Date DESC LIMIT 1".format(schema))
        for line in cur:
            last_update_date = line[0]
            break
    return last_update_date

def get_load_query(schema):
    query = "INSERT INTO {0} (".format(schema)
    counter = 0
    with conn.cursor() as cur:
        cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{0}'".format(schema))
        for line in cur:
            if counter != 0: 
                query += line[0] + ', '
            counter += 1
    query = query[:-2] 
    query += ") values (" + "%s, " * (counter - 1) 
    query = query[:-2] + ')'
    return query

def update_schema(schema):
    query = get_load_query(schema)
    dataset = schema_map[schema]
    update_date = get_update_date(schema)
    dataset = dataset[dataset['date'] > str(update_date)]
    dataset['date'] = dataset['date'].astype(str)
    data_ls = dataset.values.tolist()
    if dataset.shape[0] == 0:
        logger.error("INFO: No new data on schema {0}.".format(schema))
    else:
        latest_date = max(dataset['date'])
        with conn.cursor() as cur:
           cur.executemany(query, data_ls)
           conn.commit()
        logger.error("INFO: Update {0} queries on schema {1}, updated to {2}.".format(dataset.shape[0], schema, latest_date))
    return dataset.shape[0]