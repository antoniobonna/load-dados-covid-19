# -*- coding: utf-8 -*-

import credentials
import csv
from subprocess import call
import requests
from os import remove
import psycopg2
from datetime import date,datetime,timedelta
#import pandas as pd

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
tablename = 'covid_19.usa_cities_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'us_cities.csv'
columns  = ['date','FIPS','Admin2','Province_State','Confirmed','Deaths','Recovered']
current_date = str(date.today()-timedelta(days=1))
csv_date = (date.today()-timedelta(days=1)).strftime('%m-%d-%Y')+'.csv'

CSV_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/'

def parseCSV(url,current_date):
    with requests.get(url, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.DictReader(lines)
        with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
            for row in reader:
                if row['FIPS'] and row['Admin2'] and int(row['Confirmed']) > 0 and 'county' not in row['Admin2'].lower() and row['Admin2'] != 'Unassigned' and not row['Admin2'].startswith('Out'):
                    row['date'] = current_date
                    writer.writerow(row)

# dates = pd.date_range(start="2020-03-23",end="2020-04-24")
# for data in dates:
    # csv_date = data.strftime('%m-%d-%Y')+'.csv'
    # current_date = str(data)[:10]
    # print(csv_date)
parseCSV(CSV_URL+csv_date,current_date)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

## copy
with open(outdir+file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()

cursor.close()
db_conn.close()
remove(outdir+file)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)