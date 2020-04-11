
# -*- coding: utf-8 -*-

from datetime import date,datetime,timedelta
import credentials
import csv
from subprocess import call
import requests
from os import remove
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
tablename = 'covid_19.usa_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'states.csv'
current_date = date.today()-timedelta(days=1)

CSV_URL = 'https://covidtracking.com/api/states.csv'

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

query = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_schema = '"+tablename.split('.')[0]+"' AND table_name = '"+tablename.split('.')[1]+"' ORDER BY ordinal_position;" 
cursor.execute(query)
columns = [item[0] for item in cursor.fetchall()]

with requests.get(CSV_URL, stream=True) as r:
    lines = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.DictReader(lines)
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
        for row in reader:
            row['date'] = current_date
            writer.writerow(row)

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