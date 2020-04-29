# -*- coding: utf-8 -*-

import credentials
import csv
from subprocess import call
import requests
from os import remove
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
tablename = 'covid_19.brazil_cities_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'brazil_cities.csv'
columns  = ['date','city_ibge_code','state','city','confirmed','deaths','estimated_population_2019']

CSV_URL = 'http://brasil.io/dataset/covid19/caso?format=csv'

with requests.get(CSV_URL, stream=True) as r:
    lines = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.DictReader(lines)
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
        for row in reader:
            if row['city_ibge_code'] and row['place_type'] == 'city' and row['is_last'] == 'True':
                writer.writerow(row)

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