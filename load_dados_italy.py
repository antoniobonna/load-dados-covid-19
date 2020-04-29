# -*- coding: utf-8 -*-

import credentials
import csv
from subprocess import call
import requests
from os import remove
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
tablename = 'covid_19.italy_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'italy.csv'
columns  = ['data','codice_regione','denominazione_regione','terapia_intensiva','totale_ospedalizzati','isolamento_domiciliare','dimessi_guariti','deceduti','totale_casi','casi_testati']

CSV_URL = 'https://raw.githubusercontent.com/pcm-dpc/COVID-19/master/dati-regioni/dpc-covid19-ita-regioni-latest.csv'

def parseCSV(url):
    with requests.get(url, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.DictReader(lines)
        with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
            for row in reader:
                if int(row['totale_positivi']) > 0 and row['denominazione_regione'] != 'P.A. Bolzano':
                    row['data'] = row['data'][:10]
                    if row['denominazione_regione'] == 'P.A. Trento':
                        row['denominazione_regione'] = 'Trentino-Alto Adige'
                    writer.writerow(row)

parseCSV(CSV_URL)

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