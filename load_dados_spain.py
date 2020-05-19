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
tablename = 'covid_19.spain_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'serie_historica.csv'
current_date = date.today()-timedelta(days=1)
columns  = ['CCAA','FECHA','PCR+','Hospitalizados','UCI','Fallecidos','Recuperados']
found = False

CSV_URL = 'https://cnecovid.isciii.es/covid19/resources/agregados.csv' 

with requests.get(CSV_URL, stream=True) as r:
    lines = (line.decode('latin-1') for line in r.iter_lines())
    reader = csv.DictReader(lines)
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
        for row in reader:
            if row and len(row['CCAA']) == 2 and datetime.strptime(row['FECHA'],'%d/%m/%Y').date() == current_date:
                found = True
                # if row['CCAA'] == 'GA':
                    # row['PCR+'] = row['CASOS']
                # else:
                    # row['PCR+'] = int(row['PCR+']) + int(row['TestAc+'])
                row['FECHA'] = str(datetime.strptime(row['FECHA'], '%d/%m/%Y').date())
                writer.writerow(row)
if not found:
    raise ValueError('No data for ' + str(current_date))

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