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

CSV_URL = 'https://covid19.isciii.es/resources/serie_historica_acumulados.csv'

with requests.get(CSV_URL, stream=True) as r:
    lines = (line.decode('latin-1') for line in r.iter_lines())
    reader = csv.reader(lines)
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for row in reader:
            if row and len(row[0]) == 2 and datetime.strptime(row[1],'%d/%m/%Y').date() == current_date:
                row[1] = str(datetime.strptime(row[1], '%d/%m/%Y').date())
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