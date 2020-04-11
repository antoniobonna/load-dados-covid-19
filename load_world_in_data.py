
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
tablename = 'covid_19.world_in_data'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'full_data.csv'
current_date = str(date.today())

CSV_URL = 'https://covid.ourworldindata.org/data/ecdc/full_data.csv'

with requests.get(CSV_URL, stream=True) as r:
    lines = (line.decode('utf-8') for line in r.iter_lines())
    reader = csv.reader(lines)
    header = next(reader, None)  ### Pula o cabeçalho
    with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.writer(ofile, delimiter=';')
        found = False
        for row in reader:
            if row[0] == current_date and row[1] != 'World':
                found = True
                row[1] = row[1].replace('Cape Verde','Cabo Verde').replace("Cote d'Ivoire",'Ivory Coast').replace('Sint Maarten (Dutch part)','Saint Martin').replace('Timor','Timor-Leste').replace('Curacao','Curaçao')
                writer.writerow(row)

if not found:
    raise ValueError('No data for ' + current_date)

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