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
tablename = 'covid_19.brazil_stg'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
file = 'cases-brazil-states.csv'
current_date = str(date.today()-timedelta(days=1))
columns  = ['date','state','totalCases','deaths']

#CSV_URL = 'https://raw.githubusercontent.com/wcota/covid19br/master/cases-brazil-states.csv'

def parseCSV(url):
    with requests.get(url, stream=True) as r:
        lines = (line.decode('latin-1') for line in r.iter_lines())
        reader = csv.DictReader(lines)
        with open(outdir+file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
            found = False
            for row in reader:
                if row['date'] == current_date and len(row['state']) == 2:
                    found = True
                    writer.writerow(row)

    if not found:
        raise ValueError('No data for ' + current_date)

def loadData(url):
    parseCSV(url)

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

#loadData(CSV_URL)