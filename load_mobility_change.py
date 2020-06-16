# -*- coding: utf-8 -*-

import credentials
from subprocess import call
from os import remove
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_state = 'covid_19.mobility_state'
csv_file = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv?cachebust=b3ad67084527db32'

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getMaxDate(cursor,table_state):
    cursor.execute(f"SELECT MAX(date) FROM {table_state}")
    maxdate = cursor.fetchall()[0][0]

    return str(maxdate)

def parse_name(state_name):
    name = state_name.replace('State of ','').replace('Federal District','Distrito Federal')
    return name

def parseCSV(csv_file,maxdate):
    import requests
    import csv
    outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
    file_state = 'mobility_state.csv'
    columns_state  = ['date','country_region','state','retail_and_recreation_percent_change_from_baseline','grocery_and_pharmacy_percent_change_from_baseline','parks_percent_change_from_baseline','transit_stations_percent_change_from_baseline','workplaces_percent_change_from_baseline','residential_percent_change_from_baseline']

    with requests.get(csv_file, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.DictReader(lines)
        with open(outdir+file_state,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.DictWriter(ofile, fieldnames=columns_state,restval='', extrasaction='ignore',delimiter=';')
            for row in reader:
                if row['country_region'] == 'Brazil' and row['sub_region_1'] and row['date'] > maxdate:
                    row['state'] = parse_name(row['sub_region_1'])
                    writer.writerow(row)
    return outdir+file_state

def main():
    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    maxdate = getMaxDate(cursor,table_state)
    _file = parseCSV(csv_file,maxdate)

    ## copy
    with open(_file, 'r') as ifile:
        SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
        print("Executing Copy in "+table_state)
        cursor.copy_expert(sql=SQL_STATEMENT % table_state, file=ifile)
        db_conn.commit()

    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_state+'";',shell=True)
    remove(_file)

if __name__=="__main__":
    main()