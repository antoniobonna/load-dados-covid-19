# -*- coding: utf-8 -*-

from datetime import date,datetime
import credentials
from subprocess import call
from os import remove
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_state = 'covid_19.traffic_state'
csv_file = 'http://tiny.cc/idb-traffic-daily' 

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getMaxDate(cursor,table_state):
    cursor.execute(f"SELECT MAX(date) FROM {table_state}")
    maxdate = cursor.fetchall()[0][0]

    return maxdate

def parse_name(state_name):
    name = state_name.replace('Amapa','Amapá').replace('Ceara','Ceará').replace('Goias','Goiás').replace('Maranhao','Maranhão').replace('Para','Pará')
    name = name.replace('Parana','Paraná').replace('Paraiba','Paraíba').replace('Goias','Goiás').replace('Piaui','Piauí').replace('Rondonia','Rondônia')
    name = name.replace('Mato Grosso Do Sul','Mato Grosso do Sul').replace('Rio Grande Do Norte','Rio Grande do Norte').replace('Rio De Janeiro','Rio de Janeiro')
    return name.replace('Sao Paulo','São Paulo').replace('Espirito Santo','Espírito Santo').replace('Rio Grande Do Sul','Rio Grande do Sul')

def parseCSV(csv_file,maxdate):
    from urllib.request import urlopen
    from io import StringIO
    import csv
    outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
    file_state = 'data_state.csv'
    columns_state  = ['date','state','tcp']

    r = urlopen(csv_file).read().decode('utf-8','ignore')
    lines = StringIO(r)
    reader = csv.DictReader(lines)
    with open(outdir+file_state,'w', newline="\n", encoding="utf-8") as ofile:
        writer = csv.DictWriter(ofile, fieldnames=columns_state,restval='', extrasaction='ignore',delimiter=';')
        for row in reader:
            if row['country_name'] == 'Brazil' and row['region_type'] == 'state' and date(2020,int(row['month']),int(row['day'])) > maxdate:
                row['date'] = str(date(2020,int(row['month']),int(row['day'])))
                row['state'] = parse_name(row['region_name'])
                row['tcp'] = float(row['tcp'])/100
                writer.writerow(row)
    return outdir+file_state

def main():
    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    maxdate = getMaxDate(cursor,table_state)
    _file = parseCSV(csv_file,maxdate)

    ### copy
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