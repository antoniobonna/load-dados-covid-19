# -*- coding: utf-8 -*-

from datetime import datetime, date
import credentials
import psycopg2
from subprocess import call
import requests
import re
from os import remove

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_name = 'covid_19.sus_beds_supplies'
url = 'https://covid-insumos.saude.gov.br/paineis/insumos/graficos/uti_sus.php'

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(url):
    response = requests.get(url)
    match = re.search(r'\d{2}\/\d{2}\/\d{4}',response.text).group()
    last_date = datetime.strptime(match, '%d/%m/%Y').date()

    return last_date

def getMaxDate(cursor,table_name):
    cursor.execute(f"SELECT MAX(date) FROM {table_name}")
    maxdate = cursor.fetchall()[0][0]

    return maxdate

def parseCSV(last_date):
    import csv
    CSV_URL = 'https://covid-insumos.saude.gov.br/paineis/insumos/lista_csv_painel.php?output=csv'
    outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
    _file = 'lista_insumos.csv'

    with requests.get(CSV_URL, stream=True) as r:
        lines = (line.decode('utf-8') for line in r.iter_lines())
        reader = csv.reader(lines,delimiter=';')
        header = next(reader)
        with open(outdir+_file,'w', newline="\n", encoding="utf-8") as ofile:
            writer = csv.writer(ofile, delimiter=';')
            for row in reader:
                if row[0] not in ('MD','MJ'):
                    row.insert(0,last_date)
                    writer.writerow(row)
    return outdir+_file

def main():
    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    last_date = getDate(url)

    if last_date > getMaxDate(cursor,table_name):

        _file = parseCSV(str(last_date))
        with open(_file, 'r') as ifile:
            SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
            print("Executing Copy in "+table_name)
            cursor.copy_expert(sql=SQL_STATEMENT % table_name, file=ifile)
            db_conn.commit()
        remove(_file)

        ### VACUUM ANALYZE
        call(f'psql -d torkcapital -c "VACUUM ANALYZE {table_name};"',shell=True)

    cursor.close()
    db_conn.close()

if __name__=="__main__":
    main()