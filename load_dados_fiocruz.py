# -*- coding: utf-8 -*-

from datetime import date,datetime,timedelta
import credentials
import csv
from subprocess import call
import os
import psycopg2

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
tablename = 'covid_19.fiocruz_stg'
indir = '/home/ubuntu/dump/dados_covid_19/fiocruz/'
outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv/'
columns  = ['year','week','Unidade da Federação','Número de casos','update_date']
filename = 'situacao_da_gripe.csv'
current_date = str(date.today())

query_etl = '''INSERT INTO covid_19_dw.sars_brazil
SELECT to_date(concat(f.year::text,'-',f.week::text),'iyyy-iw')+6, s.state_id, new_cases
    FROM covid_19.fiocruz_stg f
    JOIN covid_19_dw.state s ON s.state=f.state AND s.country = 'Brazil'
    WHERE f.year >= 2016
    '''

def parseCSV(file,year):
    with open(indir+year+'/'+file,'r',encoding='utf-8') as ifile:
        reader = csv.DictReader(ifile)
        with open(outdir+filename,'a', newline="\n", encoding="utf-8") as ofile:
            writer = csv.DictWriter(ofile, fieldnames=columns,restval='', extrasaction='ignore',delimiter=';')
            for row in reader:
                #print(row)
                if row['Unidade da Federação'] != 'Brasil':
                    row['Número de casos'] = row['Número de casos'].split(" ")[0]
                    row['year'] = int(year)
                    row['week'] = int(file.split('.csv')[0])
                    row['update_date'] = current_date
                    writer.writerow(row)
    os.remove(indir+year+'/'+file)

years = [d for d in os.listdir(indir)]
for year in years:
    files = [f for f in os.listdir(indir+year) if f.endswith('.csv')]
    for file in files:
        parseCSV(file,year)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

## copy
with open(outdir+filename, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()

os.remove(outdir+filename)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+tablename+'";',shell=True)

# ### ELT para o DW...
# print("Carregando dados na tabela fato sars_brazil...")
# cursor.execute(query_etl)
# db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE covid_19_dw.sars_brazil";',shell=True)