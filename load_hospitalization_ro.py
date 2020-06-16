# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import pandas as pd

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Rondônia'
current_date = date.today()-timedelta(days=1)
csv_file = 'http://covid19.sesau.ro.gov.br/arquivos/Leitos.csv'
str_date = current_date.strftime('%d/%m/%Y')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(df):
    size = len(df) - 1
    df = df[size:]
    last_date = df[df.columns[0]].iloc[0]
    if not last_date:
        last_date = df[df.columns[1]].iloc[0]

    return last_date

def parseCSV(df):
    headers = ['Total de Leitos Clínicos','Leitos Clínicos ocupados - SUSPEITOS','Leitos Clínicos ocupados - CONFIRMADOS',
    'Total de Leitos UTI','UTI ocupadas - SUSPEITOS','UTI ocupados - CONFIRMADOS']
    size = len(df) - 1

    df = df[headers]
    df = df[size:]

    icu = int(df['UTI ocupadas - SUSPEITOS'].iloc[0]) + int(df['UTI ocupados - CONFIRMADOS'].iloc[0])
    inpatients = icu + int(df['Leitos Clínicos ocupados - SUSPEITOS'].iloc[0]) + int(df['Leitos Clínicos ocupados - CONFIRMADOS'].iloc[0])
    icu_beds = int(df['Total de Leitos UTI'].iloc[0])
    nursery_beds = int(df['Total de Leitos Clínicos'].iloc[0])

    return (icu,inpatients,icu_beds,nursery_beds)

def insertDB(db_conn,cursor,query):
    # print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    df = pd.read_csv(csv_file,sep=';',encoding='utf-8',skiprows=1)

    if getDate(df) == str_date:
        icu, inpatients, icu_beds, nursery_beds = parseCSV(df)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_local}',{inpatients},{icu})"
        insertDB(db_conn,cursor,query)

        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','ICU',{icu_beds})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','Nursery',{nursery_beds})"
        insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

    else:
        raise ValueError('No data for ' + str_date)

if __name__=="__main__":
    main()