# -*- coding: utf-8 -*-

from datetime import  date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
import json

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
current_date = date.today()-timedelta(days=1)
state_name = 'Acre'
url = 'http://sesacrenetnovo.ac.gov.br/relatorio/api/public/dashboard/33d9152b-2190-432b-b101-5df0475c1a8c/card/'

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def parseJson(url):
    links = [('916','nursery_beds'),('1070','nursery'),('917','icu_beds'),('1071','icu')]
    row = {}
    for (link,value) in links:
        response = requests.get(url+link)
        d = json.loads(response.text)
        row[value] = d['data']['rows'][0][0]

    return (row['icu'],row['nursery'],row['icu_beds'],row['nursery_beds'])

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():

    icu,nursery,icu_beds,nursery_beds = parseJson(url)
    inpatients = icu + nursery

    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_name}',{inpatients},{icu})"
    insertDB(db_conn,cursor,query)

    query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_name}','ICU',{icu_beds})"
    insertDB(db_conn,cursor,query)
    query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_name}','Nursery',{nursery_beds})"
    insertDB(db_conn,cursor,query)

    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

if __name__=="__main__":
    main()