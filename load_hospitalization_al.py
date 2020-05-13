# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
from bs4 import BeautifulSoup as bs
from locale import setlocale,LC_TIME

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Alagoas'
current_date = date.today()-timedelta(days=1)
url = 'http://www.alagoascontraocoronavirus.al.gov.br/#covid-alagoas'
setlocale(LC_TIME, 'pt_BR')
str_date = current_date.strftime('%d de %B de %Y')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')
    return (db_conn,cursor)

def getDate(bs_page, str_date):
    if str_date in bs_page.select('#inicio')[0].text:
        return True
    return False

def parsePage(boxes):
    inpatients = nursery_beds = 0
    for box in boxes:
        if box.startswith('UTI INTERMEDIÁRIA'):
            a,b = [int(s) for s in box.split() if s.isdigit()]
            nursery_beds += a
            inpatients += b

        elif box.startswith('UTI'):
            icu_beds,icu = [int(s) for s in box.split() if s.isdigit()]
            inpatients += icu

        elif box.startswith('LEITOS CLÍNICOS'):
            a,b = [int(s) for s in box.split() if s.isdigit()]
            nursery_beds += a
            inpatients += b

    return (inpatients,icu,nursery_beds,icu_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    page = requests.get(url)
    bs_page = bs(page.content, 'html.parser')

    if getDate(bs_page,str_date):

        boxes = [b.text for b in bs_page.find_all('h3')]
        inpatients,icu,nursery_beds,icu_beds = parsePage(boxes)

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