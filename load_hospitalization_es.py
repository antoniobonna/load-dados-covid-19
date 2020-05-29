# -*- coding: utf-8 -*-

import requests
from parsel import Selector
from datetime import datetime, date, timedelta
import credentials
import psycopg2
from subprocess import call

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
current_date = date.today()-timedelta(days=1)
url = "https://coronavirus.es.gov.br/leitos-uti"
state_name = 'Espírito Santo'

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(selector):
    import re

    str_date = selector.xpath('//strong[contains(text(),"Atualizado")]/text()').get()
    match = re.search(r'\d{2}\/\d{2}\/\d{4}',str_date).group()
    last_date = datetime.strptime(match, '%d/%m/%Y').date()

    return last_date

def parseTable(selector):
    import pandas as pd

    table = selector.xpath('//table[1]').get()
    df = pd.read_html(table)[0]
    header = [h for h in df.iloc[0][1:]]
    header.insert(0,'BED')
    df = df[1:]
    df.columns = header
    nursery_beds = df[df['BED'] == 'Enfermaria'].iloc[0]['TOTAL DE LEITOS']
    icu_beds = df[df['BED'] == 'UTI'].iloc[0]['TOTAL DE LEITOS']
    icu = df[df['BED'] == 'UTI'].iloc[0]['LEITOS OCUPADOS']
    inpatients = df[df['BED'] == 'Total de Leitos Covid-19'].iloc[0]['LEITOS OCUPADOS']

    return (icu,inpatients,icu_beds,nursery_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    page = requests.get(url)
    selector = Selector(text=page.text)
    last_date = getDate(selector)

    if last_date == current_date:

        icu,inpatients,icu_beds,nursery_beds = parseTable(selector)

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

    else:
        raise ValueError('No data for ' + str(current_date))
        driver.quit()

if __name__=="__main__":
    main()