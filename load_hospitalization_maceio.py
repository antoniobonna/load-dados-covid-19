# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
from bs4 import BeautifulSoup as bs

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
city_local = 'Maceió - AL'
current_date = date.today()-timedelta(days=1)
url = 'http://www.saude.al.gov.br/leitos-para-enfrentamento-da-covid-19/'
str_date = current_date.strftime('%d.%m.%y')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def findPDFLink(url,str_date):
    page = requests.get(url)
    bs_page = bs(page.content, 'html.parser')
    boxes = [item.find_all('a') for item in bs_page.select('article p') if str_date in item.text]
    links = [item.get('href') for sublist in boxes for item in sublist if item.get('href').endswith('.pdf')]
    if links:
        #return links[-1]
        return links[0]
    return None

def parseDF(pdf_file):
    import tabula
    #import pandas as pd
    headers = ['city','bed_type','bed_number','inpatients','occupation']

    df = tabula.read_pdf(pdf_file,pages = 1, area=(215.431,347.518,794.38,594.576))[-1]
    df = df.dropna(axis=1, how='all')
    df.columns = headers

    idx_row = df.index[df['city'] == 'Maceió'].tolist()[0]
    end_row = idx_row + 3
    df = df[idx_row:end_row]

    icu = getValuesDF(df,'UTI','inpatients')
    inpatients = icu + getValuesDF(df,'U intermediária','inpatients') + getValuesDF(df,'Leitos Clínicos','inpatients')
    icu_beds = getValuesDF(df,'UTI','bed_number')
    nursery_beds = getValuesDF(df,'U intermediária','bed_number') + getValuesDF(df,'Leitos Clínicos','bed_number')

    return (icu,inpatients,icu_beds,nursery_beds)

def getValuesDF(df,bed,col):
    value = int(df[df['bed_type'] == bed].iloc[0][col])
    return value

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    pdf_file = findPDFLink(url,str_date)

    if pdf_file:
        icu, inpatients, icu_beds, nursery_beds = parseDF(pdf_file)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{city_local}',{inpatients},{icu})"
        insertDB(db_conn,cursor,query)

        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{city_local}','ICU',{icu_beds})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{city_local}','Nursery',{nursery_beds})"
        insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

    else:
        raise ValueError('No data for ' + str(current_date))

if __name__=="__main__":
    main()