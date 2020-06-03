# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
from bs4 import BeautifulSoup as bs
import re

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Sergipe'
current_date = date.today()-timedelta(days=1)
url = 'https://todoscontraocorona.net.br/boletins/'
str_date = current_date.strftime('%d-%m-%Y')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(label):
    boletim_date = re.search(r'\d{2}-\d{2}-\d{4}',label).group()

    return boletim_date

def findBoletimLink(url,str_date):
    page = requests.get(url)
    bs_page = bs(page.content, 'html.parser')
    boxes = bs_page.find_all('a', {'class': 'fusion-link-wrapper'})
    for box in boxes:
        if getDate(box.get('aria-label')) == str_date:
            return box.get('href')
    return None

def parseBoletim(link):
    from parsel import Selector

    page = requests.get(link)
    selector = Selector(text=page.text)
    beds = selector.xpath('//h5[text() ="Rede Pública"]/ancestor::node()[2]')
    icu_beds = parseNumber(beds.xpath('.//node()[contains(text(),"UTI Estadual")]/text()').get())
    nursery_beds = parseNumber(beds.xpath('.//node()[contains(text(),"Enfermaria Estadual")]/text()').get())
    #icu_beds_aracaju = parseNumber(beds.xpath('.//node()[contains(text(),"UTI Aracaju")]/text()').get())
    nursery_beds_aracaju = parseNumber(beds.xpath('.//node()[contains(text(),"Enfermaria")]/text()').get())
    #icu_beds += icu_beds_aracaju
    nursery_beds += nursery_beds_aracaju

    icu = parseNumber(selector.xpath('//node()[text()="UTI"]/ancestor::node()[4]//node()[contains(text(),"Pública")]/text()').get())
    nursery = parseNumber(selector.xpath('//node()[text()="Enfermaria"]/ancestor::node()[4]//node()[contains(text(),"Pública")]/text()').get())
    inpatients = icu + nursery

    return (icu,inpatients,icu_beds,nursery_beds)

def parseNumber(str):
    return int(str.split()[-1])

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    boletim = findBoletimLink(url,str_date)

    if boletim:
        icu, inpatients, icu_beds, nursery_beds = parseBoletim(boletim)

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