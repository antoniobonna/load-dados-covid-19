# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
from requests import Session
from bs4 import BeautifulSoup as bs

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
city_local = 'Paraná'
current_date = date.today()-timedelta(days=1)
url = 'http://www.saude.pr.gov.br/modules/conteudo/conteudo.php?conteudo=3507'
str_date = current_date.strftime('%d/%m/%Y')
tor_file = '/home/ubuntu/scripts/load-dados-covid-19/tor_port'
session = Session()
session.proxies = {'http': 'socks5://127.0.0.1:9050'}

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def findPDFLink(url,str_date):
    page = session.get(url)
    bs_page = bs(page.content, 'html.parser')
    boxes = [item.find('a').get('href') for item in bs_page.find_all('strong') if str_date in item.text]
    if boxes:
        return boxes[0]
    return None

def parseDF(pdf_file):
    import tabula
    from os import remove
    headers = ['region','icu_beds','icu','icu_rate','nursery_beds', 'nursery', 'nursery_rate']
    _file = pdf_file.split('/')[-1]

    response = session.get(pdf_file)
    with open(_file, 'wb') as f:
        f.write(response.content)

    for i in range(3,10):
        try:
            df = tabula.read_pdf(_file, pages = i, area=(239.116,0.744,751.559,592.025), silent=True)[0]
            df = df.dropna(axis=1, how='all')
            df = df[df[df.columns[0]] == 'TOTAL']
            df = df[list(df)[:7]]
            df.columns = headers

            icu = int(df['icu'].iloc[0])
            inpatients = icu + int(df['nursery'].iloc[0])
            icu_beds = int(df['icu_beds'].iloc[0])
            nursery_beds = int(df['nursery_beds'].iloc[0].replace('.',''))
        except:
            pass
        else:
            remove(_file)
            return (icu,inpatients,icu_beds,nursery_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    call(f'tor -f {tor_file} --quiet',shell=True)
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