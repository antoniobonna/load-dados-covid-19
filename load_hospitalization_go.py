# -*- coding: utf-8 -*-

from datetime import datetime, date
import credentials
import psycopg2
from subprocess import call
import requests
import json

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
current_date = date.today()
state_name = 'Goiás'
url = 'https://extranet.saude.go.gov.br/pentaho/plugin/cda/api/doQuery'

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def getDate(url):
    params = (
    ('path', '/coronavirus/paineis/painel.cda'),
    )
    data = {
      'dataAccessId': 'DSHCAMPLineInternacoesLastDate',
      'paramUnidadeEPI': 'GERAL',
      'paramUnidadeSaudeInternacoesArgos': 'HCAMP'
    }
    response = requests.post(url, params=params, data=data)
    d = json.loads(response.text)
    str_date = d['resultset'][0][0]
    last_date = datetime.strptime(str_date,'%d/%m/%Y').date()

    return last_date

def parseJson(url):
    params = (
    ('path', '/mapa_de_leitos/paineis/painel.cda'),
    ('dataAccessId', 'ds_panel_db_big_number_t'),
    )
    response = requests.get(url, params=params)
    d = json.loads(response.text)
    covid_criticos = [b for b in d['resultset'] if b[-1] == 'covid_criticos'][0]
    covid_semicriticos = [b for b in d['resultset'] if b[-1] == 'covid_semicriticos'][0]
    icu = covid_criticos[2]
    icu_beds = icu + covid_criticos[1]
    nursery = covid_semicriticos[2]
    nursery_beds = nursery + covid_semicriticos[1]

    return (icu,icu+nursery,icu_beds,nursery_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    last_date = getDate(url)

    if last_date == current_date:

        icu,inpatients,icu_beds,nursery_beds = parseJson(url)

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