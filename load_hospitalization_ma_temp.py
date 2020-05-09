# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Maranhão'
current_date = date.today()-timedelta(days=34)
url_mainpage = 'http://www.saude.ma.gov.br/boletins-covid-19/'
str_date = current_date.strftime('BOLETIM %d/%m')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')
    return (db_conn,cursor)

def findExcelLink(url,str_date):
    page = requests.get(url)
    bs_page = bs(page.content, 'html.parser')
    excel_file = [item for item in bs_page.find_all('a') if str_date in item.text][0].get('href')
    return excel_file

def findColumns(df): ### acha colunas onde tem informações de leitos
    headers = list(df)
    for i in range(len(headers)):
        header = headers[i]
        if not df[df[header].astype(str).str.match("TAXA DE OCUPAÇÃO DE LEITOS DE UTI EXCLUSIVOS Covid-19") == True].empty:
            return (headers[i],headers[i+1])

def splitDF(header1,header2,df): ### divide dataframes em capital e municipio
    df = df[[header1,header2]]
    df = df[df[header1].notna()].reset_index(drop=True) ### remove linhas com valores nulos
    municipio_idx = df.index[df[header1] == 'TAXA DE OCUPAÇÃO DE LEITOS DE UTI EXCLUSIVOS Covid-19'].tolist()[0]
    municipio_idx2 = df.index[df[header1] == 'TAXA DE OCUPAÇÃO DE LEITOS DE CLÍNICOS EXCLUSIVOS Covid-19'].tolist()[0]
    if municipio_idx2 < municipio_idx:
        municipio_idx=municipio_idx2
    df_municipio = df[municipio_idx:]
    df_municipio = df_municipio[df_municipio[header2].notna()]
    return df_municipio

def getValuesDF(data,string):
    data.columns = ['key','value']
    value = data[data['key'] == string].iloc[0]['value']
    return value

def parseDF(df_capital):
    icu_capital = getValuesDF(df_capital,'Leitos ocupados UTI')
    inpatients_capital = getValuesDF(df_capital,'Leitos ocupados') + icu_capital
    icu_beds_capital = getValuesDF(df_capital,'Total de leitos UTI')
    nursery_beds_capital = getValuesDF(df_capital,'Total de leitos')

    return (icu_capital,inpatients_capital,icu_beds_capital,nursery_beds_capital)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    excel_file = findExcelLink(url_mainpage,str_date)
    df = pd.read_excel(excel_file,sheet_name=0)
    df = df.dropna(axis=1, how='all') ### deleta as colunas com todos valores nulos
    header1,header2 = findColumns(df)
    df_capital = splitDF(header1,header2,df)
    icu_capital,inpatients_capital,icu_beds_capital,nursery_beds_capital= parseDF(df_capital)

    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_local}',{inpatients_capital},{icu_capital})"
    insertDB(db_conn,cursor,query)
    
    query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','ICU',{icu_beds_capital})"
    insertDB(db_conn,cursor,query)
    query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','Nursery',{nursery_beds_capital})"
    insertDB(db_conn,cursor,query)

    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

if __name__=="__main__":
    main()