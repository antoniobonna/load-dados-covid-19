# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import tabula
import requests
from bs4 import BeautifulSoup
from locale import setlocale,LC_TIME

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
setlocale(LC_TIME, 'pt_BR')
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
local = 'São Paulo - SP'
current_date = date.today()-timedelta(days=1)
pdf = 'https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/saude/{}_{}.pdf'
suffixes = ['BoletimDiario','Boletim_Diario','Boletim Diário','BoletimDiário','Boletim_Diário','Boletim Diario']
url_mainpage = 'https://www.prefeitura.sp.gov.br/cidade/secretarias/saude/vigilancia_em_saude/doencas_e_agravos/coronavirus/index.php?p=295572'

def parseDay(current_date):
    day = int(current_date.day)
    if day == 1:
        return (str(day)) + 'º'
    return str(day)

def findBoletimLink(url,current_date):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    boletins = [l for l in soup.find_all('a') if 'boletim' in l.text.lower()]
    day = parseDay(current_date)
    newest_link = [l for l in boletins if current_date.strftime('{} de %B de %Y'.format(day)) in l.text][0].get('href')
    return newest_link

def findPDF(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    pdf_url = [l for l in soup.find_all('a') if 'pdf' in l.get('href')][0].get('href')
    return pdf_url

def parsePDF(pdf_url,pdf,current_date):
    try:
        for i in range(3,6):
            try:
                df = tabula.read_pdf(pdf_url,pages = i, area=(472.162,6.75,725.287,538.65), silent=True)
                if df:
                    return df[0]
            except:
                pass
    except:
        for suffix in suffixes:
            try:
                file = pdf.format(current_date.strftime('%d%m%Y'),suffix)
                print(file)
                df = tabula.read_pdf(file,pages = 4, area=(472.162,6.75,725.287,538.65), silent=True)
            except:
                pass
            else:
                return df[0]

boletim = findBoletimLink(url_mainpage,current_date)
pdf_url = findPDF(boletim)

df = parsePDF(pdf_url,pdf,current_date)
if len(df.columns) > 2:
    df.drop(df.columns[1], axis=1,inplace=True)
df.columns = ['key','value']

hospitalizated = int(df[df['key'].str.match("Internados na Rede Municipal") == True].iloc[0]['value'].replace('.',''))
icu = int(df[df['key'].str.match("Internados em UTI") == True].iloc[0]['value'].replace('.',''))
icu_rate = int(df[df['key'].str.contains("Taxa de Ocupação") == True].iloc[0]['value'].replace('%',''))/100

icu_beds = round(icu/icu_rate)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

query = "INSERT INTO {} VALUES ('{}','{}',{},{})".format(table_icu,str(current_date),local,hospitalizated,icu)
print(query)
cursor.execute(query)
db_conn.commit()

query = "INSERT INTO {} VALUES ('{}','{}','ICU',{})".format(table_beds,str(current_date),local,icu_beds)
print(query)
cursor.execute(query)
db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)