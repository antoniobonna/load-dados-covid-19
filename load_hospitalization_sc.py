# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import requests
from parsel import Selector
from locale import setlocale,LC_TIME

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Santa Catarina'
setlocale(LC_TIME, 'pt_BR')
current_date = date.today()-timedelta(days=1)
url = 'https://www.coronavirus.sc.gov.br/boletins/'
str_date = current_date.strftime('%d %B %Y')

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def findPDFLink(url,str_date):
    page = requests.get(url)
    selector = Selector(text=page.text)
    new_boletim = next((s for s in selector.xpath('//h4/a/text()').extract() if str_date in s), None)
    if new_boletim:
        pdf_file = selector.xpath(f'//h4/a[text()="{new_boletim}"]/ancestor::node()[2]//a[contains(@href,".pdf")]/@href').get()
        return pdf_file
    return None

def parseDF(pdf_file):
    import pdfplumber
    from os import remove
    import wget
    outdir = '/home/ubuntu/scripts/load-dados-covid-19/csv'

    _file = wget.download(pdf_file, outdir, bar=None)
    pdf = pdfplumber.open(_file)
    remove(_file)

    for i in range(10):
        try:
            page = pdf.pages[i]
            table = [int(t) for sublist in page.extract_table() for t in sublist if 'Internações\nem UTI' in sublist and t.isdigit()]
            icu = table[2]
            occupation_rate = [float(w.replace('%','').replace(',','.')) for w in page.extract_text().split() if w.endswith('%')][0]
            icu_beds = round(icu/occupation_rate * 100)
        except:
            pass
        else:
            return (icu,icu_beds)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    pdf_file = findPDFLink(url,str_date)

    if pdf_file:
        icu, icu_beds = parseDF(pdf_file)

        db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
        query = f"INSERT INTO {table_icu} VALUES ('{str(current_date)}','{state_local}',NULL,{icu})"
        insertDB(db_conn,cursor,query)

        # query = f"INSERT INTO {table_beds} VALUES ('{str(current_date)}','{state_local}','ICU',{icu_beds})"
        # insertDB(db_conn,cursor,query)

        cursor.close()
        db_conn.close()
        db_conn.close()

        ### VACUUM ANALYZE
        call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
        #call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

    else:
        raise ValueError('No data for ' + str(current_date))

if __name__=="__main__":
    main()