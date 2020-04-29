# -*- coding: utf-8 -*-

from datetime import date, timedelta
import credentials
import psycopg2
from subprocess import call
import tabula

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
local = 'São Paulo - SP'
current_date = date.today()-timedelta(days=1)
pdf = 'https://www.prefeitura.sp.gov.br/cidade/secretarias/upload/saude/{}_{}.pdf'
suffixes = ['BoletimDiario','Boletim_Diario','Boletim Diário','BoletimDiário','Boletim_Diário','Boletim Diario']

def parsePDF(pdf,current_date):
    for suffix in suffixes:
        try:
            file = pdf.format(current_date.strftime('%d%m%Y'),suffix)
            print(file)
            df = tabula.read_pdf(file,pages = 2, area=(16.743,286.498,495.976,593.088))
        except:
            pass
        else:
            return df[0]

df = parsePDF(pdf,current_date)
header = list(df)[0]

hospitalizated = int(df[df[header].str.match("Municipal")].iloc[0][header].split()[-1].replace('.',''))
icu = int(df[df[header].str.contains("Internados em UTI")].iloc[0][header].split()[-1].replace('.',''))
icu_rate = int(df[df[header].str.contains("%")].iloc[0][header].split()[-1].replace('%',''))/100

icu_beds = round(icu/icu_rate)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()

query = "INSERT INTO {} VALUES ('{}','{}',{},{})".format(table_icu,str(current_date),local,hospitalizated,icu)
# print(query)
cursor.execute(query)
db_conn.commit()

query = "INSERT INTO {} VALUES ('{}','{}','ICU',{})".format(table_beds,str(current_date),local,icu_beds)
# print(query)
cursor.execute(query)
db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)