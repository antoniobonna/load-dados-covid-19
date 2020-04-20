import pandas as pd
import credentials
from sqlalchemy import create_engine
from subprocess import call
import requests
from datetime import date,timedelta

### definicoes de variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER,PASSWORD,HOST,DATABASE))
schema = 'covid_19'
table_name = 'coronavirus_who_stg'
url = 'https://www.worldometers.info/coronavirus/#countries'
headers = ['country', 'total_cases', 'new_cases', 'total_deaths', 'new_deaths', 'total_recovered', 'active_cases', 'critical', 'cases_per_milion', 'deaths_per_milion', 'total_tests', 'tests_per_milion']
current_date = date.today()-timedelta(days=1)

def parseHtmlTable(url):
    html = requests.get(url).content
    df_list = pd.read_html(html)
    table = df_list[1]
    df = table[table['Country,Other'] != 'World'][:-1]
    df = df[df.columns[:12]]

    df.columns = headers
    df = df.copy()
    df['country'] = df['country'].str.replace('USA','United States').replace('UK','United Kingdom').replace('S. Korea','South Korea').replace('UAE','United Arab Emirates').replace('CAR','Central African Republic').replace('Turks and Caicos','Turks and Caicos Islands').replace('Vatican City','Vatican').replace('North Macedonia','Macedonia').replace('DRC','Democratic Republic of Congo').replace('Czechia','Czech Republic')
    df['new_cases'] = df['new_cases'].str.replace('+','')
    df['new_cases'] = df['new_cases'].str.replace(',','')
    df['new_deaths'] = df['new_deaths'].str.replace('+','')
    df['new_deaths'] = df['new_deaths'].str.replace(',','')
    #df['first_case'] = pd.to_datetime('2020 ' + df['first_case'], format='%Y %b %d', errors='ignore')
    df['date'] = current_date

    df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')

parseHtmlTable(url)

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_name),shell=True)