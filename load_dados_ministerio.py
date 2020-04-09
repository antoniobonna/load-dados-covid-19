import pandas as pd
import numpy as np 
import credentials
from sqlalchemy import create_engine
from subprocess import call
import requests
from datetime import date,timedelta
from bs4 import BeautifulSoup
from time import sleep

### definicoes de variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER,PASSWORD,HOST,DATABASE))
schema = 'covid_19'
table_name = 'brazil_stg'
url = 'https://www.saude.gov.br/noticias'
url_prefix = 'https://www.saude.gov.br/'
headers = ['data', 'estado', 'confirmados', 'obitos']
current_date = (date.today()-timedelta(days=1)).strftime('%d/%m/%y')
TAG_WORDS = ['casos','mortes','coronavírus']
TAG_WORDS2 = ['casos','mortes','covid-19']

def parseHtmlTable(newurl):
    html = requests.get(newurl).content
    df_list = pd.read_html(html)
    df = df_list[0]
    df['date'] = date.today()-timedelta(days=1)
    df = df[['date']+[1,2,3]]
    df.columns = headers
    df = df[df['estado'].map(len) == 2]
    df = df.copy()
    df['confirmados'] = df['confirmados'].str.replace('.','')
    df['obitos'] = df['obitos'].str.replace('.','')
    df['obitos'] = df['obitos'].replace('-', np.nan)

    df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_name),shell=True)

def checkNewData(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    #while True:
    print('Checking new data...')
    newurl = ''
    for link in soup.find_all(class_='tileItem'):
        if link.find(class_='subtitle').text.startswith('ATUALIZAÇÃO DOS CASOS') or all(w in link.find(class_='tileHeadline').text.lower() for w in TAG_WORDS) or all(w in link.find(class_='tileHeadline').text.lower() for w in TAG_WORDS2):
            for item in link.find_all('li'):
                if item.text.strip() == current_date:
                    newurl = link.find('a').get('href')
    if newurl:
        parseHtmlTable(url_prefix+newurl)
    else:
        raise ValueError('could not find new data')
        #sleep(delay)

#checkNewData(url)