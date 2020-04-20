import pandas as pd
import credentials
from sqlalchemy import create_engine
from subprocess import call

### definicoes de variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER,PASSWORD,HOST,DATABASE))
db_conn = engine.raw_connection()
cursor = db_conn.cursor()
schema = 'covid_19'
table_name = 'tests_stg'
url = 'https://github.com/owid/covid-19-data/blob/master/public/data/testing/covid-testing.xlsx?raw=true'
headers = ['country', 'date', 'tests']

def parseHtmlTable(url):
    df = pd.read_excel(url,sheet_name=1)
    df = df[['Entity','Date','Cumulative total']]
    df = df[df['Entity'] != 'United States - specimens tested (CDC)']
    df = df[df['Entity'] != 'India - people tested']
    df = df[df['Entity'] != 'Singapore - swabs tested']
    df = df[df['Entity'] != 'Japan - people tested']
    df = df[df['Cumulative total'].notna()]

    df.columns = headers
    df = df.copy()
    df['country'] = df['country'].str.split(' - ').str[0]
    df = df.groupby(['country','date'], as_index=False).max()

    cursor.execute("TRUNCATE {}.{}".format(schema,table_name))
    db_conn.commit()
    df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')
    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_name),shell=True)

parseHtmlTable(url)