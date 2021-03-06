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
table_name = 'healthcare_stg'
indir = '/home/ubuntu/dump/dados_covid_19/'
file = 'coronavirus_hapvida.xlsx'

def parseExcel(excelfile):
    df = pd.read_excel(excelfile,sheet_name=1)
    cursor.execute("TRUNCATE {}.{}".format(schema,table_name))
    db_conn.commit()
    df.to_sql(table_name,engine,schema=schema,index=False,if_exists='append')
    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_name),shell=True)

parseExcel(indir+file)