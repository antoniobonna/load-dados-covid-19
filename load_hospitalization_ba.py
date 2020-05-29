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
table_nameh = 'local_hospitalization'
table_nameb = 'local_beds'
indir = '/home/ubuntu/dump/dados_covid_19/'
file = 'coronavirus_bahia.xlsx'
current_local = 'Bahia'

def getMaxDate(cursor,schema,table_nameh,local):
    cursor.execute(f"SELECT MAX(date) FROM {schema}.{table_nameh} WHERE local = '{local}'")
    maxdate = str(cursor.fetchall()[0][0])
    return maxdate

def parseExcel(excelfile,maxdate):
    df = pd.read_excel(excelfile,sheet_name=0)
    df = df[df['date'] > maxdate]
    return df

def parseRow(row):
    local = row['local']
    current_date = str(row['date'].date())
    beds_icu = row['beds_icu']
    beds_nursery = row['beds_nursery']
    icu_inpatients = int(row['icu'])
    inpatients = int(row['inpatients'])
    return (local,current_date,beds_icu,beds_nursery,icu_inpatients,inpatients)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def main():
    maxdate = getMaxDate(cursor,schema,table_nameh,current_local)
    df = parseExcel(indir+file,maxdate)
    for idx, row in df.iterrows():
        local,current_date,beds_icu,beds_nursery,icu_inpatients,inpatients = parseRow(row)

        query = f"INSERT INTO {schema}.{table_nameh} VALUES ('{current_date}','{local}',{inpatients},{icu_inpatients})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {schema}.{table_nameb} VALUES ('{current_date}','{local}','ICU',{beds_icu})"
        insertDB(db_conn,cursor,query)
        query = f"INSERT INTO {schema}.{table_nameb} VALUES ('{current_date}','{local}','Nursery',{beds_nursery})"
        insertDB(db_conn,cursor,query)

    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_nameh),shell=True)
    call('psql -d torkcapital -c "VACUUM ANALYZE {}.{}"'.format(schema,table_nameb),shell=True)

if __name__=="__main__":
    main()