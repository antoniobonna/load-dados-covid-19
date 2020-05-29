import re
import credentials
import psycopg2
from subprocess import call
from sys import argv

### variaveis
DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()
table_icu = 'covid_19.local_hospitalization'
table_beds = 'covid_19.local_beds'
state_local = 'Rio de Janeiro - RJ'
TAG_SUS = ['SUS','internados','UTI']
TAG_QUEUE = ['fila','aguardando transferência','UTI']
_date = argv[1]
text = argv[2]

def _Postgres(DATABASE, USER, HOST, PASSWORD):
    ### conecta no banco de dados
    db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
    cursor = db_conn.cursor()
    print('Connected to the database')

    return (db_conn,cursor)

def insertDB(db_conn,cursor,query):
    print(query)
    cursor.execute(query)
    db_conn.commit()

def parseList(list):
    for phrase in list:
        if all(w in phrase for w in TAG_SUS):
            inpacients, icu = [int(w.replace('.','')) for w in phrase.split() if w.replace('.','').isdigit()]
        elif len(re.findall(r'[1-9][0-9]%',phrase)) == 2:
            tx_icu, tx_nursery = [int(i.replace('%','')) for i in re.findall(r'[1-9][0-9]%',phrase)]
        elif all(w in phrase for w in TAG_QUEUE):
            queue, queue_icu = [int(w.replace('.','')) for w in phrase.split() if w.replace('.','').isdigit()]
    nursery = inpacients - icu
    icu_beds = round(icu/tx_icu * 100)
    nursery_beds = round(nursery/tx_nursery * 100)

    return (icu_beds,nursery_beds,queue,queue_icu)

def main():
    list = [t.strip() for t in text.split('\n') if t.strip()]
    icu_beds,nursery_beds,queue,queue_icu = parseList(list)

    db_conn,cursor = _Postgres(DATABASE, USER, HOST, PASSWORD)
    query = f"""INSERT INTO {table_icu} (date,local,queue,icu_queue) VALUES ('{_date}','{state_local}',{queue},{queue_icu}) 
    ON CONFLICT  (date, local) DO UPDATE SET queue = EXCLUDED.queue, icu_queue = EXCLUDED.icu_queue
    WHERE local_hospitalization.date = EXCLUDED.date AND local_hospitalization.local = EXCLUDED.local"""
    insertDB(db_conn,cursor,query)

    query = f"INSERT INTO {table_beds} VALUES ('{_date}','{state_local}','ICU SUS',{icu_beds})"
    insertDB(db_conn,cursor,query)
    query = f"INSERT INTO {table_beds} VALUES ('{_date}','{state_local}','Nursery SUS',{nursery_beds})"
    insertDB(db_conn,cursor,query)

    cursor.close()
    db_conn.close()

    ### VACUUM ANALYZE
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_icu+'";',shell=True)
    call('psql -d torkcapital -c "VACUUM ANALYZE '+table_beds+'";',shell=True)

if __name__=="__main__":
    main()
