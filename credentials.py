from base64 import b64decode

database_login = {
        'DATABASE' : 'torkcapital',
        'USER' : 'postgres',
        'HOST' : 'ec2-18-229-106-103.sa-east-1.compute.amazonaws.com',
        'PASSWORD' : 'VG9ya0AyMDE4'
}

def setDatabaseLogin():
        return [database_login['DATABASE'], database_login['HOST'], database_login['USER'], b64decode(database_login['PASSWORD']).decode("utf-8")]
