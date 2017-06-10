import MySQLdb as mdb
import xmlrpclib

server_url = 'http://54.77.168.116:4560'
EJABBERD_XMLRPC_LOGIN = {'user': 'admin', 'server': 'localhost', 'password': 'racers23'}


def calling(command, data):
    server = xmlrpclib.ServerProxy(server_url)
    fn = getattr(server, command)
    return fn(EJABBERD_XMLRPC_LOGIN, data)


con = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_v3_04052017');
cur = con.cursor()
cur.execute('SELECT id,name FROM users ')
for row in cur.fetchall():
    print "user register ejabberd username id :::::::::::::" + str(row[0])
    print
    try:
        result = calling('register', {'user': str(row[0]), 'password': str(row[0]), 'host': 'localhost'})
        print result
    except Exception as e:
        print e.faultString
