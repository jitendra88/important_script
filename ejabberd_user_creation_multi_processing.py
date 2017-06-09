import MySQLdb as mdb
import xmlrpclib
import multiprocessing
import sys
PAGINATION_LIMIT = 2000
server_url = 'http://beta-chat.myu.co:4560'
EJABBERD_XMLRPC_LOGIN = {'user': 'admin', 'server': 'localhost', 'password': 'racers23'}


def calling(command, data):
    server = xmlrpclib.ServerProxy(server_url)
    fn = getattr(server, command)
    return fn(EJABBERD_XMLRPC_LOGIN, data)


con = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_v3_3005');
cur = con.cursor()
page = sys.argv[1:][0]
print page
if page:
    try:
        page = int(page)
        if page == 0:
            print ("Page value 0 not allowed  ...............")
            exit();
    except:
        print ("Page value only integer allowed")
        exit()

cur.execute('SELECT id,name FROM users ')

pool = multiprocessing.Pool(processes=20 * multiprocessing.cpu_count())
for row in cur.fetchall():
    print "user register ejabberd username id :::::::::::::" + str(row[0])
    print
    try:
        result = pool.apply_async(calling('register', {'user': str(row[0]), 'password': str(row[0]), 'host': 'localhost'}))
        print result
    except Exception as e:
        print e.faultString




