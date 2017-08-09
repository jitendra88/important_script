import MySQLdb as mdb
import multiprocessing
import sys
import xmlrpclib

TOTAL_NO_PROCESS = 1

con = mdb.connect('myu-v3-prod.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'myu_root', 'myu_root123root*', 'myuv3')
cur = con.cursor()

PAGINATION_LIMIT = 5000
server_url = 'http://chat.myu.co:4560'
EJABBERD_XMLRPC_LOGIN = {'user': 'admin', 'server': 'localhost', 'password': 'racers23'}


def calling(command, data):
    server = xmlrpclib.ServerProxy(server_url)
    fn = getattr(server, command)
    return fn(EJABBERD_XMLRPC_LOGIN, data)


# ======================================================= ejabered data creation in ==========================
print "=================================Total no of process creation start .................." + str(TOTAL_NO_PROCESS)
pool = multiprocessing.Pool(processes=TOTAL_NO_PROCESS * multiprocessing.cpu_count())
print "=================================Process created  .................." + str(TOTAL_NO_PROCESS)

page = None
if sys.argv[1:]:
    page = sys.argv[1:][0]
else:
    print "Please put cmd line argument..............."
    exit()
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


def ejabberd_user_register(page):
    start = (page - 1) * PAGINATION_LIMIT;
    # cur.execute("SELECT id,name FROM users ORDER BY id DESC limit " + str(
    #     start) + " ," + str(PAGINATION_LIMIT) + "")
    cur.execute("SELECT id,name FROM users  ORDER BY id DESC limit 2000 ")

    for row in cur.fetchall():
        print "user register ejabberd username id :::::::::::::" + str(row[0])
        print
        try:
            result = pool.apply_async(
                calling('register', {'user': str(row[0]), 'password': str(row[0]), 'host': 'localhost'}))
            print result
        except Exception as e:
            print e.faultString
    print ("Complete=========================user creation ")
    pool.close()


ejabberd_user_register(page)
