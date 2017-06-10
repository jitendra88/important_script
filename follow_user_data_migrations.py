import MySQLdb as mdb
import multiprocessing
import sys

PAGINATION_LIMIT = 2000000
TOTAL_NO_PROCESS = 250
from openpyxl.workbook import Workbook

# ============================================ global object ====================================================
user_obj = {}

# ============================================ xlsx   error reporter ==============================================#
header = [u'idV2', u'errorMessage']
error_report_data = []
wb = Workbook()
dest_filename = 'error_report_follow_user.xlsx'
ws1 = wb.active
ws1.title = "errorMessage"
ws1.append(header)

# ================================ end ============================================================================#

# ==================================== V2 database ================================================
con_v2 = mdb.connect('php-beta.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'root', 'admin123*', 'myu')
cur2 = con_v2.cursor(mdb.cursors.DictCursor)
cur2.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur2.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur2.execute("SET character_set_connection=utf8mb4;")  # same as above
# =========================================== end =================================================

page = None
if sys.argv[1:]:
    page = sys.argv[1:][0]
else:
    print "Please put cmd line argument..............."
    exit()
if page:
    try:
        page = int(page)
        if page == 0:
            print ("Page value 0 not allowed  ...............")
            exit()
    except:
        print ("Page value only integer allowed")
        exit()

# ---------------------------------- V3 ---------------------------------------------------
con_v3 = mdb.connect('beta-v3.ctjt9fapuyu7.eu-west-1.rds.amazonaws.com', 'root', 'myuroot123', 'myuv3');
cur_3 = con_v3.cursor(mdb.cursors.DictCursor)
cur_3.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur_3.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur_3.execute("SET character_set_connection=utf8mb4;")  # same as above

print "===================use object creation started ................."

cur_3.execute('SELECT id,idV2 FROM users WHERE  idV2 is NOT  NULL ')
for row in cur_3.fetchall():
    idV2 = str(row["idV2"])
    idV3 = str(row["id"])
    user_obj[idV2] = idV3

print "=======================end======================"

print "=================================Total no of process creation start .................." + str(TOTAL_NO_PROCESS)
pool = multiprocessing.Pool(processes=TOTAL_NO_PROCESS * multiprocessing.cpu_count())
print "=================================Process created  .................." + str(TOTAL_NO_PROCESS)


def save_follow_data(page):
    start = (page - 1) * PAGINATION_LIMIT
    print "================page  :===" + str(page)
    print "================Pagination Limit  :===" + str(PAGINATION_LIMIT)

    cur2.execute(
        "SELECT * FROM favorite WHERE  is_favorite = 1  limit " + str(
            start) + " ," + str(PAGINATION_LIMIT) + "")
    print "================Total Message count In Follow Table :===" + str(cur2.rowcount)

    for row in cur2.fetchall():
        create_v3_chat_obj = dict()
        try:
            followerUserId = str(row['user_id'])
            followedUserId = str(row['professor_id'])
            idV2 = row['id']
            if followerUserId not in user_obj:
                data_error_row = list()
                data_error_row.append(row['user_id'])
                data_error_row.append("followerUserId UserId does exist in V3 database ")
                ws1.append(data_error_row)
                continue
            if followedUserId not in user_obj:
                data_error_row = list()
                data_error_row.append(row['professor_id'])
                data_error_row.append("followedUserId UserId does exist in V3 database ")
                ws1.append(data_error_row)
                continue
            create_v3_chat_obj['followerUserId'] = str(followerUserId)
            create_v3_chat_obj['followedUserId'] = str(followedUserId)
            create_v3_chat_obj['idV2'] = str(idV2)
            pool.apply_async(saveFollowUser(create_v3_chat_obj))
        except Exception as e:
            data_error_row = list()
            data_error_row.append(row["id"])
            data_error_row.append("Foriegn Key Constraints error")
            ws1.append(data_error_row)
            continue
    con_v2.close()
    con_v3.close()
    print "============================= script completed ==================================================="
    wb.save(filename=dest_filename)
    #wb1.save(filename=dest_filename_message)
    pool.close()
    print "============================= please check error report file ==========================================="
    exit()


def saveFollowUser(data_v3_obj):
    query = (
        "insert into follow (followedUserId,followerUserId,idV2) values ('%s', '%s', '%s')" % (
            data_v3_obj['followedUserId'], data_v3_obj['followerUserId'],data_v3_obj['idV2']))
    cur_3.execute(query)
    con_v3.commit()

save_follow_data(page)
