# -*- coding: utf8 -*-
import MySQLdb as mdb
import base64
import multiprocessing
import sys
from json import loads, dumps

from openpyxl.workbook import Workbook

# ============================================ global object ====================================================
user_obj = {}

duplicate_msg_id_list = []

# =========================================================================================

# ============================================ xlsx   error reporter ==============================================#
header = [u'messageID', u'errorMessage', u'body']
error_report_data = []
wb = Workbook()
dest_filename = 'error_report_chat_message.xlsx'
ws1 = wb.active
ws1.title = "errorMessage"
ws1.append(header)

# ================================ end ============================================================================#

CHAT_TYPE_IMAGE_V2 = 'vImage'
CHAT_TYPE_TEXT_V2 = 'vText'
CHAT_TYPE_IMAGE_V3 = 'chat_image'
CHAT_TYPE_TEXT_V3 = 'chat_text'
PAGINATION_LIMIT = 4000000
TOTAL_NO_PROCESS = 250
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
con_v3 = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_v3_04052017');
cur_3 = con_v3.cursor(mdb.cursors.DictCursor)

cur_3.execute('SELECT id,idV2 FROM users WHERE  idV2 is NOT  NULL ')

print "===================use object creation started ................."
for row in cur_3.fetchall():
    idV2 = str(row["idV2"])
    idV3 = str(row["id"])
    user_obj[idV2] = idV3
con_v3.close()

print "=======================end======================"

# ====================================================== end  ======================


# ==================================== V2 database ================================================
con_v2 = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_26042017_0')
cur2 = con_v2.cursor(mdb.cursors.DictCursor)
cur2.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur2.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur2.execute("SET character_set_connection=utf8mb4;")  # same as above
# =========================================== end =================================================
# =================================== chat database v3 ===========================================
con_v3_chat = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_v3_chat')
cur3 = con_v3_chat.cursor(mdb.cursors.DictCursor)
cur3.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur3.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur3.execute("SET character_set_connection=utf8mb4;")  # same as above

# ============================================================end =================================


print "=================================Total no of process created .................."+str(TOTAL_NO_PROCESS)
pool = multiprocessing.Pool(processes=TOTAL_NO_PROCESS * multiprocessing.cpu_count())


def get_chat_data_from_v2(page):
    start = (page - 1) * PAGINATION_LIMIT;
    for_duplicate_msg_id = {}
    print "================page  :===" + str(page)
    print "================Pagination Limit  :===" + str(PAGINATION_LIMIT)

    cur2.execute(
        "SELECT * FROM ofMessageArchive where  messageID NOT IN  (SELECT message_id from deleted_messages) ORDER BY messageID DESC limit " + str(
            start) + " ," + str(PAGINATION_LIMIT) + "")
    print "================Total Message count In ofMessageArchive Table :===" + str(cur2.rowcount)

    for row in cur2.fetchall():
        create_v3_chat_obj = dict()
        try:
            fromJID = str(row['fromJID']).replace("@ip-172-31-42-152", '')
            toJID = str(row['toJID']).replace("@ip-172-31-42-152", '')
            if fromJID not in user_obj:
                data_error_row = list()
                data_error_row.append(row["messageID"])
                data_error_row.append("fromJID UserId does exist in V3 database ")
                data_error_row.append(toJID)
                ws1.append(data_error_row)
                continue
            elif toJID not in user_obj:
                data_error_row = list()
                data_error_row.append(row["messageID"])
                data_error_row.append("toJID  UserId does exist in V3 database ")
                data_error_row.append(toJID)
                ws1.append(data_error_row)
                continue
            else:
                create_v3_chat_obj['from'] = user_obj[fromJID] + "@localhost"
                create_v3_chat_obj['to'] = user_obj[toJID] + "@localhost"
                create_v3_chat_obj['sender'] = user_obj[fromJID]
                create_v3_chat_obj['receiver'] = user_obj[toJID]
                create_v3_chat_obj['timestamp'] = str(row['sentDate']) + "000"
                dataBody = loads(base64.b64decode(str(row['body']).encode("utf-8").replace("%2B", "+")))
                if dataBody:
                    if dataBody['msg_id'] not in for_duplicate_msg_id:
                        create_v3_chat_obj['msg_id'] = dataBody['msg_id']
                        for_duplicate_msg_id[dataBody['msg_id']] = dataBody['msg_id']
                        if dataBody['chat_type'] == CHAT_TYPE_IMAGE_V2:
                            create_v3_chat_obj['chat_type'] = CHAT_TYPE_IMAGE_V3
                            messageBody = dict()
                            messageBody['s3MediaThumbnailUrl'] = dataBody['posted_thumbimage']
                            messageBody['s3MediaUrl'] = dataBody['posted_image']
                            create_v3_chat_obj['body'] = dumps(messageBody).encode("utf-8").encode("base64").replace("\n",
                                                                                                                     '')
                        elif dataBody['chat_type'] == CHAT_TYPE_TEXT_V2:
                            create_v3_chat_obj['chat_type'] = CHAT_TYPE_TEXT_V3
                            create_v3_chat_obj['body'] = (dataBody['Post_Message']).encode("utf-8").encode(
                                'base64').replace(
                                "\n", '')
                    else:
                        duplicate_msg_id_list.append(for_duplicate_msg_id[dataBody['msg_id']])

        except Exception as e:
            data_error_row = list()
            data_error_row.append(row["messageID"])
            data_error_row.append(str(e.message))
            data_error_row.append(str(row['body']))
            ws1.append(data_error_row)
            continue
        if 'body' in create_v3_chat_obj and 'sender' in create_v3_chat_obj and 'receiver' in create_v3_chat_obj:
            pool.apply_async(insert_data_into_chat_database(create_v3_chat_obj))
        else:
            data_error_row = list()
            data_error_row.append(row["messageID"])
            data_error_row.append("Body data Null ")
            data_error_row.append(str(row['body']))
            ws1.append(data_error_row)

    con_v2.close()
    con_v3_chat.close()
    print "============================= duplicate message count is :" + str(len(duplicate_msg_id_list))
    print "============================= script completed ==================================================="
    wb.save(filename=dest_filename)
    print "============================= please check error report file ==========================================="


def insert_data_into_chat_database(data_v3_obj):
    xml = '<message xml:lang="en" to="' + data_v3_obj['to'] + '" from="' + data_v3_obj['from'] + '" type="chat" id="' + \
          data_v3_obj['msg_id'] + '" xmlns="jabber:client"><body>' + data_v3_obj['body'] + '</body><subject>' + \
          data_v3_obj['chat_type'] + '</subject></message>'
    query = (
        "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')" % (
            data_v3_obj['sender'], xml, data_v3_obj['to'], data_v3_obj['timestamp'], '', data_v3_obj['to'],
            data_v3_obj['body'],
            'chat'))
    query1 = (
        "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s')" % (
            data_v3_obj['receiver'], xml, data_v3_obj['from'], data_v3_obj['timestamp'], '', data_v3_obj['from'],
            data_v3_obj['body'],
            'chat'))
    cur3.execute(query)
    cur3.execute(query1)
    con_v3_chat.commit()


get_chat_data_from_v2(page)
