# -*- coding: utf8 -*-
import MySQLdb as mdb
import base64
import multiprocessing
import sys
from json import loads, dumps

CHAT_TYPE_IMAGE_V2 = 'vImage'
CHAT_TYPE_TEXT_V2 = 'vText'
CHAT_TYPE_IMAGE_V3 = 'chat_image'
CHAT_TYPE_TEXT_V3 = 'chat_text'
PAGINATION_LIMIT = 1000000
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
user_obj = {}
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

pool = multiprocessing.Pool(processes=250* multiprocessing.cpu_count())


def get_chat_data_from_v2(page):
    start = (page - 1) * PAGINATION_LIMIT;

    print "================page  :===" + str(page)
    print "================Pagination Limit  :===" + str(PAGINATION_LIMIT)

    cur2.execute(
        "SELECT * FROM ofMessageArchive where  messageID NOT IN  (SELECT message_id from deleted_messages) ORDER BY messageID DESC limit " + str(
            start) + " ," + str(PAGINATION_LIMIT) + "")
    print "================Total Message count In ofMessageArchive Table :===" + str(cur2.rowcount)

    for row in cur2.fetchall():
        create_v3_chat_obj = dict()
        try:
            create_v3_chat_obj['from'] = user_obj[str(row['fromJID']).replace("@ip-172-31-42-152", '')] + "@localhost"
            create_v3_chat_obj['to'] = user_obj[str(row['toJID']).replace("@ip-172-31-42-152", '')] + "@localhost"
            create_v3_chat_obj['sender'] = user_obj[str(row['fromJID']).replace("@ip-172-31-42-152", '')]
            create_v3_chat_obj['receiver'] = user_obj[str(row['toJID']).replace("@ip-172-31-42-152", '')]
            create_v3_chat_obj['timestamp'] = str(row['sentDate']) + "000"
            dataBody = loads(base64.b64decode(str(row['body']).replace("%2B", "+")))
            if dataBody:
                create_v3_chat_obj['msg_id'] = dataBody['msg_id']
                if dataBody['chat_type'] == CHAT_TYPE_IMAGE_V2:
                    create_v3_chat_obj['chat_type'] = CHAT_TYPE_IMAGE_V3
                    messageBody = dict()
                    messageBody['s3MediaThumbnailUrl'] = dataBody['posted_thumbimage']
                    messageBody['s3MediaUrl'] = dataBody['posted_image']
                    create_v3_chat_obj['body'] = dumps(messageBody).encode("utf-8").encode("base64").replace("\n", '')
                elif dataBody['chat_type'] == CHAT_TYPE_TEXT_V2:
                    create_v3_chat_obj['chat_type'] = CHAT_TYPE_TEXT_V3
                    create_v3_chat_obj['body'] = (dataBody['Post_Message']).encode("utf-8").encode('base64').replace(
                        "\n", '')
        except Exception as e:
            print "Data Body :"+ str(str(row['body']))
            print "error message :"+ str(e.message)
            continue

        if create_v3_chat_obj and create_v3_chat_obj['body'] and create_v3_chat_obj["sender"] and create_v3_chat_obj[
            "receiver"]:
            pool.apply_async(insert_data_into_chat_database(create_v3_chat_obj))
    con_v2.close()
    con_v3_chat.close()
    print "============================= script completed ==================================================="
    exit()


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
