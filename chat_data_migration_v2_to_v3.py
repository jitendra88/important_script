# -*- coding: utf8 -*-
import MySQLdb as mdb
import base64
import multiprocessing
import sys
from json import loads, dumps

from openpyxl.workbook import Workbook

from chat_migration_start import user_obj, ws1, wb, dest_filename, delete_message_obj, default_start

default_start()
# ============================================ global object ====================================================


duplicate_msg_id_list = []

# =========================================================================================


# ========================================= duplicate message header ===============================================#
header_1 = [u'messageID', u'senderID', u'receiverID', u'MsgId', u'ChatType', u'body']
message_duplicate_report_data = []
wb1 = Workbook()
dest_filename_message = 'duplicate_message_report.xlsx'
ws2 = wb1.active
ws2.title = "errorMessage"
ws2.append(header_1)
# ====================================== csv wirter ==============================================================



# ======================================== END =====================================================================#

CHAT_TYPE_IMAGE_V2 = 'vImage'
CHAT_TYPE_TEXT_V2 = 'vText'
CHAT_TYPE_IMAGE_V3 = 'chat_image'
CHAT_TYPE_TEXT_V3 = 'chat_text'
PAGINATION_LIMIT = 100000
TOTAL_NO_PROCESS = 1
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

# ====================================================== end  ======================


# ==================================== V2 database ================================================
con_v2 = mdb.connect('chat.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'root', 'admin123*', 'myu')
cur2 = con_v2.cursor(mdb.cursors.DictCursor)
cur2.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur2.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur2.execute("SET character_set_connection=utf8mb4;")  # same as above
# =========================================== end =================================================
# =================================== chat database v3 ===========================================
con_v3_chat = mdb.connect('myu-v3-prod.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'myu_root', 'myu_root123root*', 'myuv3chat')
cur3 = con_v3_chat.cursor(mdb.cursors.DictCursor)
cur3.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur3.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur3.execute("SET character_set_connection=utf8mb4;")  # same as above

# ============================================================end =================================


def insert_data_into_chat_database(data_v3_obj, onlyForTo, onlyForFrom):
    xml = '<message xml:lang="en" to="' + data_v3_obj['to'] + '" from="' + data_v3_obj['from'] + '" type="chat" id="' + \
          data_v3_obj['msg_id'] + '" xmlns="jabber:client"><body>' + data_v3_obj['body'] + '</body><subject>' + \
          data_v3_obj['chat_type'] + '</subject></message>'
    if onlyForFrom:
        query = (
            "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind,msg_id) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s')" % (
                data_v3_obj['sender'], xml, data_v3_obj['to'], data_v3_obj['timestamp'], '', data_v3_obj['to'],
                data_v3_obj['body'],
                'chat',data_v3_obj['msg_id']))
        cur3.execute(query)
    elif onlyForTo:
        query1 = (
            "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind,msg_id) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s')" % (
                data_v3_obj['receiver'], xml, data_v3_obj['from'], data_v3_obj['timestamp'], '', data_v3_obj['from'],
                data_v3_obj['body'],
                'chat',data_v3_obj['msg_id']))
        cur3.execute(query1)

    else:
        query = (
            "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind,msg_id) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s')" % (
                data_v3_obj['sender'], xml, data_v3_obj['to'], data_v3_obj['timestamp'], '', data_v3_obj['to'],
                data_v3_obj['body'],
                'chat',data_v3_obj['msg_id']))

        query1 = (
            "insert into archive (username,xml,bare_peer,timestamp,nick,peer,txt,kind,msg_id) values ('%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s')" % (
                data_v3_obj['receiver'], xml, data_v3_obj['from'], data_v3_obj['timestamp'], '', data_v3_obj['from'],
                data_v3_obj['body'],
                'chat',data_v3_obj['msg_id']))
        cur3.execute(query)
        cur3.execute(query1)


def get_chat_data_from_v2(page):
    start = (page - 1) * PAGINATION_LIMIT;
    counter = 0
    commit = False;

    print "================page  :===" + str(page)
    print "================Pagination Limit  :===" + str(PAGINATION_LIMIT)

    cur2.execute(
        "SELECT fromJID,toJID,sentDate,body,messageID FROM ofMessageArchive WHERE  fromJID != toJID  ORDER BY messageID DESC  limit " + str(
            start) + " ," + str(PAGINATION_LIMIT) + "")
    print "================Total Message count In ofMessageArchive Table :===" + str(cur2.rowcount)

    for row in cur2.fetchall():
        counter = counter + 1
        create_v3_chat_obj = dict()
        onlyForTo = False
        onlyForFrom = False
        try:
            fromJID = str(row['fromJID']).replace("@ip-172-31-42-152", '')
            toJID = str(row['toJID']).replace("@ip-172-31-42-152", '')
            messageID = str(row["messageID"])

            if (toJID in delete_message_obj and (messageID in delete_message_obj[toJID])) and (
                    fromJID in delete_message_obj and (messageID in delete_message_obj[fromJID])):
                data_error_row = list()
                data_error_row.append(row["messageID"])
                data_error_row.append("Chat deleted by this user :::::")
                data_error_row.append(toJID)
                ws1.append(data_error_row)
                continue
            if fromJID in delete_message_obj and (messageID in delete_message_obj[fromJID]):
                onlyForTo = True
            if toJID in delete_message_obj and (messageID in delete_message_obj[toJID]):
                onlyForFrom = True
            if fromJID not in user_obj:
                data_error_row = list()
                data_error_row.append(row["messageID"])
                data_error_row.append("fromJID UserId does exist in V3 database ")
                data_error_row.append(fromJID)
                ws1.append(data_error_row)
                continue
            if toJID not in user_obj:
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
                #create_v3_chat_obj['timestamp'] = str(row['sentDate']) + "000"
                create_v3_chat_obj['body'] = None
                dataBody = loads(base64.b64decode(str(row['body']).encode("utf-8").replace("%2B", "+")))
                if dataBody:
                    # if dataBody['msg_id'] not in for_duplicate_msg_id:
                    create_v3_chat_obj['msg_id'] = dataBody['msg_id']
                    create_v3_chat_obj['timestamp'] = str(dataBody['post_timestamp']) + "000000"
                    # for_duplicate_msg_id[dataBody['msg_id']] = dataBody['msg_id']
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

                        # else:
                        #     data_duplicate_msg_row = list()
                        #     data_duplicate_msg_row.append(row["messageID"])
                        #     data_duplicate_msg_row.append(create_v3_chat_obj['sender'])
                        #     data_duplicate_msg_row.append(create_v3_chat_obj['receiver'])
                        #     data_duplicate_msg_row.append(dataBody['msg_id'])
                        #     if dataBody['chat_type'] == CHAT_TYPE_IMAGE_V2:
                        #         data_duplicate_msg_row.append(CHAT_TYPE_IMAGE_V2)
                        #         data_duplicate_msg_row.append(dataBody['posted_image'])
                        #     elif dataBody['chat_type'] == CHAT_TYPE_TEXT_V2:
                        #         data_duplicate_msg_row.append(CHAT_TYPE_TEXT_V2)
                        #         data_duplicate_msg_row.append(dataBody['chat_msg'])
                        #     ws2.append(data_duplicate_msg_row)
                        #     duplicate_msg_id_list.append(for_duplicate_msg_id[dataBody['msg_id']])
                        #     continue

        except Exception as e:
            data_error_row = list()
            data_error_row.append(row["messageID"])
            data_error_row.append(str(e.message))
            data_error_row.append(str(row['body']))
            ws1.append(data_error_row)
            continue
        if create_v3_chat_obj is not None and create_v3_chat_obj['body'] != None and create_v3_chat_obj['body'] != '':
            if counter == 5000:
                counter = 0
                con_v3_chat.commit()
            try:
                insert_data_into_chat_database(create_v3_chat_obj, onlyForTo, onlyForFrom)
            except Exception as e:
                data_error_row = list()
                data_error_row.append(row["messageID"])
                data_error_row.append(str(e.message))
                data_error_row.append(str(row['body']))
                ws1.append(data_error_row)
                continue

        else:
            data_error_row = list()
            data_error_row.append(row["messageID"])
            data_error_row.append("Body data Null ")
            data_error_row.append(str(row['body']))
            ws1.append(data_error_row)
            continue
    con_v3_chat.commit()
    con_v2.close()
    con_v3_chat.close()
    # print "============================= duplicate message count is :" + str(len(duplicate_msg_id_list))
    print "============================= script completed ==================================================="
    wb.save(str(page) + "____" + dest_filename)
    # wb1.save(filename=dest_filename_message)

    print "============================= please check error report file ==========================================="
    exit()

get_chat_data_from_v2(page)
