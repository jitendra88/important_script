import MySQLdb as mdb
import commands
from openpyxl.workbook import Workbook

user_obj = {}
delete_message_obj = {}


def default_start():

    print "======================== chat migration start==================================================\N"
    print "jitendra kumar dixit"
    # ---------------------------------- V3 ---------------------------------------------------
    con_v3 = mdb.connect('beta-v3-1.ctjt9fapuyu7.eu-west-1.rds.amazonaws.com', 'root', 'myuroot123', 'myuv3');
    cur_3 = con_v3.cursor(mdb.cursors.DictCursor)
    cur_3.execute('SELECT id,idV2 FROM users WHERE  idV2 is NOT  NULL')

    print "===================use object creation started ................."
    for row in cur_3.fetchall():
        idV2 = str(row["idV2"])
        idV3 = str(row["id"])
        user_obj[idV2] = idV3
    con_v3.close()
    print "=======================end======================"


    # ---------------------------------- V2 ---------------------------------------------------
    con_v2 = mdb.connect('beta-v2.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'root', 'admin123*', 'myu');
    cur_2 = con_v2.cursor(mdb.cursors.DictCursor)
    cur_2.execute('SELECT message_id,user_id FROM deleted_messages ')
    print "===================delete message object creation started ................."
    print "================= count delete message====================================="+str(cur_2.rowcount)
    for row in cur_2.fetchall():
        message_id = str (row["message_id"])
        user_id = str(row["user_id"])
        if user_id in delete_message_obj:
            message_id_list = delete_message_obj[user_id]
            message_id_list.append(message_id)
            delete_message_obj[user_id]= message_id_list
        else:
            message_id_list =list()
            message_id_list.append(message_id)
            delete_message_obj[user_id] = message_id_list
        # message_id_check = message_id+user_id
        # delete_message_obj[message_id_check] = user_id
    con_v2.close()
    print "=====================length of delete message object========================="+str(len(delete_message_obj))
    print "=======================end======================"




# ============================================ xlsx   error reporter ==============================================#
header = [u'messageID', u'errorMessage', u'body']
error_report_data = []
wb = Workbook()
dest_filename = 'error_report_chat_message.xlsx'
ws1 = wb.active
ws1.title = "errorMessage"
ws1.append(header)


# ================================ end ============================================================================#




