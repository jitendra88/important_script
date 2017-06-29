import MySQLdb as mdb
import commands
from openpyxl.workbook import Workbook

user_obj = {}
v2_user_university_obj = {}


def default_start():

    # ---------------------------------- V2 ---------------------------------------------------
    con_v2 = mdb.connect('beta-php.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'root', 'admin123*', 'myu');
    cur_2 = con_v2.cursor(mdb.cursors.DictCursor)
    cur_2.execute('SELECT id,university FROM user ')
    for row in cur_2.fetchall():
        university = str (row["university"])
        id = str(row["id"])
        v2_user_university_obj[id]= university
    con_v2.close()
    print "=====================v2 user le  ========================="+str(len(v2_user_university_obj))
    print "=======================end======================"

    # ---------------------------------- V3 ---------------------------------------------------

    con_v3 = mdb.connect('beta-v3-1.ctjt9fapuyu7.eu-west-1.rds.amazonaws.com', 'root', 'myuroot123', 'myuv3');
    cur_3 = con_v3.cursor(mdb.cursors.DictCursor)
    cur_3.execute('SELECT universityId,idV2 FROM users WHERE  idV2 is NOT  NULL')

    print "===================use object creation started ................."
    for row in cur_3.fetchall():
        idV2 = str(row["idV2"])
        universityId = str(row["universityId"])
        if idV2 in v2_user_university_obj and v2_user_university_obj[idV2] != universityId:
            print ("================User id V2  not correct mapping university "+ idV2)

    con_v3.close()
    print "=======================end======================"

default_start()








