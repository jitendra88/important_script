# -*- coding: utf8 -*-
import MySQLdb as mdb
import multiprocessing

con_v2 = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_26042017_0')
cur_v2 = con_v2.cursor(mdb.cursors.DictCursor)
cur_v2.execute('SELECT * FROM user ORDER BY  id DESC limit 10000')
#
# con_v3 = mdb.connect('148.251.178.194', 'jitendra', 'jitendra', 'myu_v3')
# cur_v3 = con_v2.cursor(mdb.cursors.DictCursor)


def insert_data(table, data_dict):
    columns_string = '(' + ','.join(data_dict.keys()) + ')'
    values_string = '(' + ','.join(map(str, data_dict.values())) + ')'
    sql = """INSERT INTO %s %s
        VALUES %s""" % (table, columns_string, values_string)
    print sql


def load_file(user_v2_row):
    user_v3_row = dict()
    try:
        if user_v2_row['university'] == 0 or user_v2_row['university'] == "":
            return
        if user_v2_row['password'] == "":
            return
        if str(user_v2_row["username"]).strip() == "" or not user_v2_row["username"]:
            return

        if str(user_v2_row['email']).strip() != "":
            user_v3_row["email"] = user_v2_row["email"]
        else:
            user_v3_row["email"] = user_v2_row["university_email"]
        user_v3_row['username'] = str(user_v2_row['username']).strip()
        user_v3_row['idV2'] = user_v2_row['id']
        user_v3_row['universityId'] = user_v2_row['university']
        user_v3_row['bio'] = user_v2_row['bio']

        # *********************************** user type###################################################
        if user_v2_row["faculty"] == "yes":
            user_v3_row["userTypeId"] = 1
        elif user_v2_row["faculty"] == "no":
            user_v3_row["userTypeId"] = 2
        elif user_v2_row["faculty"] == "club_student":
            user_v3_row["userTypeId"] = 3
        if user_v2_row["gender"] == u'سلام':
            user_v2_row["gender"] = "Male"
        elif user_v2_row["gender"] == u'أنثى':
            user_v2_row["gender"] = "Female"
        elif user_v2_row["gender"] == "Not selected":
            user_v2_row["gender"] = ""

        user_v3_row["gender"] = user_v2_row["gender"]
        user_v3_row["major"] = user_v2_row["major"]
        try:
            user_v3_row["graduationYear"] = int(user_v2_row["graduation_year"])
        except:
            user_v3_row["graduationYear"] = 0
        user_v3_row["canAddNews"] = user_v2_row["can_add_news"]
        user_v3_row["receivePrivateMsgNotification"] = user_v2_row["private_message_notification"]
        user_v3_row["receivePrivateMsg"] = user_v2_row["receive_private_messages"]
        user_v3_row["receiveCommentNotification"] = user_v2_row["comment_notification"]
        user_v3_row["receiveLikeNotification"] = user_v2_row["like_notification"]
        user_v3_row["receiveFavoriteFollowNotification"] = user_v2_row["favorite_and_follow_notification"]
        user_v3_row["receiveNewPostNotification"] = user_v2_row["new_post_notification"]
        user_v3_row["allowInPopularList"] = user_v2_row["allow_in_popular_list"]
        user_v3_row["xmppResponse"] = user_v2_row["xmpp_response"]
        user_v3_row["xmppDatetime"] = str(user_v2_row["xmpp_datetime"])
        if user_v2_row["xmpp_datetime"] == "0000-00-00 00:00:00":
            user_v3_row["xmppDatetime"] = str(user_v2_row["date_created"])

        user_v3_row["lastPasswordReset"] = ""
        user_v3_row["authorities"] = "ADMIN, ROOT"
        user_v3_row["status"] = user_v2_row["is_deactivated"]
        user_v3_row["deactivatedByAdmin"] = user_v2_row["is_active"]
        user_v3_row["userVerified"] = user_v2_row["is_verified"]
        user_v3_row["createdAt"] = str(user_v2_row["date_created"])
        user_v3_row["modifiedAt"] = str(user_v2_row["amazon_updated_at"])
        if user_v3_row["modifiedAt"] == "0000-00-00 00:00:00":
            user_v3_row["modifiedAt"] = str(user_v2_row["date_created"])
        user_v3_row["updatedBy"] = ""
        table = 'users'
        insert_data(table,user_v3_row)
    except Exception as e:
        print e.message

    print "\n"


pool = multiprocessing.Pool(processes=2 * multiprocessing.cpu_count())
for user_v2_row in cur_v2.fetchall():
    pool.apply_async(load_file(user_v2_row))

pool.close()
pool.join()



