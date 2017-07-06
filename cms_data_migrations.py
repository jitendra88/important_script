import MySQLdb as mdb
import requests
from uploadForm import UploadForm
import urllib
filepath ="/tmp/300x250-flight-23042015.jpg"
import os
media_upload_url ="http://beta-api.myu.co/mediaservice/api/v3/media/directs3"

tab_v2_v3={
    "1":"4",
    "2":"5",
    "3":"7"

}

permision_obj_list = [{
    "id":1,
},{
    "id":2,
},{
    "id":3,
},{
    "id":4,
},{
    "id":5,
},{
    "id":6,
},{
    "id":7,
},{
    "id":8,
},{
    "id":9,
},{
    "id":10,
}

]
# ==================================== V2 database ================================================
con_v2 = mdb.connect('beta-v2.c03pbdmxnxpo.eu-west-1.rds.amazonaws.com', 'root', 'admin123*', 'myu')
cur2 = con_v2.cursor(mdb.cursors.DictCursor)
cur2.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur2.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur2.execute("SET character_set_connection=utf8mb4;")  # same as above
# =========================================== end =================================================
# =================================== chat database v3 ===========================================
con_v3_cms = mdb.connect('beta-v3-1.ctjt9fapuyu7.eu-west-1.rds.amazonaws.com', 'root', 'myuroot123', 'cms_user');
cur3 = con_v3_cms.cursor(mdb.cursors.DictCursor)
cur3.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur3.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur3.execute("SET character_set_connection=utf8mb4;")  # same as above


# ============================================================end =================================


def getDataFromV2SchoolAdmin():
    data_v3_obj ={}
    cur2.execute('SELECT id,name,email,password,profile_pic FROM school_admin where id=16')
    print "===================use object creation started ................."
    for row in cur2.fetchall():
        cur2.execute('SELECT university_id FROM school_admin_university where school_admin_id='+str(row['id']))
        univesity_ids = cur2.fetchall()
        cur2.execute('SELECT *  FROM school_admin_menu_access where school_admin_id='+str(row['id']))
        permission_list = cur2.fetchall()
        data_v3_obj["first_name"] = row["name"]
        data_v3_obj["username"] = row["email"]
        data_v3_obj["email"] = row["email"]
        data_v3_obj["password"] = row["password"]
        data_insert(data_v3_obj,univesity_ids,permission_list)
        #print permission_list


def data_insert_cms_uni(userId,univesity_ids):
    for univesity_id in univesity_ids:
        query = (
            "insert into school_admin_university_mappings (userId,universityId) values ('%s', '%s')" % (
                userId,univesity_id['university_id']))
        cur3.execute(query)

    con_v3_cms.commit()


def data_insert_cms_permission(userId,permission_list):
    insert_id= list()
    for perms_obj in permision_obj_list:
        if perms_obj['id'] in insert_id:
            continue;
        for perms in permission_list:
            try:
                if str(perms_obj["id"]) == str(perms["school_menu_id"]) and str(perms["school_menu_id"]) != '4' :
                     change_id = tab_v2_v3[str(perms["school_menu_id"])]
                     query = (
                         "insert into master_users_permission_tab_mappings (userId,permissionTypeId,canView,canAdd,canedit,candelete) values ('%s', '%s','%s', '%s','%s', '%s')" % (
                             userId,change_id,perms["read"],perms["create"],perms["update"],perms["delete"]))
                     query1 = (
                         "insert into master_users_permission_tab_mappings (userId,permissionTypeId,canView,canAdd,canedit,candelete) values ('%s', '%s','%s', '%s','%s', '%s')" % (
                             userId,perms_obj["id"],0,0,0,0))
                     cur3.execute(query)
                     cur3.execute(query1)
                     insert_id.append(perms["school_menu_id"])
                     break
                elif perms_obj["id"] == 10 :
                    query = (
                        "insert into master_users_permission_tab_mappings (userId,permissionTypeId,canView,canAdd,canedit,candelete) values ('%s', '%s','%s', '%s','%s', '%s')" % (
                            userId,perms_obj["id"],1,1,1,1))
                    cur3.execute(query)
                else:
                    query = (
                        "insert into master_users_permission_tab_mappings (userId,permissionTypeId,canView,canAdd,canedit,candelete) values ('%s', '%s','%s', '%s','%s', '%s')" % (
                            userId,perms_obj["id"],0,0,0,0))
                    cur3.execute(query)
            except Exception as e:
                print e.message
    con_v3_cms.commit()



def data_insert_cms_user(userId,univesity_ids,permission_list):
    query = (
        "insert into cms_user (userTypeId,userId) values ('%s', '%s')" % (
            1,userId))
    cur3.execute(query)
    con_v3_cms.commit()
    userId_in = cur3.lastrowid
    data_insert_cms_uni(userId_in,univesity_ids)
    data_insert_cms_permission(userId_in,permission_list)


def data_insert(data_v3_obj,univesity_ids,permission_list):
    try:
        query = (
            "insert into auth_user (first_name,email,username,password,is_active) values ('%s', '%s', '%s', '%s','%s')" % (
                data_v3_obj['first_name'],data_v3_obj['email'],data_v3_obj['username'],data_v3_obj['password'],1))
        cur3.execute(query)
        con_v3_cms.commit()
        print cur3.lastrowid

    except Exception as e:
        print e.message
    if cur3.lastrowid:
        data_insert_cms_user(cur3.lastrowid,univesity_ids,permission_list)






# def media_upload_admin(url, file_path,file_name):
#     try:
#         form = UploadForm()
#         with open(file_path, 'rb+') as f:
#             file_name = str(file_name)
#             form.add_field('media', (f, os.path.getsize(file_path)), filename=file_name)
#             field = form.add_field('upload_file', u'This is the content of upload file\'s text file.\n', filename=file_name)
#             field.add_header('Content-Type', 'text/plain', charset='UTF-8')
#             form.add_field("mediaFor", "cmsUserProfile")
#             form.add_field("mediaType", "image")
#             headers, content_length = form.get_request_headers()
#             headers['Myu-Auth-Token'] = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOjY4NDgyLCJhdWRpZW5jZSI6IndlYiIsImNyZWF0ZWQiOjE0OTkyNTc2MDU1ODUsImlkIjoiZTAxNWZlNDctYmViYy00NzcyLWE3MDktMTQ3MTg2NDcyMDY1IiwiZXhwIjoyMTA0MDU3NjA1fQ.to_jlmsuJU_zsoKGSwQAWQXVfBGdjnyu2lY9Td4fpkk"
#             content = str(form)
#             f.close()
#     except Exception as e:
#         print e.message
#
#     response = requests.post(url, data=content,headers=headers)
#     print response
#     return response



#media_upload_admin(media_upload_url,filepath,"62d19757d37270e2be83bfe08b329be0")
getDataFromV2SchoolAdmin();