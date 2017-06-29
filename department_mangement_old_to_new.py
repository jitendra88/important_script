import csv
import MySQLdb as mdb

con_v3 = mdb.connect('beta-v3-1.ctjt9fapuyu7.eu-west-1.rds.amazonaws.com', 'root', 'myuroot123', 'myuv3');
cur3 = con_v3.cursor(mdb.cursors.DictCursor)
cur3.execute("SET NAMES utf8mb4;")  # or utf8 or any other charset you want to handle

cur3.execute("SET CHARACTER SET utf8mb4;")  # same as above

cur3.execute("SET character_set_connection=utf8mb4")

path = 'department_sheet.csv'
file = open(path, "r")
reader = csv.reader(file)
for line in reader:
    try:
        if line[10] == "yes":
            name = str(line[2])
            id = str(line[0])
            id_old = str(line[1])
            sql = " UPDATE users SET departmentId = (%s) WHERE departmentId=(%s)"
            query1 = "DELETE FROM master_departments WHERE id='%s'" % id_old
            print ("department_id :"+id_old)
            cur3.execute(sql , (None,id_old))
            cur3.execute(query1)
            con_v3.commit()
            continue
        if line[0] != line[1]:
            name = str(line[2])
            id = str(line[0])
            id_old = str(line[1])
            sql = " UPDATE users SET departmentId = (%s) WHERE departmentId=(%s)"
            query1 = "DELETE FROM master_departments WHERE id='%s'" % id_old
            print ("department_id :"+id_old)
            cur3.execute(sql , (id,id_old))
            cur3.execute(query1)
            con_v3.commit()
            continue
        if line[0] == line[1]:
            name = str(line[2])
            id = str(line[0])
            sql = "UPDATE master_departments SET name = (%s) WHERE id=(%s)"
            cur3.execute(sql,(name,id))
            con_v3.commit()
            print ("department_id :"+id)
            continue
    except Exception as e:
        print  e.message


print "complete ..........................."


con_v3.close()






