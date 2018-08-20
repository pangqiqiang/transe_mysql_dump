import torndb_handler

MYDB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                           database="dts_jjd", user="dev",
                           password="KRkFcVCbopZbS8R7",
                           tablename="user_passport")


print(MYDB.fetch_from_salt("201611210122113267"))
