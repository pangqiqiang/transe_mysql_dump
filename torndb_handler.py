#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb


class MyDB:
    def __init__(self, host, database, user, password, tablename, connect_timeout=10):
        try:
            self.db = torndb.Connection(host=host,
                                        database=database,
                                        user=user,
                                        password=password
                                        )
        except Exception as e:
            print(str(e))
        self.tablename = tablename

    def fetch_from_salt(self, salt):
        if salt == "NULL" or len(salt) == 0:
            return "NULL"
        sql = "SELECT uid FROM " + self.tablename + " WHERE salt=%s"
        res = self.db.query(sql, salt)
        res = res[0].uid if res else "NULL"
        return res

    def fetch_from_origin_id(self, origin_id):
        if origin_id == "NULL" or len(origin_id) == 0:
            return "NULL"
        sql = "SELECT id FROM " + self.tablename + " WHERE original_id=%s"
        res = self.db.query(sql, origin_id)
        res = res[0].id if res else "NULL"
        return res

    def get_max_id(self):
        sql = "SELECT MAX(id) id FROM " + self.tablename
        res = self.db.get(sql)
        res = res.id if res else 0
        return res

    def get_max_uid(self):
        sql = "SELECT MAX(uid) uid FROM " + self.tablename
        res = self.db.get(sql)
        res = res.uid if res else 0
        return res


if __name__ == "__main__":
    MYDB = MyDB(host="10.111.30.20:3306",
                database="jjd_9th", user="dev",
                password="KRkFcVCbopZbS8R7",
                tablename="loan_installment_list")

    print(MYDB.fetch_from_origin_id("201805181844316221"))
