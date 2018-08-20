#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb


class MyDB:
    def __init__(self, host, database, user, password, tablename):
        self.db = torndb.Connection(host=host,
                                    database=database,
                                    user=user,
                                    password=password
                                    )
        self.tablename = tablename

    def fetch_from_salt(self, salt):
        sql = "SELECT uid FROM " + self.tablename + " WHERE salt=%s"
        res = self.db.get(sql, salt)
        res = res.uid if res else None
        return res

    def fetch_from_origin_id(self, origin_id):
        sql = "SELECT id FROM " + self.tablename + " WHERE original_id=%s"
        res = self.db.get(sql, origin_id)
        res = res.id if res else None
        return res
