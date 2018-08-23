#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler

LOAN_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                              database="dts_jjd", user="dev",
                              password="KRkFcVCbopZbS8R7",
                              tablename="loan")

LOAN_INSTALLMENT_LIST_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                               database="dts_jjd", user="dev",
                                               password="KRkFcVCbopZbS8R7",
                                               tablename="loan_installment_list")

LOAN_WRITE_OFF_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                        database="dts_jjd", user="dev",
                                        password="KRkFcVCbopZbS8R7",
                                        tablename="loan_write_off")

USER_PASSPORT_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                       database="dts_jjd", user="dev",
                                       password="KRkFcVCbopZbS8R7",
                                       tablename="user_passport")

TRADE_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                               database="dts_jjd", user="dev",
                               password="KRkFcVCbopZbS8R7",
                               tablename="trade")

COLLECTION_ACCOUNT_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                            database="dts_jjd", user="dev",
                                            password="KRkFcVCbopZbS8R7",
                                            tablename="collection_account")

COLLECTION_APPLY_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                          database="dts_jjd", user="dev",
                                          password="KRkFcVCbopZbS8R7",
                                          tablename="collection_apply")


LOAN_OFFLINE_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                      database="dts_jjd", user="dev",
                                      password="KRkFcVCbopZbS8R7",
                                      tablename="loan_offline")

PRODUCT_BID_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                     database="dts_jjd", user="dev",
                                     password="KRkFcVCbopZbS8R7",
                                     tablename="product_bid")

BID_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                             database="dts_jjd", user="dev",
                             password="KRkFcVCbopZbS8R7",
                             tablename="bid")
