#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler

HOST = "prod.jjd.mysql01.srv:3308"
DATABASE = "jjd_6th"
USER = "dev"
PASS = "KRkFcVCbopZbS8R7"

LOAN_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="loan")

LOAN_INSTALLMENT_LIST_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                               user=USER, password=PASS,
                                               tablename="loan_installment_list")

LOAN_WRITE_OFF_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="loan_write_off")

USER_PASSPORT_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="user_passport")

TRADE_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="trade")

COLLECTION_ACCOUNT_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                            user=USER, password=PASS,
                                            tablename="collection_account")

COLLECTION_APPLY_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                          user=USER, password=PASS,
                                          tablename="collection_apply")


LOAN_OFFLINE_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="loan_offline")

PRODUCT_BID_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                     user=USER, password=PASS,
                                     tablename="product_bid")

BID_DB = torndb_handler.MyDB(host=HOST, database=DATABASE,
                             user=USER, password=PASS,
                             tablename="bid")
