#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
from config import *
# 尽量加快，采用一个线程一个实例
LOAN_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")
LOAN_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                               user=USER, password=PASS,
                               tablename="loan")


LOAN_INSTALLMENT_LIST_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")
LOAN_INSTALLMENT_LIST_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                                user=USER, password=PASS,
                                                tablename="loan_installment_list")


LOAN_WRITE_OFF_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")
LOAN_WRITE_OFF_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                         user=USER, password=PASS,
                                         tablename="loan_write_off")


USER_PASSPORT_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")
USER_PASSPORT_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                        user=USER, password=PASS,
                                        tablename="user_passport")


TRADE_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")
TRADE_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                user=USER, password=PASS,
                                tablename="trade")


COLLECTION_ACCOUNT_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")
COLLECTION_ACCOUNT_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                             user=USER, password=PASS,
                                             tablename="collection_account")


LOAN_OFFLINE_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")
LOAN_OFFLINE_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                       user=USER, password=PASS,
                                       tablename="loan_offline")


PRODUCT_BID_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")
PRODUCT_BID_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                      user=USER, password=PASS,
                                      tablename="product_bid")


BID_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
BID_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                              user=USER, password=PASS,
                              tablename="bid")
COLLECTION_APPLY_DB0 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB1 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB2 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB3 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB4 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB5 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB6 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB7 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB8 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
COLLECTION_APPLY_DB9 = torndb_handler.MyDB(host=HOST, database=DATABASE,
                                           user=USER, password=PASS,
                                           tablename="collection_apply")
