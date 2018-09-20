#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import common_dbs
import csv


# 导入数据库类
USER_PASSPORT_DB = common_dbs.USER_PASSPORT_DB
PRODUCT_BID_DB = common_dbs.PRODUCT_BID_DB
#TRADE_DB = common_dbs.TRADE_DB
BID_DB = common_dbs.BID_DB
#gmatch = iter_gmatch.gmatch
#valid = "INSERT"


def conver_file(input_file, output_file):
    with open(input_file, 'r') as fin:
        with open(output_file,"w") as datacsv:
            csvwriter = csv.writer(datacsv,delimiter=",",
                                doublequote=False,
                                escapechar='\\', 
                                quotechar="'",
                                strict=True)
            reader = csv.reader(fin, delimiter="\t", 
                                doublequote=False,
                                escapechar='\\', 
                                quotechar="'",
                                strict=True)
            next(reader)
            for input_arr in reader:
                output_arr = list(range(25))
                output_arr[1] = input_arr[0]
                # uid(c_lender_id)
                output_arr[3] = input_arr[3]
                output_arr[2] = USER_PASSPORT_DB.fetch_from_salt(
                    output_arr[3])
                # borrower_uid(c_borrower_id)
                output_arr[5] = input_arr[2]
                output_arr[4] = USER_PASSPORT_DB.fetch_from_salt(
                    output_arr[5])
                # guarantee_uid(c_guarantee_id)
                output_arr[7] = input_arr[4]
                output_arr[6] = PRODUCT_BID_DB.fetch_from_origin_id(
                    output_arr[7])
                # relation_type
                output_arr[8] = "1"
                # relation_id(c_relation_id)
                output_arr[10] = input_arr[1]
                output_arr[9] = BID_DB.fetch_from_origin_id(
                    output_arr[10])
                output_arr[11:13] = ["NULL", "NULL"]
                # guarantee_amount
                try:
                    output_arr[13] = int(float(input_arr[6]) * 100 + 0.5)
                except ValueError:
                    output_arr[13] = input_arr[6]
                # amount
                try:
                    output_arr[14] = int(float(input_arr[7]) * 100 + 0.5)
                except ValueError:
                    output_arr[14] = input_arr[7]
                # online_status,valid_status,end_status
                output_arr[15] = input_arr[9]
                output_arr[16] = input_arr[8]
                output_arr[17] = input_arr[10]
                # receive_time(t_rcv_tm)
                output_arr[19] = input_arr[14]
                output_arr[18] = datetime2int(output_arr[19])
                # reconciliation_status
                output_arr[20] = input_arr[11]
                # reconciliation_time(t_reconciliation_tm)
                output_arr[22] = input_arr[15]
                output_arr[21] = datetime2int(output_arr[22])
                # create_time
                output_arr[23] = datetime2timestamp(input_arr[13])
                # update_time
                output_arr[24] = str(output_arr[23])
                del output_arr[0]
                csvwriter.writerow(output_arr)
            



start_time = time.time()
conver_file("t_bid_list.csv",
            "/home/t_bid_list_out.csv")
end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
