#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import common_dbs


SEP = os.linesep
# 导入数据库类
USER_PASSPORT_DB = common_dbs.USER_PASSPORT_DB
PRODUCT_BID_DB = common_dbs.PRODUCT_BID_DB
TRADE_DB = common_dbs.TRADE_DB
gmatch = iter_gmatch.gmatch
valid = "INSERT"


def conver_file(input_file, output_file, valid):
    # 维护自增id
    seq_count = 50
    with open(input_file, 'r') as fin:
        with open(output_file, 'w') as fout:
            for line in fin:
                if not line.startswith(valid):
                    continue
                line.rstrip()
                pre_pos = line.find("VALUES")
                if pre_pos == -1:
                    continue
                post = line[(pre_pos + 1):]
                pre = line[:(pre_pos + 1 + len("VALUES"))
                           ].replace("t_product_bid_list", "loan_pre_pay_list")
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    # 维护自增id
                    seq_count += 1
                    # 输出映射数组
                    output_arr = list(range(25))
                    # id
                    output_arr[0] = "(" + str(seq_count)
                    item = item.strip(",")
                    input_arr = item.split(",")
                    # original_id
                    output_arr[1] = input_arr[0].lstrip("(")
                    # uid(c_lender_id)
                    output_arr[3] = input_arr[3]
                    output_arr[2] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[3].strip("'"))
                    # borrower_uid(c_borrower_id)
                    output_arr[5] = input_arr[2]
                    output_arr[4] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[5].strip("'"))
                    # guarantee_uid(c_guarantee_id)，空
                    output_arr[6:8] = ["NULL", "NULL"]
                    # relation_type
                    output_arr[8] = "2"
                    # relation_id(c_relation_id)
                    output_arr[10] = input_arr[1]
                    output_arr[9] = PRODUCT_BID_DB.fetch_from_origin_id(
                        output_arr[10].strip("'"))
                    # trade_id,c_trade_id
                    output_arr[12] = input_arr[2]
                    output_arr[11] = TRADE_DB.fetch_from_origin_id(
                        output_arr[12].strip("'"))
                    # guarantee_amount空
                    output_arr[13] = "NULL"
                    # amount
                    try:
                        output_arr[14] = int(float(input_arr[5]) * 100 + 0.5)
                    except ValueError:
                        output_arr[14] = input_arr[5]
                    # online_status,valid_status,end_status
                    output_arr[15] = input_arr[6]
                    output_arr[16] = input_arr[7]
                    output_arr[17] = input_arr[8]
                    # receive_time(t_rcv_tm)
                    output_arr[19] = input_arr[12]
                    output_arr[18] = datetime2int(output_arr[19])
                    # reconciliation_status
                    output_arr[20] = input_arr[9]
                    # reconciliation_time(t_reconciliation_tm)
                    output_arr[22] = input_arr[13].rstrip(")")
                    output_arr[21] = datetime2int(output_arr[22])
                    # create_time
                    output_arr[23] = datetime2int(input_arr[11])
                    # update_time
                    output_arr[24] = str(output_arr[23]) + ")"
                    new_values.append(
                        ",".join([str(i) for i in output_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


start_time = time.clock()
conver_file("t_product_bid_list.sql",
            "/tmp/t_product_bid_list_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
