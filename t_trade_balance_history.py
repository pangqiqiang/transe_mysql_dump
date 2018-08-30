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
PASSWORD_DB = common_dbs.USER_PASSPORT_DB
gmatch = iter_gmatch.gmatch
valid = "INSERT"


def conver_file(input_file, output_file, valid):
    # 维护自增id
    seq_count = 6728223
    with open(input_file, 'r') as fin:
        with open(output_file, 'w') as fout:
            for line in fin:
                if not line.startswith(valid):
                    continue
                line = unescape_quote(line)
                # print(line)
                pre_pos = line.find("VALUES")
                if pre_pos == -1:
                    continue
                post = line[(pre_pos + 1):]
                pre = line[:(pre_pos + 1 + len("VALUES"))
                           ].replace("t_trade_balance_history", "trade")
                new_values = []
                for item in gmatch(line, "(", "),", pre_pos):
                        # 输出映射数组
                    out_arr = list(range(25))
                    # 维护自增id
                    seq_count += 1
                    out_arr[0] = "(" + str(seq_count)
                    item = item.strip(",")
                    input_arr = parse_sql_fields(item)
                    # original_id
                    out_arr[1] = input_arr[0].lstrip("(")
                    # pay_order_no
                    out_arr[2] = "NULL"
                    # trade_type
                    out_arr[3] = "2"
                    # uid,c_user_id
                    out_arr[5] = input_arr[3]
                    out_arr[4] = PASSWORD_DB.fetch_from_salt(
                        out_arr[5])
                    # bank_account
                    out_arr[6] = "NULL"
                    # withdraw_type(int->bit)
                    out_arr[7] = "b'0'"
                    # relation_type,reserve_data
                    out_arr[8] = input_arr[2]
                    out_arr[9] = "NULL"
                    # amount,fee_amount
                    out_arr[10] = float_char_to_int(input_arr[3])
                    out_arr[11] = "0"
                    #send_time, t_send_tm
                    out_arr[14] = "NULL"
                    out_arr[13] = "NULL"
                    # b_valid
                    out_arr[15] = input_arr[4]
                    # b_rcv_bank
                    out_arr[16] = "0"
                    # receive_time,t_rcv_tm
                    out_arr[18] = input_arr[6]
                    out_arr[17] = datetime2timestamp(out_arr[18])
                    # trade_status
                    if (out_arr[15] == 1 and out_arr[18] != "NULL"):
                        out_arr[12] = 3
                    elif (out_arr[15] == 0 and out_arr[18] != "NULL"):
                        out_arr[12] = 4
                    else:
                        out_arr[12] = 2
                    # reconciliation_status
                    out_arr[19] = "b'" + input_arr[5] + "'"
                    # reconciliation_time,t_reconciliation_tm
                    out_arr[21] = input_arr[7]
                    out_arr[20] = datetime2timestamp(out_arr[21])
                    # lease_status
                    out_arr[22] = "b'0'"
                    # create_time,update_time
                    out_arr[23] = input_arr[8].rstrip(")")
                    out_arr[24] = get_cur_time_str() + ")"
                    new_values.append(
                        ",".join([str(i) for i in out_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + post + ";" + SEP)


start_time = time.time()
conver_file("t_trade_balance_history.sql",
            "/home/pangqiqiang/t_trade_balance_history_out.sql", valid)
end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
