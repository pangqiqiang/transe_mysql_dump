#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import common_dbs

SEP = os.linesep

LOAN_DB = common_dbs.LOAN_DB

USER_PASSPORT_DB = common_dbs.USER_PASSPORT_DB

PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}


# 导入迭代器函数
gmatch = iter_gmatch.gmatch
# 数据有效行必须以INSERT 开头
valid = "INSERT"
COLS = '("original_id","cur_period","total_period","loan_id","borrower_uid","lender_uid", \
"guarantee_uid","overdue_rate","total_amount","amount","interest_amount","forfeit_amount", \
"overdue_manage_amount","overdue_manage_amount_special","return_overdue_manage_amount", \
"get_amount","got_amount","paid_amount","paid_interest_amount","paid_forfeit_amount",\
"paid_overdue_manage_amount","purpose","repay_time","normal_repay_amount","online_status", \
"end_status","overdue_status","valid_status","version_number","payoff_time")'



def conver_file(input_file, output_file, valid):
    with open(input_file, 'r') as fin:
        with open(output_file, 'w') as fout:
            for line in fin:
                if not line.startswith(valid):
                    continue
                line = unescape_quote(line)
                pre_pos = line.find("VALUES")
                if pre_pos == -1:
                    continue
                post = line[(pre_pos + 1):]
                pre = line[:pre_pos].replace("t_iou_installment_list", "loan_installment_list")
                new_values = []
                for item in gmatch(line, "(", "),", pre_pos):
                    # 输出映射数组
                    output_arr = list(range(40))
                    item = item.strip(",")
                    input_arr = parse_sql_fields(item)
                    # Original_id,[1],cur_period,total_period,
                    output_arr[1] = input_arr[0]
                    # cur_period,total_period
                    output_arr[2] = input_arr[1]
                    output_arr[3] = input_arr[2]
                    # loan_id
                    output_arr[5] = input_arr[3]
                    output_arr[4] = LOAN_DB.fetch_from_origin_id(output_arr[5])
                    #,c_iou_id,borrower_uid,c_borrower_id,
                    # lender_uid,c_lender_id,guarantee_uid,c_guarantee_id
                    output_arr[7] = input_arr[4]
                    output_arr[6] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[7])
                    output_arr[9] = input_arr[5]
                    output_arr[8] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[9])
                    output_arr[11] = input_arr[6]
                    output_arr[10] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[11])
                    # overdue_rate
                    output_arr[12] = input_arr[7]
                    # total_amount
                    output_arr[13] = float_char_to_int(input_arr[17])
                    # interest_amount,forfeit_amount,overdue_manage_amount
                    # overdue_manage_amount_special,return_overdue_manage_amount
                    for i in range(14, 19):
                        output_arr[i] = float_char_to_int(input_arr[i - 6])
                    # return_overdue_manage_amount
                    output_arr[19] = float_char_to_int(input_arr[18])
                    # get_amount
                    output_arr[20] = float_char_to_int(input_arr[22])
                    # got_amount
                    output_arr[21] = float_char_to_int(input_arr[21])
                    # paid_amount
                    output_arr[22] = float_char_to_int(input_arr[14])
                    # paid_interest_amount
                    output_arr[23] = float_char_to_int(input_arr[15])
                    # paid_forfeit_amount
                    output_arr[24] = float_char_to_int(input_arr[16])
                    # paid_overdue_manage_amount
                    output_arr[25] = float_char_to_int(input_arr[13])
                    # Purpose,c_purpose
                    output_arr[27] = input_arr[19]
                    output_arr[26] = get_from_map(
                        PURPOSE_TYPE_MAP, output_arr[27])
                    output_arr[26] = 8 if output_arr[26] == None else output_arr[26]
                    # repay_time,t_repay_tm
                    output_arr[29] = input_arr[29]
                    output_arr[28] = date2int(output_arr[29])
                    # normal_repay_amount
                    output_arr[30] = float_char_to_int(input_arr[20])
                    # online_status,end_status,overdue_status
                    for i in range(31, 34):
                        output_arr[i] = input_arr[i - 8]
                    # valid_status
                    output_arr[34] = "b'1'"
                    # version_number
                    output_arr[35] = input_arr[28]
                    # payoff_time,t_pay_off_tm
                    output_arr[37] = input_arr[30]
                    output_arr[36] = date2int(output_arr[37])
                    #create_time, update_time
                    output_arr[38] = datetime2timestamp(input_arr[31])
                    output_arr[39] = str(datetime2timestamp(
                        input_arr[32].rstrip(")"))) + ")"
                    del output_arr[0]
                    new_values.append(
                        ",".join([str(i) for i in output_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + COLS + "VALUES" + post + ";" + SEP)


start_time = time.time()
conver_file("t_iou_installment_list.sql",
            "/home/loan_installment_list_out.sql", valid)
end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
