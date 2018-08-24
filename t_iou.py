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
TRADE_DB = common_dbs.TRADE_DB
LOAN_OFFLINE_DB = common_dbs.LOAN_OFFLINE_DB
PRODUCT_BID_DB = common_dbs.PRODUCT_BID_DB
BID_DB = common_dbs.BID_DB

PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}

gmatch = iter_gmatch.gmatch
valid = "INSERT"


def conver_file(input_file, output_file, valid):
    # 维护自增id
    seq_count = 0
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
                           ].replace("t_iou", "loan")
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    # 维护自增id
                    seq_count += 1
                    # 输出映射数组
                    out_arr = list(range(48))
                    # id
                    out_arr[0] = "(" + str(seq_count)
                    item = item.strip(",")
                    input_arr = item.split(",")
                    # original_id
                    out_arr[1] = input_arr[0].lstrip("(")
                    # borrower_uid,c_borrower_id
                    out_arr[3] = input_arr[1]
                    out_arr[2] = PASSWORD_DB.fetch_from_salt(
                        out_arr[3].strip("'"))
                    # lender_uid,c_lender_id
                    out_arr[5] = input_arr[2]
                    out_arr[4] = PASSWORD_DB.fetch_from_salt(
                        out_arr[5].strip("'"))
                    # guarantee_uid,c_guarantee_id
                    out_arr[7] = input_arr[3]
                    out_arr[6] = PASSWORD_DB.fetch_from_salt(
                        out_arr[7].strip("'"))
                    # service_amount
                    out_arr[8] = float_char_to_int(input_arr[5])
                    # guarantee_amount
                    out_arr[9] = float_char_to_int(input_arr[6])
                    # total_amount
                    out_arr[10] = float_char_to_int(input_arr[15])
                    # amount,[11]
                    out_arr[11] = float_char_to_int(input_arr[4])
                    # interest_amount
                    out_arr[12] = float_char_to_int(input_arr[7])
                    # forfeit_amount
                    out_arr[13] = float_char_to_int(input_arr[8])
                    # overdue_manage_amount
                    out_arr[14] = float_char_to_int(input_arr[9])
                    # overdue_manage_amount_special
                    out_arr[15] = float_char_to_int(input_arr[10])
                    # return_overdue_manage_amount
                    out_arr[16] = float_char_to_int(input_arr[16])
                    # return_overdue_manage_id,c_repay_forfeit_id
                    out_arr[18] = input_arr[17]
                    out_arr[17] = TRADE_DB.fetch_from_origin_id(
                        out_arr[18].strip("'"))
                    # get_amount
                    out_arr[19] = float_char_to_int(input_arr[18])
                    # got_amount
                    out_arr[20] = float_char_to_int(input_arr[19])
                    # paid_amount
                    out_arr[21] = float_char_to_int(input_arr[12])
                    # paid_interest_amount
                    out_arr[22] = float_char_to_int(input_arr[13])
                    # paid_forfeit_amount
                    out_arr[23] = float_char_to_int(input_arr[14])
                    # paid_overdue_manage_amount
                    out_arr[24] = float_char_to_int(input_arr[11])
                    # c_purpose,purpose_type
                    out_arr[26] = input_arr[20]
                    out_arr[25] = PURPOSE_TYPE_MAP.get(
                        out_arr[26].strip("'"))
                    out_arr[25] = 8 if out_arr[25] == None else out_arr[25]
                    # memo,repay_type,period,interest_rate,overdue_rate
                    for i in range(27, 32):
                        out_arr[i] = input_arr[i - 6]
                    # borrow_time[32](t_borrow_tm)[33]
                    out_arr[33] = input_arr[26]
                    out_arr[32] = datetime2timestamp(out_arr[33])
                    # repay_time[34](t_repay_tm)[35]
                    out_arr[35] = input_arr[27]
                    out_arr[34] = datetime2timestamp(out_arr[35])
                    # online_status,[36],# pic_list,[37],source_type,[38]
                    for i in range(36, 39):
                        out_arr[i] = input_arr[i - 8]
                    # source_id
                    out_arr[40] = input_arr[31]
                    # 根据source_type获取source_id
                    if int(out_arr[38]) == 0:
                        out_arr[39] = LOAN_OFFLINE_DB.fetch_from_origin_id(
                            out_arr[40].strip("'"))
                    elif int(out_arr[38]) == 1:
                        out_arr[39] = PRODUCT_BID_DB.fetch_from_origin_id(
                            out_arr[40].strip("'"))
                    else:
                        out_arr[39] = BID_DB.fetch_from_origin_id(
                            out_arr[40].strip("'"))
                    # valid_status,[41],end_status,[42]
                    out_arr[41], out_arr[42] = "b'1'", "b'0'"
                    # version_number,[43],local_agreement_status,[44],ecloud_agreement_status,[45]
                    for i in range(43, 46):
                        out_arr[i] = input_arr[i - 9]
                    # create_time,[46]
                    out_arr[46] = datetime2timestamp(input_arr[37])
                    # update_time[47]
                    out_arr[47] = str(datetime2timestamp(
                        input_arr[38].rstrip(")"))) + ")"
                    new_values.append(
                        ",".join([str(i) for i in out_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


start_time = time.clock()
conver_file("t_iou.sql",
            "/tmp/loan_t_iou_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
