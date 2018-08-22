#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch

SEP = os.linesep

LOAN_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                              database="dts_jjd", user="dev",
                              password="KRkFcVCbopZbS8R7",
                              tablename="loan")

USER_PASSPORT_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                       database="dts_jjd", user="dev",
                                       password="KRkFcVCbopZbS8R7",
                                       tablename="user_passport")

PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}


def date2int(date):
    try:
        time_arr = time.strptime(date, "%Y-%m-%d")
        return int(time.mktime(time_arr))
    except ValueError:
        return 0


def datetime2int(date):
    try:
        time_arr = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        return int(time.mktime(time_arr))
    except ValueError:
        return 0


def conver_file(input_file, output_file, valid):
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
                pre = line[:(pre_pos + 1 + len("VALUES"))]
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    item = item.strip(",")
                    temp_arr = item.split(",")
                    c_iou_id = temp_arr[5]
                    loan_id = LOAN_DB.fetch_from_origin_id(c_iou_id)
                    temp_arr[4] = loan_id
                    c_borrower_id = temp_arr[7]
                    borrower_uid = USER_PASSPORT_DB.fetch_from_salt(
                        c_borrower_id)
                    tem_arr[6] = borrower_uid
                    c_lender_id = temp_arr[9]
                    lender_uid = USER_PASSPORT_DB.fetch_from_salt(c_lender_id)
                    temp_arr[8] = lender_uid
                    c_guarantee_id = temp_arr[11]
                    guarantee_id = USER_PASSPORT_DB.fetch_from_salt(
                        c_guarantee_id)
                    # 处理total_amount\amount \interest_amount \forfeit_amount \overdue_manage_amount
                    # overdue_manage_amount_special \return_overdue_manage_amount \get_amount \got_amount
                    # paid_amount \paid_interest_amount \paid_forfeit_amount \paid_overdue_manage_amount
                    for i in range(13, 26):
                        temp_arr[i] *= 100
                    c_purpose = temp_arr[27]
                    purpose = PURPOSE_TYPE_MAP.get(c_purpose) or 8
                    temp_arr[26] = purpose
                    t_repay_tm = temp_arr[29]
                    repay_time = date2int(t_repay_tm)
                    temp_arr[28] = repay_time
                    # normal_repay_amount
                    temp_arr[30] *= 100
                    # valid_status
                    temp_arr[34] = "b'0'"
                    t_payoff_time = temp_arr[37]
                    # payoff_time
                    temp_arr[36] = date2int(t_payoff_time)
                    # create_time
                    temp_arr[38] = datetime2int(temp_arr[38])
                    # update_time
                    temp_arr[39] = datetime2int(temp_arr[39])
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


gmatch = iter_gmatch.gmatch
valid = "INSERT"
start_time = time.clock()
conver_file("loan_installment_list.sql",
            "/tmp/loan_installment_list_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
