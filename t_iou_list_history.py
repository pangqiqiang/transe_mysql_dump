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
LOAN_DB = common_dbs.LOAN_DB
LOAN_INSTALLMENT_LIST_DB = common_dbs.LOAN_INSTALLMENT_LIST_DB
LOAN_WRITE_OFF_DB = common_dbs.LOAN_WRITE_OFF_DB
USER_PASSPORT_DB = common_dbs.USER_PASSPORT_DB
TRADE_DB = common_dbs.TRADE_DB
COLLECTION_ACCOUNT_DB = common_dbs.COLLECTION_ACCOUNT_DB
COLLECTION_APPLY_DB = common_dbs.COLLECTION_APPLY_DB
OFFLINE_PAY_METHOD_MAP = {'支付宝': 0, '微信': 1, '银行卡': 2}
# 导入迭代器
gmatch = iter_gmatch.gmatch
valid = "INSERT"


def conver_file(input_file, output_file, valid):
    # 维护自增id
    seq_count = 186034
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
                pre = pre = line[:(pre_pos + 1 + len("VALUES"))
                                 ].replace("t_iou_list_history", "loan_repay_list")
                new_values = []
                for item in gmatch(line, "(", "),", pre_pos):
                    seq_count += 1
                    # 输出列表初始化
                    output_arr = list(range(45))
                    item = item.strip(",")
                    input_arr = parse_sql_fields(item)
                    output_arr[0] = "(" + str(seq_count)
                    # original_id
                    output_arr[1] = input_arr[0].lstrip("(")
                    # loan_id(c_iou_id)
                    output_arr[3] = input_arr[1]
                    output_arr[2] = LOAN_DB.fetch_from_origin_id(
                        output_arr[3])
                    # loan_installment_list_id(c_iou_installment_list_id),
                    output_arr[5] = input_arr[2]
                    output_arr[4] = LOAN_INSTALLMENT_LIST_DB.fetch_from_origin_id(
                        output_arr[5])
                    # write_off_id(c_write_off_id,)
                    output_arr[7] = input_arr[3]
                    output_arr[6] = LOAN_WRITE_OFF_DB.fetch_from_origin_id(
                        output_arr[7])
                    # c_user_id
                    output_arr[8] = input_arr[4]
                    # lender_uid,(c_lender_id),borrower_uid
                    #(c_borrower_id),guarantee_uid
                    for i in range(9, 14, 2):
                        output_arr[i + 1] = input_arr[6 + (i - 9) / 2]
                        output_arr[i] = USER_PASSPORT_DB.fetch_from_salt(
                            output_arr[i + 1])
                    # repayer_type
                    # c_user_id=(c_borrower_id)type=1
                    if output_arr[8] == output_arr[12]:
                        output_arr[15] = 1
                    # c_user_id=(c_lender_id)type=2
                    elif output_arr[8] == output_arr[10]:
                        output_arr[15] = 2
                    else:
                        output_arr[15] = 3
                    # confirm_id, c_confirm_id
                    output_arr[17] = input_arr[5]
                    output_arr[16] = USER_PASSPORT_DB.fetch_from_salt(
                        output_arr[17])
                    # trade_id(c_trade_id)
                    output_arr[19] = input_arr[9]
                    output_arr[18] = TRADE_DB.fetch_from_origin_id(
                        output_arr[19])

                    # repay_amount,amount,interest_amount,forfeit_amount,
                    # commission_amount,commission_party_amount
                    for i in range(20, 26):
                        output_arr[i] = int(
                            float(input_arr[i - 10]) * 100 + 0.5)
                    # collection_account_id(c_collection_account_id)
                    output_arr[27] = input_arr[16]
                    output_arr[26] = COLLECTION_ACCOUNT_DB.fetch_from_origin_id(
                        output_arr[27])
                    # collection_apply_id(c_collection_apply_id)
                    output_arr[29] = input_arr[17]
                    output_arr[28] = COLLECTION_APPLY_DB.fetch_from_origin_id(
                        output_arr[29].stip("'"))
                    # overdue_manage_amount,
                    try:
                        output_arr[30] = int(float(input_arr[18]) * 100 + 0.5)
                    except ValueError:
                        output_arr[30] = input_arr[18]
                    # return_overdue_manage_amount
                    try:
                        output_arr[31] = int(float(input_arr[19]) * 100 + 0.5)
                    except ValueError:
                        output_arr[30] = input_arr[19]
                    # online_status
                    output_arr[32] = input_arr[21]
                    # offline_pay_method(c_offline_pay_method)
                    output_arr[34] = input_arr[25]
                    output_arr[33] = OFFLINE_PAY_METHOD_MAP.get(output_arr[34].strip(
                        "'"))
                    if output_arr[33] == None:
                        output_arr[33] = 3
                    # valid_status
                    output_arr[35] = input_arr[20]
                    # receive_time(t_rcv_tm)
                    output_arr[38] = input_arr[27]
                    output_arr[37] = datetime2int(output_arr[38])
                    # receive_status
                    output_arr[36] = "b'0'" if (
                        output_arr[38] == "NULL" or len(output_arr[38]) == 0) else "b'1'"
                    # reconciliation_status
                    output_arr[39] = input_arr[23]
                    # reconciliation_time(t_reconciliation_tm)
                    output_arr[41] = input_arr[28].rstrip(")")
                    output_arr[40] = datetime2int(output_arr[41])
                    # extend_time_status
                    output_arr[42] = input_arr[24]
                    # create_time
                    output_arr[43] = datetime2timestamp(input_arr[26])
                    # update_time
                    output_arr[44] = str(output_arr[43]) + ")"
                    new_values.append(",".join([str(i) for i in output_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + post + ";" + SEP)


start_time = time.time()
conver_file("t_iou_list_history.sql",
            "/home/pangqiqiang/t_iou_list_history_out.sql", valid)
end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
