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

OFFLINE_PAY_METHOD_MAP = {'支付宝': 0, '微信': 1, '银行卡': 2}


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
                    c_iou_id = temp_arr[3]
                    loan_id = LOAN_DB.fetch_from_origin_id(c_iou_id)
                    temp_arr[2] = loan_id
                    c_iou_installment_list_id = temp_arr[5]
                    loan_installment_list_id = LOAN_INSTALLMENT_LIST_DB.fetch_from_origin_id(
                        c_iou_installment_list_id)
                    temp_arr[4] = loan_installment_list_id
                    c_write_off_id = temp_arr[7]
                    write_off_id = LOAN_WRITE_OFF_DB.fetch_from_origin_id(
                        c_write_off_id)
                    temp_arr[6] = write_off_id
                    c_user_id = temp_arr[8]
                    c_borrower_id = temp_arr[12]
                    c_lender_id = temp_arr[10]
                    if c_user_id and c_user_id == c_borrower_id:
                        repayer_type = 1
                    elif c_user_id and c_user_id == c_lender_id:
                        repayer_type = 2
                    else:
                        repayer_type = 3
                    temp_arr[15] = repayer_type
                    c_confirm_id = temp_arr[17]
                    confirm_id = USER_PASSPORT_DB.fetch_from_salt(c_confirm_id)
                    temp_arr[16] = confirm_id
                    lender_uid = USER_PASSPORT_DB.fetch_from_salt(c_lender_id)
                    temp_arr[9] = lender_uid
                    borrower_uid = USER_PASSPORT_DB.fetch_from_salt(
                        c_borrower_id)
                    temp_arr[11] = borrower_uid
                    c_guarantee_id = temp_arr[14]
                    guarantee_uid = USER_PASSPORT_DB.fetch_from_salt(
                        c_guarantee_id)
                    temp[13] = guarantee_uid
                    c_trade_id = temp_arr[19]
                    trade_id = TRADE_DB.fetch_from_origin_id(c_trade_id)
                    temp[18] = trade_id
                    # 处理repay_amount\amount\interest_amount\forfeit_amount\commission_amount\commission_party_amount
                    for i in range(20, 26):
                        temp[i] *= 100
                    c_collection_account_id = temp_arr[27]
                    collection_account_id = COLLECTION_ACCOUNT_DB.fetch_from_origin_id(
                        c_collection_account_id)
                    temp_arr[26] = collection_account_id
                    c_collection_apply_id = temp_arr[29]
                    collection_apply_id = COLLECTION_APPLY_DB.fetch_from_origin_id(
                        c_collection_apply_id)
                    temp_arr[28] = collection_account_id
                    # overdue_manage_amount
                    temp_arr[30] *= 100
                    # return_overdue_manage_amount
                    temp_arr[31] *= 100
                    c_offline_pay_method = temp_arr[34]
                    offline_pay_method = OFFLINE_PAY_METHOD_MAP.get(
                        c_offline_pay_method) or 3
                    temp_arr[33] = offline_pay_method
                    t_rcv_tm = temp_arr[38]
                    receive_status = "b'0'" if not t_rcv_tm else "b'1'"
                    temp_arr[36] = receive_status
                    if t_rcv_tm and len(t_rcv_tm) > 0:
                        receive_time = datetime2int(t_rcv_tm)
                        temp_arr[37] = receive_time
                    t_reconciliation_tm = temp_arr[41]
                    if t_reconciliation_tm and len(t_reconciliation_tm) > 0:
                        reconciliation_time = datetime2int(t_reconciliation_tm)
                        temp_arr[40] = reconciliation_time
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


gmatch = iter_gmatch.gmatch
valid = "INSERT"
start_time = time.clock()
conver_file("loan_repay_list.sql",
            "/tmp/loan_repay_list_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
