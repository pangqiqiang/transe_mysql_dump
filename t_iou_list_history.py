#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import threading
from multi_thread_dbs import *

OUTPUT_FILE = "/home/pangqiqiang/t_iou_list_history_out.sql"
INPUT_FILE0 = "t_iou_list_history000"
INPUT_FILE1 = "t_iou_list_history001"
INPUT_FILE2 = "t_iou_list_history002"
INPUT_FILE3 = "t_iou_list_history003"
INPUT_FILE4 = "t_iou_list_history004"
INPUT_FILE5 = "t_iou_list_history005"
INPUT_FILE6 = "t_iou_list_history006"
INPUT_FILE7 = "t_iou_list_history007"
INPUT_FILE8 = "t_iou_list_history008"
INPUT_FILE9 = "t_iou_list_history009"

OFFLINE_PAY_METHOD_MAP = {'支付宝': 0, '微信': 1, '银行卡': 2}
# 导入迭代器
gmatch = iter_gmatch.gmatch
valid = "INSERT"
# 进程锁解决资源冲突
mutex = threading.Lock()
# 线程池
threads = []
seq_count = 191031

# 定义线程类


class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, valid, loan_db,
                loan_installment_list_db, loan_write_off_db,
                user_passport_db, trade_db, collection_account_db,
                collection_apply_db):
    # 维护自增id
    global seq_count
    with open(input_file, 'r') as fin:
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
                # 输出列表初始化
                output_arr = list(range(45))
                item = item.strip(",")
                input_arr = parse_sql_fields(item)
                mutex.acquire()
                seq_count += 1
                output_arr[0] = "(" + str(seq_count)
                mutex.release()
                # original_id
                output_arr[1] = input_arr[0].lstrip("(")
                # loan_id(c_iou_id)
                output_arr[3] = input_arr[1]
                output_arr[2] = loan_db.fetch_from_origin_id(
                    output_arr[3])
                # loan_installment_list_id(c_iou_installment_list_id),
                output_arr[5] = input_arr[2]
                output_arr[4] = loan_installment_list_db.fetch_from_origin_id(
                    output_arr[5])
                # write_off_id(c_write_off_id,)
                output_arr[7] = input_arr[3]
                output_arr[6] = loan_write_off_db.fetch_from_origin_id(
                    output_arr[7])
                # c_user_id
                output_arr[8] = input_arr[4]
                # lender_uid,(c_lender_id),borrower_uid
                #(c_borrower_id),guarantee_uid
                for i in range(9, 14, 2):
                    output_arr[i + 1] = input_arr[6 + (i - 9) / 2]
                    output_arr[i] = user_passport_db.fetch_from_salt(
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
                output_arr[16] = user_passport_db.fetch_from_salt(
                    output_arr[17])
                # trade_id(c_trade_id)
                output_arr[19] = input_arr[9]
                output_arr[18] = trade_db.fetch_from_origin_id(
                    output_arr[19])

                # repay_amount,amount,interest_amount,forfeit_amount,
                # commission_amount,commission_party_amount
                for i in range(20, 26):
                    output_arr[i] = int(
                        float(input_arr[i - 10]) * 100 + 0.5)
                # collection_account_id(c_collection_account_id)
                output_arr[27] = input_arr[16]
                output_arr[26] = collection_account_db.fetch_from_origin_id(
                    output_arr[27])
                # collection_apply_id(c_collection_apply_id)
                output_arr[29] = input_arr[17]
                output_arr[28] = collection_apply_db.fetch_from_origin_id(
                    output_arr[29])
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
                output_arr[33] = get_from_map(
                    OFFLINE_PAY_METHOD_MAP, output_arr[34])
                if output_arr[33] == None:
                    output_arr[33] = 3
                # valid_status
                output_arr[35] = input_arr[20]
                # receive_time(t_rcv_tm)
                output_arr[38] = input_arr[27]
                mutex.acquire()
                output_arr[37] = datetime2int(output_arr[38])
                mutex.release()
                # receive_status
                output_arr[36] = "b'0'" if (
                    output_arr[38] == "NULL" or len(output_arr[38]) == 0) else "b'1'"
                # reconciliation_status
                output_arr[39] = input_arr[23]
                # reconciliation_time(t_reconciliation_tm)
                output_arr[41] = input_arr[28].rstrip(")")
                mutex.acquire()
                output_arr[40] = datetime2int(output_arr[41])
                # extend_time_status
                output_arr[42] = input_arr[24]
                # create_time
                output_arr[43] = datetime2timestamp(input_arr[26])
                mutex.release()
                # update_time
                output_arr[44] = str(output_arr[43]) + ")"
                new_values.append(",".join([str(i) for i in output_arr]))
            post = ",".join(new_values)
            mutex.acquire()
            write_lines_in_file(output_file, pre + " " + post + ";")
            mutex.release()


start_time = time.time()
# 创建线程
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE, valid, LOAN_DB0,
                                 LOAN_INSTALLMENT_LIST_DB0, LOAN_WRITE_OFF_DB0,
                                 USER_PASSPORT_DB0, TRADE_DB0, COLLECTION_ACCOUNT_DB0,
                                 COLLECTION_APPLY_DB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE, valid, LOAN_DB1,
                                 LOAN_INSTALLMENT_LIST_DB1, LOAN_WRITE_OFF_DB1,
                                 USER_PASSPORT_DB1, TRADE_DB1, COLLECTION_ACCOUNT_DB1,
                                 COLLECTION_APPLY_DB1))

thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE, valid, LOAN_DB2,
                                 LOAN_INSTALLMENT_LIST_DB2, LOAN_WRITE_OFF_DB2,
                                 USER_PASSPORT_DB2, TRADE_DB2, COLLECTION_ACCOUNT_DB2,
                                 COLLECTION_APPLY_DB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE, valid, LOAN_DB3,
                                 LOAN_INSTALLMENT_LIST_DB3, LOAN_WRITE_OFF_DB3,
                                 USER_PASSPORT_DB3, TRADE_DB3, COLLECTION_ACCOUNT_DB3,
                                 COLLECTION_APPLY_DB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE, valid, LOAN_DB4,
                                 LOAN_INSTALLMENT_LIST_DB4, LOAN_WRITE_OFF_DB4,
                                 USER_PASSPORT_DB4, TRADE_DB4, COLLECTION_ACCOUNT_DB4,
                                 COLLECTION_APPLY_DB4))
thread5 = myThread(conver_file, (INPUT_FILE5, OUTPUT_FILE, valid, LOAN_DB5,
                                 LOAN_INSTALLMENT_LIST_DB5, LOAN_WRITE_OFF_DB5,
                                 USER_PASSPORT_DB5, TRADE_DB5, COLLECTION_ACCOUNT_DB5,
                                 COLLECTION_APPLY_DB5))
thread6 = myThread(conver_file, (INPUT_FILE6, OUTPUT_FILE, valid, LOAN_DB6,
                                 LOAN_INSTALLMENT_LIST_DB6, LOAN_WRITE_OFF_DB6,
                                 USER_PASSPORT_DB6, TRADE_DB6, COLLECTION_ACCOUNT_DB6,
                                 COLLECTION_APPLY_DB6))
thread7 = myThread(conver_file, (INPUT_FILE7, OUTPUT_FILE, valid, LOAN_DB7,
                                 LOAN_INSTALLMENT_LIST_DB7, LOAN_WRITE_OFF_DB7,
                                 USER_PASSPORT_DB7, TRADE_DB7, COLLECTION_ACCOUNT_DB7,
                                 COLLECTION_APPLY_DB7))
thread8 = myThread(conver_file, (INPUT_FILE8, OUTPUT_FILE, valid, LOAN_DB8,
                                 LOAN_INSTALLMENT_LIST_DB8, LOAN_WRITE_OFF_DB8,
                                 USER_PASSPORT_DB8, TRADE_DB8, COLLECTION_ACCOUNT_DB8,
                                 COLLECTION_APPLY_DB8))
thread9 = myThread(conver_file, (INPUT_FILE9, OUTPUT_FILE, valid, LOAN_DB9,
                                 LOAN_INSTALLMENT_LIST_DB9, LOAN_WRITE_OFF_DB9,
                                 USER_PASSPORT_DB9, TRADE_DB9, COLLECTION_ACCOUNT_DB9,
                                 COLLECTION_APPLY_DB9))

# 添加线程到线程列表
threads.append(thread0)
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)
threads.append(thread5)
threads.append(thread6)
threads.append(thread7)
threads.append(thread8)
threads.append(thread9)

# 启动进程
for t in threads:
    t.start()

# 等待所有线程完成
for t in threads:
    t.join()

end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
