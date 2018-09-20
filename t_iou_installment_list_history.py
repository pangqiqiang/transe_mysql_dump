#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import threading
from multi_thread_dbs import *


OUTPUT_FILE = "/home/loan_installment_list_history_out.sql"
INPUT_FILE0 = "t_iou_installment_list_history000"
INPUT_FILE1 = "t_iou_installment_list_history001"
INPUT_FILE2 = "t_iou_installment_list_history002"
INPUT_FILE3 = "t_iou_installment_list_history003"
INPUT_FILE4 = "t_iou_installment_list_history004"
INPUT_FILE5 = "t_iou_installment_list_history005"
INPUT_FILE6 = "t_iou_installment_list_history006"
INPUT_FILE7 = "t_iou_installment_list_history007"
INPUT_FILE8 = "t_iou_installment_list_history008"
INPUT_FILE9 = "t_iou_installment_list_history009"

PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}


gmatch = iter_gmatch.gmatch
# 进程锁解决资源冲突
mutex = threading.Lock()
# 线程池
threads = []
valid = "INSERT"
seq_count = 1073352

# 定义线程类


class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, valid, loan_db, user_passport_db):
    # 维护主键
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
            pre = line[:(pre_pos + 1 + len("VALUES"))
                       ].replace("t_iou_installment_list_history", "loan_installment_list")
            new_values = []
            for item in gmatch(line, "(", "),", pre_pos):
                # 输出映射数组
                output_arr = list(range(40))
                item = item.strip(",")
                input_arr = parse_sql_fields(item)
                mutex.acquire()
                seq_count += 1
                # print(seq_count)
                output_arr[0] = "(" + str(seq_count)
                mutex.release()
                # Original_id,[1],cur_period,total_period,
                output_arr[1] = input_arr[0].lstrip("(")
                # cur_period,total_period
                output_arr[2] = input_arr[1]
                output_arr[3] = input_arr[2]
                # loan_id
                output_arr[5] = input_arr[3]
                output_arr[4] = loan_db.fetch_from_origin_id(
                    output_arr[5])
                #,c_iou_id,borrower_uid,c_borrower_id,
                # lender_uid,c_lender_id,guarantee_uid,c_guarantee_id
                output_arr[7] = input_arr[4]
                output_arr[6] = user_passport_db.fetch_from_salt(
                    output_arr[7])
                output_arr[9] = input_arr[5]
                output_arr[8] = user_passport_db.fetch_from_salt(
                    output_arr[9])
                output_arr[11] = input_arr[6]
                output_arr[10] = user_passport_db.fetch_from_salt(
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
                output_arr[26] = get_from_map(PURPOSE_TYPE_MAP, output_arr[27])
                if output_arr[26] == None:
                    output_arr[26] = 8
                # repay_time,t_repay_tm
                output_arr[29] = input_arr[28]
                mutex.acquire()
                output_arr[28] = date2int(output_arr[29])
                mutex.release()
                # normal_repay_amount
                output_arr[30] = float_char_to_int(input_arr[20])
                # online_status,end_status,overdue_status
                for i in range(31, 34):
                    output_arr[i] = input_arr[i - 8]
                # valid_status
                output_arr[34] = "b'1'"
                # version_number
                output_arr[35] = input_arr[27]
                # payoff_time,t_pay_off_tm
                output_arr[37] = input_arr[29]
                mutex.acquire()
                output_arr[36] = date2int(output_arr[37])
                # create_time, update_time
                output_arr[38] = datetime2timestamp(input_arr[30])
                output_arr[39] = str(datetime2timestamp(
                    input_arr[31].rstrip(")"))) + ")"
                mutex.release()
                new_values.append(
                    ",".join([str(i) for i in output_arr]))
            post = ",".join(new_values)
            mutex.acquire()
            write_lines_in_file(output_file, pre + " " + post + ";")
            mutex.release()


start_time = time.time()
# 创建线程
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE,
                                 valid, LOAN_DB0, USER_PASSPORT_DB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE,
                                 valid, LOAN_DB1, USER_PASSPORT_DB1))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE,
                                 valid, LOAN_DB2, USER_PASSPORT_DB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE,
                                 valid, LOAN_DB3, USER_PASSPORT_DB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE,
                                 valid, LOAN_DB4, USER_PASSPORT_DB4))
thread5 = myThread(conver_file, (INPUT_FILE5, OUTPUT_FILE,
                                 valid, LOAN_DB5, USER_PASSPORT_DB5))
thread6 = myThread(conver_file, (INPUT_FILE6, OUTPUT_FILE,
                                 valid, LOAN_DB6, USER_PASSPORT_DB6))
thread7 = myThread(conver_file, (INPUT_FILE7, OUTPUT_FILE,
                                 valid, LOAN_DB7, USER_PASSPORT_DB7))
thread8 = myThread(conver_file, (INPUT_FILE8, OUTPUT_FILE,
                                 valid, LOAN_DB8, USER_PASSPORT_DB8))
thread9 = myThread(conver_file, (INPUT_FILE9, OUTPUT_FILE,
                                 valid, LOAN_DB9, USER_PASSPORT_DB9))

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
