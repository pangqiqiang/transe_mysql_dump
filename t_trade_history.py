#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import multi_thread_dbs
import threading


OUTPUT_FILE = "/home/pangqiqiang/t_trade_history_out.sql"
INPUT_FILE0 = "t_trade_history000"
INPUT_FILE1 = "t_trade_history001"
INPUT_FILE2 = "t_trade_history002"
INPUT_FILE3 = "t_trade_history003"
INPUT_FILE4 = "t_trade_history004"
INPUT_FILE5 = "t_trade_history005"
INPUT_FILE6 = "t_trade_history006"
INPUT_FILE7 = "t_trade_history007"
INPUT_FILE8 = "t_trade_history008"
INPUT_FILE9 = "t_trade_history009"

# 导入数据库类
PASSWORD_DB0 = multi_thread_dbs.USER_PASSPORT_DB0
PASSWORD_DB1 = multi_thread_dbs.USER_PASSPORT_DB1
PASSWORD_DB2 = multi_thread_dbs.USER_PASSPORT_DB2
PASSWORD_DB3 = multi_thread_dbs.USER_PASSPORT_DB3
PASSWORD_DB4 = multi_thread_dbs.USER_PASSPORT_DB4
PASSWORD_DB5 = multi_thread_dbs.USER_PASSPORT_DB5
PASSWORD_DB6 = multi_thread_dbs.USER_PASSPORT_DB6
PASSWORD_DB7 = multi_thread_dbs.USER_PASSPORT_DB7
PASSWORD_DB8 = multi_thread_dbs.USER_PASSPORT_DB8
PASSWORD_DB9 = multi_thread_dbs.USER_PASSPORT_DB9

gmatch = iter_gmatch.gmatch
valid = "INSERT"
# 进程锁解决资源冲突
mutex = threading.Lock()
# 线程池
threads = []
# 维护自增id
seq_count = 6378

# 定义线程类


class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, valid, mydb):
    global seq_count
    with open(input_file, 'r') as fin:
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
                       ].replace("t_trade_history", "trade")
            new_values = []
            for item in gmatch(line, "(", "),", pre_pos):
                    # 输出映射数组
                out_arr = list(range(25))
                # 维护自增id
                mutex.acquire()
                seq_count += 1
                out_arr[0] = "(" + str(seq_count)
                mutex.release()
                item = item.strip(",")
                input_arr = parse_sql_fields(item)
                # original_id
                out_arr[1] = input_arr[0].lstrip("(")
                # pay_order_no
                out_arr[2] = input_arr[1]
                # trade_type
                out_arr[3] = input_arr[2]
                # uid,c_user_id
                out_arr[5] = input_arr[3]
                out_arr[4] = mydb.fetch_from_salt(
                    out_arr[5])
                # bank_account
                out_arr[6] = input_arr[4]
                # withdraw_type(int->bit)
                out_arr[7] = "b'" + input_arr[5] + "'"
                # relation_type,reserve_data
                out_arr[8] = input_arr[6]
                out_arr[9] = input_arr[7]
                # amount,fee_amount
                out_arr[10] = float_char_to_int(input_arr[8])
                out_arr[11] = float_char_to_int(input_arr[9])
                #send_time, t_send_tm
                out_arr[14] = input_arr[13]
                out_arr[13] = datetime2int(out_arr[14])
                # b_valid
                out_arr[15] = input_arr[10]
                # b_rcv_bank
                out_arr[16] = input_arr[11]
                # receive_time,t_rcv_tm
                out_arr[18] = input_arr[14]
                out_arr[17] = datetime2int(out_arr[18])
                # trade_status
                if (out_arr[16] == 0 and out_arr[14] != "NULL"):
                    out_arr[12] = 2
                elif (out_arr[16] == 1 and out_arr[15] == 1):
                    out_arr[12] = 3
                elif (out_arr[16] == 1 and out_arr[15] == 0):
                    out_arr[12] = 4
                else:
                    out_arr[12] = 1
                # reconciliation_status
                out_arr[19] = "b'" + input_arr[12] + "'"
                # reconciliation_time,t_reconciliation_tm
                out_arr[21] = input_arr[15]
                out_arr[20] = datetime2int(out_arr[21])
                # lease_status
                out_arr[22] = "b'0'"
                # create_time,update_time
                out_arr[23] = input_arr[16].rstrip(")")
                out_arr[24] = out_arr[23] + ")"
                new_values.append(
                    ",".join([str(i) for i in out_arr]))
            post = ",".join(new_values)
            mutex.acquire()
            write_lines_in_file(output_file, pre + " " + post + ";")
            mutex.release()


start_time = time.time()
# 创建线程
thread0 = myThread(
    conver_file, (INPUT_FILE0, OUTPUT_FILE, valid, PASSWORD_DB0))
thread1 = myThread(
    conver_file, (INPUT_FILE1, OUTPUT_FILE, valid, PASSWORD_DB1))
thread2 = myThread(
    conver_file, (INPUT_FILE2, OUTPUT_FILE, valid, PASSWORD_DB2))
thread3 = myThread(
    conver_file, (INPUT_FILE3, OUTPUT_FILE, valid, PASSWORD_DB3))
thread4 = myThread(
    conver_file, (INPUT_FILE4, OUTPUT_FILE, valid, PASSWORD_DB4))
thread5 = myThread(
    conver_file, (INPUT_FILE5, OUTPUT_FILE, valid, PASSWORD_DB5))
thread6 = myThread(
    conver_file, (INPUT_FILE6, OUTPUT_FILE, valid, PASSWORD_DB6))
thread7 = myThread(
    conver_file, (INPUT_FILE7, OUTPUT_FILE, valid, PASSWORD_DB7))
thread8 = myThread(
    conver_file, (INPUT_FILE8, OUTPUT_FILE, valid, PASSWORD_DB8))
thread9 = myThread(
    conver_file, (INPUT_FILE9, OUTPUT_FILE, valid, PASSWORD_DB9))
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
