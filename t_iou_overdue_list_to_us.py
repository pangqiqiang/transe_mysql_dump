#!/usr/bin/env python
#-*-coding:utf-8-*-
#
import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
from multi_thread_dbs import *

OUTPUT_FILE = "/home/t_iou_overdue_list_to_us_out.sql"
INPUT_FILE0 = "t_iou_overdue_list_to_us000"
INPUT_FILE1 = "t_iou_overdue_list_to_us001"
INPUT_FILE2 = "t_iou_overdue_list_to_us002"
INPUT_FILE3 = "t_iou_overdue_list_to_us003"
INPUT_FILE4 = "t_iou_overdue_list_to_us004"
INPUT_FILE5 = "t_iou_overdue_list_to_us005"
INPUT_FILE6 = "t_iou_overdue_list_to_us006"
INPUT_FILE7 = "t_iou_overdue_list_to_us007"
INPUT_FILE8 = "t_iou_overdue_list_to_us008"
INPUT_FILE9 = "t_iou_overdue_list_to_us009"

gmatch = iter_gmatch.gmatch
valid = "INSERT"
# 进程锁解决资源冲突
mutex = threading.Lock()
# 线程池
threads = []

# 定义线程类


class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, valid, loan_installment_list):
    with open(input_file, 'r') as fin:
        for line in fin:
            if not line.startswith(valid):
                continue
            line.rstrip()
            pre_pos = line.find("VALUES")
            if pre_pos == -1:
                continue
            post = line[(pre_pos + 1):]
            pre = line[:(pre_pos + 1 + len("VALUES"))].replace(
                "t_iou_overdue_list_to_us", "loan_overdue_manage_fee_list")
            new_values = []
            for item in gmatch(line, "(", ")", pre_pos):
                item = item.strip(",")
                temp_arr = parse_sql_fields(item)
                origin_id = temp_arr[0].lstrip("(")
                # mutex.acquire()公用dbclient的时候必须加锁
                new_id = loan_installment_list.fetch_from_origin_id(origin_id)
                # mutex.release()
                temp_arr[2] = int(float(temp_arr[2]) * 100)
                temp_arr[3] = int(float(temp_arr[3]) * 100)
                del temp_arr[1]
                temp_arr.insert(0, "(" + "'" + str(new_id) + "'")
                temp_arr[1] = origin_id
                temp_arr[-1] = temp_arr[-1].rstrip(")")
                temp_arr.append("b'0')")
                new_values.append(",".join([str(i) for i in temp_arr]))
            post = ",".join(new_values)
            mutex.acquire()
            write_lines_in_file(output_file, pre + " " + post + ";")
            mutex.release()


start_time = time.time()
# 创建线程
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB1))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB4))
thread5 = myThread(conver_file, (INPUT_FILE5, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB5))
thread6 = myThread(conver_file, (INPUT_FILE6, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB6))
thread7 = myThread(conver_file, (INPUT_FILE7, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB7))
thread8 = myThread(conver_file, (INPUT_FILE8, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB8))
thread9 = myThread(conver_file, (INPUT_FILE9, OUTPUT_FILE,
                                 valid, LOAN_INSTALLMENT_LIST_DB9))
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
