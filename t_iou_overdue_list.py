#!/usr/bin/env python
#-*-coding:utf-8-*-

import iter_gmatch
import torndb_handler
import threading
import os
import time
import common_dbs

OUTPUT_FILE = "/home/pangqiqiang/t_iou_overdue_list_out.sql"
INPUT_FILE0 = "t_iou_overdue_list000"
INPUT_FILE1 = "t_iou_overdue_list001"
INPUT_FILE2 = "t_iou_overdue_list002"
INPUT_FILE3 = "t_iou_overdue_list003"
INPUT_FILE4 = "t_iou_overdue_list004"
SEP = os.linesep
# 为了多线程加速采用多个db对象查询
MYDB0 = common_dbs.LOAN_INSTALLMENT_LIST_DB
MYDB1 = common_dbs.LOAN_INSTALLMENT_LIST_DB
MYDB2 = common_dbs.LOAN_INSTALLMENT_LIST_DB
MYDB3 = common_dbs.LOAN_INSTALLMENT_LIST_DB
MYDB4 = common_dbs.LOAN_INSTALLMENT_LIST_DB
# 导入迭代器函数
gmatch = iter_gmatch.gmatch
# 数据有效行必须以INSERT 开头
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


def conver_file(input_file, output_file, valid, mydb):
    with open(input_file, 'r') as fin:
        with open(output_file, 'a') as fout:
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
                    temp_arr = parse_sql_fields(item)
                    origin_id = temp_arr[0].lstrip("(")
                    # mutex.acquire()公用dbclient的时候必须加锁
                    new_id = mydb.fetch_from_origin_id(
                        origin_id.replace("'", ""))
                    # mutex.release()
                    temp_arr[2] = str(int(float(temp_arr[2]) * 100 + 0.5))
                    temp_arr[3] = str(int(float(temp_arr[3]) * 100 + 0.5))
                    del temp_arr[1]
                    temp_arr.insert(0, "(" + str(new_id))
                    temp_arr[1] = origin_id
                    temp_arr[-1] = temp_arr[-1].rstrip(")")
                    temp_arr.append("b'0')")
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                mutex.acquire()
                fout.write(pre + " " + post + SEP)
                mutex.release()


start_time = time.clock()
# 创建线程
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE, valid, MYDB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE, valid, MYDB1))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE, valid, MYDB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE, valid, MYDB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE, valid, MYDB4))


# 开始线程
thread0.start()
thread1.start()
thread2.start()
thread3.start()
thread4.start()

# 添加线程到线程列表
threads.append(thread0)
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)


# 等待所有线程完成
for t in threads:
    t.join()

end_time = time.clock()

time_elapse = (end_time - start_time)

print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
