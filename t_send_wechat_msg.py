#!/usr/bin/env python
#-*-coding:utf-8-*-

import threading
import iter_gmatch
import torndb_handler
import os
import common_dbs

OUTPUT_FILE = "/tmp/user_wechat_msg_out.sql"
INPUT_FILE0 = "t_send_wechat_msg00"
INPUT_FILE1 = "t_send_wechat_msg01"
INPUT_FILE2 = "t_send_wechat_msg02"
INPUT_FILE3 = "t_send_wechat_msg03"
INPUT_FILE4 = "t_send_wechat_msg04"
SEP = os.linesep
# 导入数据库类,用多个实例防止线程竞争，限制速度
MYDB0 = common_dbs.USER_PASSPORT_DB
MYDB1 = common_dbs.USER_PASSPORT_DB
MYDB2 = common_dbs.USER_PASSPORT_DB
MYDB3 = common_dbs.USER_PASSPORT_DB
MYDB4 = common_dbs.USER_PASSPORT_DB

mutex = threading.Lock()
threads = []
valid = 'INSERT'
gmatch = iter_gmatch.gmatch


class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, valid, mydb):
    global mutex
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
                    temp_arr = item.split(",")
                    salt = salt = temp_arr[0].lstrip("(")
                    new_id = MYDB.fetch_id(salt.replace("'", ""))
                    # print(new_id)
                    temp_arr.insert(0, "(" + "'" + str(new_id) + "'")
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                mutex.acquire()
                fout.write(pre + " " + post + SEP)
                mutex.release()


thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE, valid, pattern))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE, valid, pattern))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE, valid, pattern))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE, valid, pattern))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE, valid, pattern))

# 添加线程到线程列表
threads.append(thread0)
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)

for t in threads:
    t.start()

# 等待所有线程完成
for t in threads:
    t.join()

print("All documents complete!!!")
