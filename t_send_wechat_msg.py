#!/usr/bin/env python
#-*-coding:utf-8-*-

import threading
import iter_gmatch
import torndb_handler
import os
import common_dbs
from common_func import *

OUTPUT_FILE = "/home/pangqiqiang/user_wechat_msg_out.sql"
INPUT_FILE0 = "t_send_wechat_msg000"
INPUT_FILE1 = "t_send_wechat_msg001"
INPUT_FILE2 = "t_send_wechat_msg002"
INPUT_FILE3 = "t_send_wechat_msg003"
INPUT_FILE4 = "t_send_wechat_msg004"
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
                line = unescape_quote(line)
                pre_pos = line.find("VALUES")
                if pre_pos == -1:
                    continue
                post = line[(pre_pos + 1):]
                pre = line[:(pre_pos + 1 + len("VALUES"))
                           ].replace("t_send_wechat_msg", "user_wechat_msg")
                new_values = []
                for item in gmatch(line, "(", "),", pre_pos):
                    temp_arr = parse_sql_fields(item)
                    salt = salt = temp_arr[0].lstrip("(")
                    new_id = mydb.fetch_from_salt(salt.strip("'"))
                    # print(new_id)
                    temp_arr.insert(0, "(" + "'" + str(new_id) + "'")
                    temp_arr[1] = temp_arr[1].lstrip("(")
                    temp_arr[-1] = datetime2timestamp(temp_arr[-1].rstrip(")"))
                    temp_arr[-1] = str(temp_arr[-1]) + ")"
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                mutex.acquire()
                fout.write(pre + " " + post + ";" + SEP)
                mutex.release()


start_time = time.time()
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE, valid, MYDB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE, valid, MYDB1))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE, valid, MYDB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE, valid, MYDB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE, valid, MYDB4))

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

end_time = time.time()

time_elapse = (end_time - start_time)

print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
