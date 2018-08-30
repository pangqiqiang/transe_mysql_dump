#!/usr/bin/env python
#-*-coding:utf-8-*-

from multiprocessing import Lock, Process, Pool
import os
import time
import iter_gmatch
import torndb_handler


class MyDB:
    def __init__(self, host, database, user, password, tablename, connect_timeout=10):
        try:
            self.db = torndb.Connection(host=host,
                                        database=database,
                                        user=user,
                                        password=password
                                        )
        except Exception as e:
            print(str(e))
        self.tablename = tablename

    def fetch_from_salt(self, salt):
        if salt == "NULL" or len(salt) == 0:
            return "NULL"
        sql = "SELECT uid FROM " + self.tablename + " WHERE salt=%s"
        res = self.db.query(sql, salt)
        res = res[0].uid if res else "NULL"
        return res

    def fetch_from_origin_id(self, origin_id):
        if origin_id == "NULL" or len(origin_id) == 0:
            return "NULL"
        sql = "SELECT id FROM " + self.tablename + " WHERE original_id=%s"
        res = self.db.query(sql, origin_id)
        res = res[0].id if res else "NULL"
        return res


OUTPUT_FILE = "/tmp/t_iou_overdue_list_out2.sql"
INPUT_FILE0 = "t_iou_overdue_list000"
INPUT_FILE1 = "t_iou_overdue_list001"
INPUT_FILE2 = "t_iou_overdue_list002"
INPUT_FILE3 = "t_iou_overdue_list003"
INPUT_FILE4 = "t_iou_overdue_list004"
INPUT_LIST = (INPUT_FILE0, INPUT_FILE1, INPUT_FILE2, INPUT_FILE3, INPUT_FILE4)
SEP = os.linesep
MYDB0 = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                            database="dts_jjd", user="dev",
                            password="KRkFcVCbopZbS8R7",
                            tablename="loan_installment_list")
MYDB1 = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                            database="dts_jjd", user="dev",
                            password="KRkFcVCbopZbS8R7",
                            tablename="loan_installment_list")
MYDB2 = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                            database="dts_jjd", user="dev",
                            password="KRkFcVCbopZbS8R7",
                            tablename="loan_installment_list")
MYDB3 = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                            database="dts_jjd", user="dev",
                            password="KRkFcVCbopZbS8R7",
                            tablename="loan_installment_list")
MYDB4 = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                            database="dts_jjd", user="dev",
                            password="KRkFcVCbopZbS8R7",
                            tablename="loan_installment_list")
DBS = (MYDB0, MYDB1, MYDB2, MYDB3, MYDB4)
# 导入迭代器函数
gmatch = iter_gmatch.gmatch
# 数据有效行必须以INSERT 开头
valid = "INSERT"
# 进程锁解决资源冲突
lock = Lock()

# 定义进程函数


def conver_file(input_file, output_file, valid, mydb):
    with open(input_file, 'r') as fin:
        with open(output_file, 'a') as fout:
            for line in fin:
                if not line.startswith(valid):
                    continue
                line.rstrip()
                pre_pos = line.find("VALUES") + len("VALUES")
                if pre_pos == -1:
                    continue
                post = line[(pre_pos + 1):]
                pre = line[:(pre_pos + 1)]
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    item = item.strip(",")
                    temp_arr = item.split(",")
                    origin_id = temp_arr[0].lstrip("(")
                    new_id = mydb.fetch_id(origin_id.replace("'", ""))
                    # mutex.release()
                    temp_arr[2] = str(int(float(temp_arr[2]) * 100))
                    temp_arr[3] = str(int(float(temp_arr[3]) * 100))
                    del temp_arr[1]
                    temp_arr.insert(0, "(" + str(new_id))
                    temp_arr[1] = origin_id
                    temp_arr[-1] = temp_arr[-1].rstrip(")")
                    temp_arr.append("'0')")
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                lock.acquire()
                fout.write(pre + " " + post + SEP)
                lock.release()


start_time = time.clock()
# 创建进程池
p = Pool(processes=5)
# 异步添加进程
for i in range(5):
    p.apply_async(gen_process(DBS[i]), args=(
        INPUT_LIST[i], OUTPUT_FILE, valid,))
# 完成进程添加，开始运行
p.close()
# 等待所有进程完成
p.join()

end_time = time.clock()

time_elapse = (end_time - start_time)

print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
