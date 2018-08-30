#!/usr/bin/env python
#-*-coding:utf-8-*-
#
import torndb_handler
import os
import time
import iter_gmatch
import common_dbs
from common_func import *

SEP = os.linesep
# 导入数据库类
MYDB = common_dbs.LOAN_INSTALLMENT_LIST_DB

gmatch = iter_gmatch.gmatch
valid = "INSERT"


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
                pre = line[:(pre_pos + 1 + len("VALUES"))].replace(
                    "t_iou_overdue_list_to_us", "loan_overdue_manage_fee_list")
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    item = item.strip(",")
                    temp_arr = parse_sql_fields(item)
                    origin_id = temp_arr[0].lstrip("(")
                    # mutex.acquire()公用dbclient的时候必须加锁
                    new_id = MYDB.fetch_from_origin_id(
                        origin_id.replace("'", ""))
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
                fout.write(pre + " " + post + ";" + SEP)


start_time = time.clock()
conver_file("t_iou_overdue_list_to_us.sql",
            "/home/pangqiqiang/t_iou_overdue_list_to_us_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
