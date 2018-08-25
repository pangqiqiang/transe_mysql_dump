#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
from common_func import *
import common_dbs

SEP = os.linesep
LOAN_INSTALLMENT_LIST_DB = common_dbs.LOAN_INSTALLMENT_LIST_DB
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
                    "t_iou_overdue_list_offline", "loan_overdue_forfeit_list_offline")
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    # 输出映射数组
                    output_arr = list(range(7))
                    item = item.strip(",")
                    input_arr = parse_sql_fields(item)
                    # print(input_arr)
                    # loan_installment_list_id[0](c_iou_installment_list_id)[1]
                    output_arr[1] = input_arr[1]
                    output_arr[0] = LOAN_INSTALLMENT_LIST_DB.fetch_from_origin_id(
                        output_arr[1].strip("'"))
                    output_arr[0] = "(" + str(output_arr[0])
                    # overdue_days,[2]
                    output_arr[2] = input_arr[4]
                    # amount
                    output_arr[3] = int(float(input_arr[3]) * 100 + 0.5)
                    # forfeit_amount,[4]
                    output_arr[4] = input_arr[5]
                    # create_time,[5]
                    output_arr[5] = input_arr[6].strip(")")
                    # 处理del
                    output_arr[6] = "b'0'" + ")"
                    new_values.append(",".join([str(i) for i in output_arr]))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


start_time = time.clock()
conver_file("t_iou_overdue_list_offline.sql",
            "/home/pangqiqiang/t_iou_overdue_list_offline_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
