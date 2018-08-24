#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch
import common_dbs
from common_func import *

SEP = os.linesep
LOAN_DB = common_dbs.LOAN_DB

valid = "INSERT"
gmatch = iter_gmatch.gmatch


def conver_file(input_file, output_file, valid):
    seq_count = 0
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
                pre = line[:(pre_pos + 1 + len("VALUES"))].replace("t_", "")
                new_values = []
                for item in gmatch(line, "(", ")", pre_pos):
                    # 维护自增id
                    seq_count += 1
                    # 输出映射数组
                    output_arr = list(range(39))
                    # id
                    output_arr[0] = "(" + str(seq_count)
                    item = item.strip(",")
                    input_arr = item.split(",")
                    # original_id
                    output_arr[1] = input_arr[0].lstrip("(")
                    # loan_id(c_iou_id)
                    output_arr[3] = input_arr[1]
                    output_arr[2] = LOAN_DB.fetch_from_origin_id(
                        output_arr[3].strip("'"))
                    for i in range(4, 7):
                        output_arr[i] = input_arr[i - 2]
                    # n_faimily_member
                    output_arr[8] = input_arr[5]
                    try:
                        output_arr[7] = int(output_arr[8])
                    except ValueError:
                        output_arr[7] = 0
                    for i in range(9, 37):
                        output_arr[i] = int(input_arr[i - 3])
                    # create_time
                    output_arr[37] = datetime2timestam(input_arr[34].rstrip(")"))
                    # update_time
                    output_arr[38] = str(output_arr[37]) + ")"
                    new_values.append(",".join([str(i) for i in output_arr]))
                    # print(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


start_time = time.clock()
conver_file("t_collection_return_info.sql",
            "/tmp/t_collection_return_info_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
