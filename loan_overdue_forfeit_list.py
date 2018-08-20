#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch

SEP = os.linesep


LOAN_INSTALLMENT_LIST_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                               database="dts_jjd", user="dev",
                                               password="KRkFcVCbopZbS8R7",
                                               tablename="loan_installment_list")


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
                pre = line[:(pre_pos + 1 + len("VALUES"))]
                new_values = []
                for item in gmatch(line, ",(", "),", pre_pos):
                    # print(item)
                    item = item.strip(",")
                    temp_arr = item.split(",")
                    c_iou_installment_list_id = temp_arr[1]
                    # loan_installment_list_id
                    temp_arr[0] = LOAN_INSTALLMENT_LIST_DB.fetch_from_origin_id(
                        c_iou_installment_list_id)
                    # amount
                    temp_arr[2] *= 100
                    #处理del
                    new_values.append(",".join(temp_arr))
                    # print(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


gmatch = iter_gmatch.gmatch
valid = "INSERT"
start_time = time.clock()
conver_file("loan_overdue_forfeit_list.sql",
            "/tmp/loan_overdue_forfeit_list_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
