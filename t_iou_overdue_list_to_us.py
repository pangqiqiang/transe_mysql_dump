#!/usr/bin/env python
#-*-coding:utf-8-*-
#
import torndb_handler
import os
import time
import iter_gmatch


SEP = os.linesep
MYDB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                           database="dts_jjd", user="dev",
                           password="KRkFcVCbopZbS8R7",
                           tablename="loan_installment_list")


def conver_file(input_file, output_file, valid):
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
                    temp_arr = item.split(",")
                    origin_id = temp_arr[0].lstrip("(")
                    # mutex.acquire()公用dbclient的时候必须加锁
                    new_id = MYDB.fetch_from_origin_id(
                        origin_id.replace("'", ""))
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
                fout.write(pre + " " + post + SEP)


gmatch = iter_gmatch.gmatch
valid = "INSERT"
start_time = time.clock()
conver_file("t_iou_overdue_list_to_us.sql",
            "/tmp/t_iou_overdue_list_to_us_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
