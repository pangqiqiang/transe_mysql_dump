#!/usr/bin/env python
#-*-coding:utf-8-*-
#
import torndb_handler
import os


def gmatch(str, start, end, init):
    start_pos = str.find(start, init)
    end_pos = str.find(end, init)
    while start_pos != -1 and end_pos != -1:
        yield str[start_pos:end_pos + 1]
        start_pos = str.find(start, end_pos + 1)
        end_pos = str.find(end, end_pos + 1)


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
                pre = line[:(pre_pos + 1)]
                new_values = []
                for item in gmatch(line, ",(", "),", pre_pos):
                    # print(item)
                    item = item.strip(",")
                    temp_arr = item.split(",")
                    salt = temp_arr[0].lstrip("(")
                    # print(salt)
                    new_id = MYDB.fetch_id(salt.replace("'", ""))
                    # print(new_id)
                    temp_arr.insert(0, "(" + str(new_id))
                    temp_arr[1] = salt
                    new_values.append(",".join(temp_arr))
                    # print(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


MYDB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                           database="dts_jjd", user="dev",
                           password="KRkFcVCbopZbS8R7",
                           tablename="user_passport")
valid = "INSERT"
SEP = os.linesep

conver_file("test.sql",
            "/tmp/user_wechat_msg_out.sql", valid)
