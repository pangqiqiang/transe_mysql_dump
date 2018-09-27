#!/user/bin/env python
#-*-coding:utf-8-*-

import time
import re
from collections import deque
import os
import threading
import csv

SEP = os.linesep


def write_lines_in_file(filename, line):
    fobj = open(filename, "a")
    try:
        fobj.write(line + SEP)
    finally:
        fobj.close()

def write_to_csv(filename, output_arr):
    with  open(filename, "a") as out_csv:
        csv_writer = csv.writer(out_csv, newline='', delimiter="\t", 
                                doublequote=False,escapechar='\\', quotechar="'",
                                strict=True)
        csv_writer.writerow(output_arr)


def get_from_map(map, item):
    return map.get(str(item).strip("'"))


def parse_sql_fields(str):
    origin_arr = str.split(",")
    dup = deque(origin_arr[::])
    fields = []
    while True:
        try:
            item = dup.popleft()
            if item.find("'") == -1:
                fields.append(item)
            elif item.count("'") % 2 == 0:
                fields.append(item)
            else:
                temp = item
                while dup and temp.count("'") % 2 != 0:
                    temp += "," + dup.popleft()
                    # print(temp)
                fields.append(temp)
        except IndexError:
            break
    return fields


def unescape_quote(value):
    pat1 = re.compile(r'\\{2}')
    value = value.rstrip().rstrip(";")
    value = value.replace("（", "(")
    value = value.replace("）", ")")
    value = pat1.sub("", value)
    value = value.replace("\\'", "")
    value = value + ","
    return value


def float_char_to_int(str):
    try:
        str = str.strip("'")
        res = int(float(str) * 100 + 0.5)
    except ValueError:
        res = 0
    return res


def date2timestamp(date):
    try:
        date = date.strip("'")
        time_arr = time.strptime(date, "%Y-%m-%d")
        # time_obj = time.mktime(time_arr)
        # return "'" + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_obj)) + "'"
        return "'" + date + "'"
    except ValueError:
        return "NULL"


def date2int(date):
    try:
        date = date.strip("'")
        time_arr = time.strptime(date, "%Y-%m-%d")
        return int(time.mktime(time_arr))
    except ValueError:
        return "NULL"


def datetime2int(date):
    try:
        date = date.strip("'")
        time_arr = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        return int(time.mktime(time_arr))
    except ValueError:
        return "NULL"


def datetime2timestamp(date):
    try:
        date = date.strip("'")
        time_arr = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        # time_obj = time.mktime(time_arr)
        # return "'" + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_obj)) + "'"
        return "'" + date + "'"
    except ValueError:
        return "NULL"


def get_cur_time_str():
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',
                             time.localtime(time.time()))
    return "'" + time_str + "'"


if __name__ == "__main__":
    print(datetime2timestamp("'2018-08-25 12:20:00'"))
    print(date2timestamp("2018-08-25"))
    print(date2int("2018-08-25"))
    print(datetime2int("'2018-08-25 12:20:00'"))
    print(parse_sql_fields("a,b,c,[{m,[3,2,1]n},{k,v}],s,'sss,seele'"))
