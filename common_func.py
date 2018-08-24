#!/user/bin/env python
#-*-coding:utf-8-*-

import time


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
        time_obj = time.mktime(time_arr)
        return "'" + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_obj)) + "'"
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
        time_obj = time.mktime(time_arr)
        return "'" + time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time_obj)) + "'"
    except ValueError:
        return "NULL"


if __name__ == "__main__":
    print(datetime2timestamp("'2018-08-25 12:20:00'"))
    print(date2timestamp("2018-08-25"))
    print(date2int("2018-08-25"))
    print(datetime2int("'2018-08-25 12:20:00'"))
