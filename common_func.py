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
