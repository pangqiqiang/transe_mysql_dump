#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import os
import time
import iter_gmatch


SEP = os.linesep
PASSWORD_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                  database="dts_jjd", user="dev",
                                  password="KRkFcVCbopZbS8R7",
                                  tablename="user_passport")

TRADE_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                               database="dts_jjd", user="dev",
                               password="KRkFcVCbopZbS8R7",
                               tablename="trade")

LOAN_OFFLINE_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                      database="dts_jjd", user="dev",
                                      password="KRkFcVCbopZbS8R7",
                                      tablename="loan_offline")

PRODUCT_BID_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                     database="dts_jjd", user="dev",
                                     password="KRkFcVCbopZbS8R7",
                                     tablename="product_bid")

BID_BID_DB = torndb_handler.MyDB(host="rm-2ze208m29he873gr9.mysql.rds.aliyuncs.com:3306",
                                 database="dts_jjd", user="dev",
                                 password="KRkFcVCbopZbS8R7",
                                 tablename="bid")


FETCH_FROM_SALT = PASSWORD_DB.fetch_from_salt
FETCH_FROM_ORIGIN_ID = TRADE_DB.fetch_from_origin_id


PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}


DBS_MAP = {0: LOAN_OFFLINE_DB.fetch_from_origin_id,
           1: PRODUCT_BID_DB.fetch_from_origin_id}


def get_info_deal_null(query, method):
    if isinstance(query, str):
        return method(query.replace("'", ""))
    return query


def date2int(date):
    try:
        time_arr = time.strptime(date, "%Y-%m-%d")
        return int(time.mktime(time_arr))
    except ValueError:
        return 0


def datetime2int(date):
    try:
        time_arr = time.strptime(date, "%Y-%m-%d %H:%M:%S")
        return int(time.mktime(time_arr))
    except ValueError:
        return 0


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
                for item in gmatch(line, "(", ")", pre_pos):
                    item = item.strip(",")
                    temp_arr = item.split(",")
                    origin_id = temp_arr[1]
                    c_borrower_id = temp_arr[3]
                    c_lender_id = temp_arr[5]
                    c_guarantee_id = temp_arr[7]
                    borrower_uid = get_info_deal_null(
                        c_borrower_id, FETCH_FROM_SALT)
                    lender_uid = get_info_deal_null(
                        c_lender_id, FETCH_FROM_SALT)
                    guarantee_uid = get_info_deal_null(
                        c_guarantee_id, FETCH_FROM_SALT)
                    temp_arr[2] = borrower_uid
                    temp_arr[4] = lender_uid
                    temp_arr[6] = guarantee_uid
                    for i in range(8, 17):
                        temp_arr[i] = str(float(temp_arr[i]) * 100)
                    c_repay_forfeit_id = temp_arr[18]
                    temp[17] = get_info_deal_null(
                        c_repay_forfeit_id, FETCH_FROM_ORIGIN_ID)
                    # 处理一堆金额
                    for i in range(19, 25):
                        temp_arr[i] = str(float(temp_arr[i]) * 100)
                    c_purpose = temp_arr[26]
                    temp_arr[25] = PURPOSE_TYPE_MAP.get(c_purpose) or 8
                    t_borrow_tm = temp_arr[33]
                    temp_arr[32] = date2int(t_borrow_tm)
                    t_repay_tm = temp_arr[35]
                    temp_arr[34] = date2int(t_repay_tm)
                    source_type = temp_arr[38]
                    c_source_id = temp_arr[39]
                    db_method = DBS_MAP.get(
                        source_type) or BID_BID_DB.fetch_from_origin_id
                    get_info_deal_null(c_source_id, db_method)
                    t_close_tm = temp_arr[50]
                    if t_close_tm and len(t_close_tm) > 0:
                        temp_arr[47] = datetime2int(t_close_tm)
                    new_values.append(",".join(temp_arr))
                post = ",".join(new_values)
                fout.write(pre + " " + post + SEP)


gmatch = iter_gmatch.gmatch
valid = "INSERT"
start_time = time.clock()
conver_file("loan.sql",
            "/tmp/loan_out.sql", valid)
end_time = time.clock()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
