#!/usr/bin/env python
#-*-coding:utf-8-*-

import torndb_handler
import threading
import os
import time
import iter_gmatch
from common_func import *
import multi_thread_dbs


SEP = os.linesep
OUTPUT_FILE = "/home/pangqiqiang/loan_t_iou_history_out.sql"
OUTPUT_JSON_FILE = "/home/pangqiqiang/t_iou_histoy_json.sql"
INPUT_FILE0 = "t_iou_history000"
INPUT_FILE1 = "t_iou_history001"
INPUT_FILE2 = "t_iou_history002"
INPUT_FILE3 = "t_iou_history003"
INPUT_FILE4 = "t_iou_history004"


# 导入数据库类
PASSWORD_DB0 = multi_thread_dbs.USER_PASSPORT_DB0
PASSWORD_DB1 = multi_thread_dbs.USER_PASSPORT_DB1
PASSWORD_DB2 = multi_thread_dbs.USER_PASSPORT_DB2
PASSWORD_DB3 = multi_thread_dbs.USER_PASSPORT_DB3
PASSWORD_DB4 = multi_thread_dbs.USER_PASSPORT_DB4

TRADE_DB0 = multi_thread_dbs.TRADE_DB0
TRADE_DB1 = multi_thread_dbs.TRADE_DB1
TRADE_DB2 = multi_thread_dbs.TRADE_DB2
TRADE_DB3 = multi_thread_dbs.TRADE_DB3
TRADE_DB4 = multi_thread_dbs.TRADE_DB4

LOAN_OFFLINE_DB0 = multi_thread_dbs.LOAN_OFFLINE_DB0
LOAN_OFFLINE_DB1 = multi_thread_dbs.LOAN_OFFLINE_DB1
LOAN_OFFLINE_DB2 = multi_thread_dbs.LOAN_OFFLINE_DB2
LOAN_OFFLINE_DB3 = multi_thread_dbs.LOAN_OFFLINE_DB3
LOAN_OFFLINE_DB4 = multi_thread_dbs.LOAN_OFFLINE_DB4

PRODUCT_BID_DB0 = multi_thread_dbs.PRODUCT_BID_DB0
PRODUCT_BID_DB1 = multi_thread_dbs.PRODUCT_BID_DB1
PRODUCT_BID_DB2 = multi_thread_dbs.PRODUCT_BID_DB2
PRODUCT_BID_DB3 = multi_thread_dbs.PRODUCT_BID_DB3
PRODUCT_BID_DB4 = multi_thread_dbs.PRODUCT_BID_DB3

BID_DB0 = multi_thread_dbs.BID_DB0
BID_DB1 = multi_thread_dbs.BID_DB1
BID_DB2 = multi_thread_dbs.BID_DB2
BID_DB3 = multi_thread_dbs.BID_DB3
BID_DB4 = multi_thread_dbs.BID_DB4


JSON_PRE = "INSERT INTO TEMP_JSON VALUES "

PURPOSE_TYPE_MAP = {"个体经营": 0, "消费": 1, "助学": 2,
                    "创业": 3, "租房": 4, "旅游": 5, "装修": 6, "医疗": 7}


gmatch = iter_gmatch.gmatch
# 进程锁解决资源冲突
mutex = threading.Lock()
# 线程池
threads = []
valid = "INSERT"
# 维护自增id
seq_count = 1049173


# 定义线程类
class myThread(threading.Thread):
    def __init__(self, func, args):
        threading.Thread.__init__(self)
        self.func = func
        self.args = args

    def run(self):
        self.func(*self.args)


def conver_file(input_file, output_file, output_file2, valid,
                password_db, trade_db, loan_offline_db, product_bid_db, bid_db):
    global seq_count
    # json_count = 0
    with open(input_file, 'r') as fin:
        for line in fin:
            if not line.startswith(valid):
                continue
            line = unescape_quote(line)
            pre_pos = line.find("VALUES")
            if pre_pos == -1:
                continue
            post = line[(pre_pos + 1):]
            pre = line[:(pre_pos + 1 + len("VALUES"))
                       ].replace("t_iou_history", "loan")
            new_values = []
            json_values = []
            for item in gmatch(line, "(", "),", pre_pos):
                # 输出映射数组
                out_arr = list(range(50))
                json_arr = []
                # id
                # 维护自增id
                mutex.acquire()
                seq_count += 1
                out_arr[0] = "(" + str(seq_count)
                mutex.release()
                item = item.strip(",")
                input_arr = parse_sql_fields(item)
                # original_id
                out_arr[1] = input_arr[0].lstrip("(")
                # json表主键
                json_arr.append(input_arr[0])
                # borrower_uid,c_borrower_id
                out_arr[3] = input_arr[1]
                out_arr[2] = password_db.fetch_from_salt(
                    out_arr[3])
                # lender_uid,c_lender_id
                out_arr[5] = input_arr[2]
                out_arr[4] = password_db.fetch_from_salt(
                    out_arr[5])
                # guarantee_uid,c_guarantee_id
                out_arr[7] = input_arr[3]
                out_arr[6] = password_db.fetch_from_salt(
                    out_arr[7])
                # service_amount
                out_arr[8] = float_char_to_int(input_arr[5])
                # guarantee_amount
                out_arr[9] = float_char_to_int(input_arr[6])
                # total_amount
                out_arr[10] = float_char_to_int(input_arr[15])
                # amount,[11]
                out_arr[11] = float_char_to_int(input_arr[4])
                # interest_amount
                out_arr[12] = float_char_to_int(input_arr[7])
                # forfeit_amount
                out_arr[13] = float_char_to_int(input_arr[8])
                # overdue_manage_amount
                out_arr[14] = float_char_to_int(input_arr[9])
                # overdue_manage_amount_special
                out_arr[15] = float_char_to_int(input_arr[10])
                # return_overdue_manage_amount
                out_arr[16] = float_char_to_int(input_arr[16])
                # return_overdue_manage_id,c_repay_forfeit_id
                out_arr[18] = input_arr[17]
                out_arr[17] = trade_db.fetch_from_origin_id(
                    out_arr[18])
                # get_amount
                out_arr[19] = float_char_to_int(input_arr[18])
                # got_amount
                out_arr[20] = float_char_to_int(input_arr[19])
                # paid_amount
                out_arr[21] = float_char_to_int(input_arr[12])
                # paid_interest_amount
                out_arr[22] = float_char_to_int(input_arr[13])
                # paid_forfeit_amount
                out_arr[23] = float_char_to_int(input_arr[14])
                # paid_overdue_manage_amount
                out_arr[24] = float_char_to_int(input_arr[11])
                # c_purpose,purpose_type
                out_arr[26] = input_arr[20]
                out_arr[25] = PURPOSE_TYPE_MAP.get(
                    out_arr[26])
                out_arr[25] = 8 if out_arr[25] == None else out_arr[25]
                # memo,repay_type,period,interest_rate,overdue_rate
                for i in range(27, 32):
                    out_arr[i] = input_arr[i - 6]
                # borrow_time[32](t_borrow_tm)[33]
                out_arr[33] = input_arr[26]
                out_arr[32] = datetime2timestamp(out_arr[33])
                # repay_time[34](t_repay_tm)[35]
                out_arr[35] = input_arr[27]
                out_arr[34] = datetime2timestamp(out_arr[35])
                # online_status,[36],# pic_list,[37],source_type,[38]
                for i in range(36, 39):
                    out_arr[i] = input_arr[i - 8]
                # source_id
                out_arr[40] = input_arr[31]
                # 根据source_type获取source_id
                if int(out_arr[38]) == 0:
                    out_arr[39] = loan_offline_db.fetch_from_origin_id(
                        out_arr[40])
                elif int(out_arr[38]) == 1:
                    out_arr[39] = product_bid_db.fetch_from_origin_id(
                        out_arr[40])
                else:
                    out_arr[39] = bid_db.fetch_from_origin_id(
                        out_arr[40])
                # valid_status,[41],end_status,[42]
                out_arr[41], out_arr[42] = "b'1'", "b'1'"
                # version_number,[43],local_agreement_status,[44],ecloud_agreement_status,[45]
                for i in range(43, 46):
                    out_arr[i] = input_arr[i - 10]
                # create_time,[46]
                out_arr[46] = datetime2timestamp(input_arr[36])
                # update_time[47]
                out_arr[47] = str(datetime2timestamp(
                    input_arr[37].rstrip(")")))
                # borrower_ip,lender_ip
                out_arr[48] = "NULL"
                out_arr[49] = "NULL" + ")"
                for i in range(38, len(input_arr)):
                    json_arr.append(input_arr[i])
                json_arr.insert(-1, "NULL")
                json_arr.insert(-1, "NULL")
                new_values.append(
                    ",".join([str(i) for i in out_arr]))
                json_values.append(",".join(json_arr))
            post = ",".join(new_values)
            json_post = ",".join(json_values)
            mutex.acquire()
            write_lines_in_file(output_file, pre + " " + post + ";")
            write_lines_in_file(output_file2, JSON_PRE + json_post + ";")
            mutex.release()


start_time = time.time()
# 创建线程
thread0 = myThread(conver_file, (INPUT_FILE0, OUTPUT_FILE, OUTPUT_JSON_FILE, valid,
                                 PASSWORD_DB0, TRADE_DB0, LOAN_OFFLINE_DB0, PRODUCT_BID_DB0, BID_DB0))
thread1 = myThread(conver_file, (INPUT_FILE1, OUTPUT_FILE, OUTPUT_JSON_FILE, valid,
                                 PASSWORD_DB1, TRADE_DB1, LOAN_OFFLINE_DB1, PRODUCT_BID_DB1, BID_DB1))
thread2 = myThread(conver_file, (INPUT_FILE2, OUTPUT_FILE, OUTPUT_JSON_FILE, valid,
                                 PASSWORD_DB2, TRADE_DB2, LOAN_OFFLINE_DB2, PRODUCT_BID_DB2, BID_DB2))
thread3 = myThread(conver_file, (INPUT_FILE3, OUTPUT_FILE, OUTPUT_JSON_FILE, valid,
                                 PASSWORD_DB3, TRADE_DB3, LOAN_OFFLINE_DB3, PRODUCT_BID_DB3, BID_DB3))
thread4 = myThread(conver_file, (INPUT_FILE4, OUTPUT_FILE, OUTPUT_JSON_FILE, valid,
                                 PASSWORD_DB4, TRADE_DB4, LOAN_OFFLINE_DB4, PRODUCT_BID_DB4, BID_DB4))
# 添加线程到线程列表
threads.append(thread0)
threads.append(thread1)
threads.append(thread2)
threads.append(thread3)
threads.append(thread4)

# 启动进程
for t in threads:
    t.start()

# 等待所有线程完成
for t in threads:
    t.join()

end_time = time.time()
time_elapse = (end_time - start_time)
print("All documents complete!!!\nTime elapsed: %.3f sec" % time_elapse)
