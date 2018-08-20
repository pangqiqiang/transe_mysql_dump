#!/user/bin/env python
#-*-coding:utf-8-*-


def gmatch(str, start, end, init):
    start_pos = str.find(start, init)
    end_pos = str.find(end, init)
    while start_pos != -1 and end_pos != -1:
        temp_str = str[start_pos:end_pos + 1]
        #处理嵌套括号的问题
        while temp_str.count(start) != temp_str.count(end):
            end_pos = str.find(end, end_pos + 1)
            temp_str = str[start_pos:end_pos + 1]
        yield str[start_pos:end_pos + 1]
        start_pos = str.find(start, end_pos + 1)
        end_pos = str.find(end, end_pos + 1)


if __file__ == "__main__":
    for item in gmatch("(ab(cd)), ssss(cd), ldjdldjl(de(me))", "(", ")", 0):
        print(item)
