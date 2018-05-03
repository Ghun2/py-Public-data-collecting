#   아파트 전월세 실거래

import pprint
import re
import sys
import r_func

totcd = r_func.dbcon()


for j in range(96,len(totcd)): #
    year = 201801
    month = 1
    container_list = []
    print('현재 지역코드 : ' + totcd[j][0] + ' / index : ' + str(j))
    while year <= 201803:
        if year % 100 == 13:
            year += 100 - 12

        nowcode = totcd[j][0]

        res = r_func.grab_data(nowcode,str(year),'아파트')

        if res == 0:
            year += month
            # print(year," 0개")
            continue
        # print(len(res))
        #print(type(res))
        if type(res) == dict :
            # print("fuck u")
            res = [res]

        container_list.extend(res)
        #print(len(container_list))
        year += month

    # print("get out~")

    if len(container_list) == 0:
        continue
    #r_func.indata_01(container_list)  #연립다세대
    r_func.indata_02(container_list)  #아파트

print('아파트 전월세 실거래 200601 ~ 201712 까지 데이터 삽입 완료')
