#   단독 다가구 매매 실거래 ( 지번 없음 )

import pprint
import re
import sys
import func

totcd = func.dbcon()


for j in range(0,len(totcd)): #
    year = 200601
    month = 1
    container_list = []
    print('현재 지역코드 : ' + totcd[j][0] + ' / index : ' + str(j))
    while year <= 201709:
        if year % 100 == 13:
            year += 100 - 12

        nowcode = totcd[j][0]

        res = func.grab_data(nowcode,str(year),'아파트')

        if res == 0:
            year += month
            continue

        #print(type(res))
        if type(res) == dict :
            # print("fuck u")
            res = [res]

        container_list.extend(res)
        #print(len(container_list))
        year += month

    # print("get out~")

    #func.indata_01(container_list)  #연립다세대
    #func.indata_02(container_list)  #아파트
    #func.indata_03(container_list)  #단독 다가구


print('아파트 매매 실거래 200601 ~ 201709 까지 데이터 삽입 완료')
