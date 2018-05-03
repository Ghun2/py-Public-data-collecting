import os
import pprint
import json
import xmltodict
import sys
import pymysql
import urllib.request

pp = pprint.PrettyPrinter(indent=4)
# db fetch addr data
# ...
# ...
conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )

def exit(): # 프로그램 종료
    conn.close()    # MariaDB 연결 close
    sys.exit()      #프로그램 종료

def fetch_addrdata():
    sql = 'SELECT addr FROM raiz2.auction_mm'
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data

def indb(arr):    # Txt

    sql = "update auction_mm set address = %s, sigunguCd = %s, bjdongCd = %s, bun = %s, ji = %s, point = %s where addr = %s"
    curs = conn.cursor()
    curs.execute(sql,arr)
    conn.commit()

def find_addr(addr) :
    encText = urllib.parse.quote(addr)
    KakaoAK = ""
    url = "https://dapi.kakao.com/v2/local/search/address.json?query=" + encText

    request = urllib.request.Request(url)

    request.add_header("Authorization",KakaoAK)
    response = urllib.request.urlopen(request)
    rescode = response.getcode()

    if(rescode==200):
      response_body = response.read().decode('UTF-8')
      return json.loads(response_body)

    else:
      print("Error Code:" + rescode)
      return None



data = fetch_addrdata()

for i,r in enumerate(data):
    print('index = ',i,' ',r[0])
    result = find_addr(r[0])
    if result == None:
        exit()
    if result['meta']['total_count'] == 0 :
        print('not found !')
        continue
    arr = ['','','','','','','']
    arr[0] = result['documents'][0]['address']['address_name'] # 본 주소
    arr[1] = result['documents'][0]['address']['b_code'][:5] # 시군구 코드
    arr[2] = result['documents'][0]['address']['b_code'][5:] # 법정동 코드
    arr[3] = result['documents'][0]['address']['main_address_no'] # 번
    if result['documents'][0]['address']['sub_address_no'] == '':
        print('hi')
    arr[4] = result['documents'][0]['address']['sub_address_no'] # 지
    # point 파싱
    pnt = 'POINT('+result['documents'][0]['address']['y']+','+result['documents'][0]['address']['x']+')'
    arr[5] = pnt
    arr[6] = r[0]

    print(arr)

    indb(arr)

    # exit()














#
