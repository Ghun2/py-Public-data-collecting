import urllib.request
import json
import xmltodict
import pymysql
import pymongo
import pprint
import re
import sys
import time
import threading
from socket import timeout

count = 0

def mdb_insert_one(iplc,conn):

    curs = conn.cursor()
    sql = "insert into getAphusBassInfo(kaptCode, kaptName, kaptAddr, codeSaleNm, codeHeatNm, kaptTarea, kaptDongCnt, kaptdaCnt, kaptBcompany, kaptAcompany, kaptTel, kaptFax, kaptUrl, codeAptNm, doroJuso, hoCnt, codeMgrNm, codeHallNm, kaptUsedate, kaptMarea, kaptMparea_60, kaptMparea_85, kaptMparea_135, kaptMparea_136, privArea, bjdCode, platPlc) values (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s)"
    curs.execute(sql,iplc)
    conn.commit()

    print('one insert complete')

def mdb_insert_bulk(trt,collec):

    print('error (bulk)')
    program_exit()

    curs = conn.cursor()
    sql = "insert into getLegaldongAptList(kaptCode,kaptName,BJD_code,BJD_sido,BJD_sigungu,BJD_dong,BJD_ri) values (%s, %s, %s, %s, %s, %s, %s)"
    curs.executemany(sql,trt)
    conn.commit()

    print('bulk insert complete')

def call_timer() :
    print("time out .. stop")
    program_exit()

def rest_call(kapt_code):

    global curr_key

    url = 'http://apis.data.go.kr/1611000/AptBasisInfoService/getAphusBassInfo?'
    params = 'kaptCode='+ kapt_code +'&ServiceKey='+ curr_key


    request = urllib.request.Request(url+params)

    request.get_method = lambda: 'GET'

    while 1:
        try:
            response_body = urllib.request.urlopen(request, timeout=30).read()
        except timeout:
            print('timeout! repeat')
            continue
        else:
            print('restcall successful.')
            break

    return response_body

def grab_data(kapt_code) :       #REST API

    global curr_key

    response_body = rest_call(kapt_code)

    tempstr = str(response_body)

    response_body = json.loads(json.dumps(xmltodict.parse(response_body)))

    # print(response_body['response']['body']['items']['item'])
    # vv = response_body['response']['body']['items']['item']
    #
    # for value in vv:
    #     print(value)
    #
    # program_exit()

    # check object
    # print(tempstr)
    # print(response_body['response']['body']['item'])
    # program_exit()

    if tempstr[3] != '?' :

        errtxt = ''

        rest_err_code = response_body['OpenAPI_ServiceResponse']['cmmMsgHeader']['returnReasonCode']
        #print(rest_err_code)
        if rest_err_code == '22' :
            curr_key = key_container.pop(0)
            errtxt = kapt_code+' change key , spare key : '+ str(len(key_container))
            err_log(errtxt)
            print(errtxt)

            if curr_key == '0' :
                errtxt = kapt_code+' 남은 키가 없습니다.'
                print(errtxt)

                #로그파일에 쓰기 , db 등 닫고 프로그램 종료
                err_log(errtxt)
                program_exit()

            response_body = rest_call(kapt_code)
            response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
        else :
            errtxt = in_sgg+in_bjd+' REST_ERROR_number : '+rest_err_code
            print(errtxt)

            #로그파일에 쓰기 , db 등 닫고 프로그램 종료
            err_log(errtxt)
            program_exit()

    # if(response_body['response']['body']['totalCount'] == '0') :
    #     return 0
    else :
        return response_body['response']['body']['item']

def err_log(txt):   # 에러로그 쓰기
    curr_time = time.asctime(time.localtime(time.time()))   #현재 시간
    f = open("insert_log.txt",'a')   #로그 file open
    f.write(curr_time + ' ' + txt + '\n')
    f.close()

def program_exit(): # 프로그램 종료
    cur.close()     # MariaDB 커서 close
    conn.close()    # MariaDB 연결 close

    sys.exit()      #프로그램 종료


                        # MAIN START

# Initiating
    # Place List
sidos = ['"서울특별시"'];
    # / Place List

    # Key Setting
    #Require Customizing
call_key1 = ''


key_container = []
key_container.append(call_key1)
key_container.append(call_key2)
key_container.append(call_key3)
key_container.append('0')

curr_key = key_container.pop(0)
    # / Key Setting

    # MongoDB connection
client = pymongo.MongoClient("localhost", 27017)
db = client.raiz
collec = db.getBrTitleInfo
    # / MongoDB connection

    # MariaDB connection
conn = pymysql.connect(
    #Require Customizing
    host='', user='', passwd='', db='', charset='utf8'
    )
    # / MariaDB connection
# / Initiating

for i in sidos:
    place = 'select kaptCode, BJD_code, BJD_sido, BJD_sigungu, BJD_dong, BJD_ri from getLegaldongAptList'
    # place = 'select BJD_code,BJD_sido,BJD_sigungu,BJD_dong,BJD_ri from insert_BJD where BJD_code = 4146510300'
    cur = conn.cursor()
    cur.execute(place)
    totplc = cur.fetchall()

    leng = len(totplc)

    for j in range(0,leng):
        plc = totplc[j]

        kapt_code = str(plc[0])

        curr_plc = plc[0]+" "+plc[1]+" "+plc[2]+" "+plc[3]+" "+plc[4]+" "+plc[5] + '  index = '+ str(j)
        print(curr_plc)

        res = grab_data(kapt_code)

        keylist = list(res.keys())

        totalkey = ['kaptCode',
                    'kaptName',
                    'kaptAddr',
                    'codeSaleNm',
                    'codeHeatNm',
                    'kaptTarea',
                    'kaptDongCnt',
                    'kaptdaCnt',
                    'kaptBcompany',
                    'kaptAcompany',
                    'kaptTel',
                    'kaptFax',
                    'kaptUrl',
                    'codeAptNm',
                    'doroJuso',
                    'hoCnt',
                    'codeMgrNm',
                    'codeHallNm',
                    'kaptUsedate',
                    'kaptMarea',
                    'kaptMparea_60',
                    'kaptMparea_85',
                    'kaptMparea_135',
                    'kaptdaSize_136',
                    'privArea'
                    ]

        if(res == 0) :
            print("0 insert complete")
            continue
        if(isinstance(res, dict)) :
            totalcnt = 1
        else :
            totalcnt = len(res)

        if totalcnt == 0 :
            print(str(totalcnt) + " insert complete")
            continue
        elif totalcnt == 1 :
            pltxt = plc[2]+" "+plc[3]+" "+plc[4]+" "+plc[5]
            iplc = []
            for a in range(0,len(totalkey)):
                chk = 0
                for b in range(0,len(keylist)):
                    if(totalkey[a] == keylist[b]) :
                        iplc.append(res[totalkey[a]])
                        chk = 1
                        break
                if(chk == 0) :
                    iplc.append('')
            # iplc.append(res['kaptCode'])
            # iplc.append(res['kaptName'])
            # iplc.append(res['kaptAddr'])
            # iplc.append(res['codeSaleNm'])
            # iplc.append(res['codeHeatNm'])
            # iplc.append(res['kaptTarea'])
            # iplc.append(res['kaptDongCnt'])
            # iplc.append(res['kaptdaCnt'])
            # iplc.append(res['kaptBcompany'])
            # iplc.append(res['kaptAcompany'])
            # iplc.append(res['kaptTel'])
            # iplc.append(res['kaptFax'])
            # iplc.append(res['kaptUrl'])
            # iplc.append(res['codeAptNm'])
            # iplc.append(res['doroJuso'])
            # iplc.append(res['hoCnt'])
            # iplc.append(res['codeMgrNm'])
            # iplc.append(res['codeHallNm'])
            # iplc.append(res['kaptUsedate'])
            # iplc.append(res['kaptMarea'])
            # iplc.append(res['kaptMparea_60'])
            # iplc.append(res['kaptMparea_85'])
            # iplc.append(res['kaptMparea_135'])
            # iplc.append(res['kaptdaSize_136'])
            # iplc.append(res['privArea'])
            iplc.append(plc[1])
            iplc.append(pltxt)

            mdb_insert_one(iplc,conn)
        else :
            trt = []
            for q in range(0,totalcnt) :
                iplc = plc
                iplc = list(iplc)
                iplc.insert(0,res[q]['kaptName'])
                iplc.insert(0,res[q]['kaptCode'])
                trt.append(iplc)

            mdb_insert_bulk(trt,conn)

        print(str(totalcnt) + " insert complete")

    print(i + ' full inserted')

cur.close()
conn.close()
