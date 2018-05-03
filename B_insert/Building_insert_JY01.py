#   총괄 표제부 삽입

import urllib.request
import json
import xmltodict
import pymysql
import pprint
import re
import sys
import time
from socket import timeout

count = 0

# def mdb_insert_one(res,collec):
#     that = collec.insert_one(res)367
#     if that.inserted_id :
#         return 1
#     else :
#         print("mongo서버에 데이터 전부가 안들어갔습니다.")
#         program_exit()
#
#
# def mdb_insert_bulk(res,collec):
#
#     that = collec.insert_many(res)
#     inserted_cnt = len(that.inserted_ids)
#     if that.inserted_ids :
#         if len(res) != inserted_cnt :
#             print("mongo서버에 데이터 전부가 안들어갔습니다. " + inserted_cnt + " / " + len(res))
#             program_exit()
#         else :
#             return inserted_cnt
#     else :
#         print("mongo서버에 데이터 전부가 안들어갔습니다.")
#         program_exit()

def mdb_insert_one(res):

    vallist = tuple(res.values())
    keylist = list(res.keys())
    keystr = ""
    for value in range(0,len(keylist)-1):
        keystr = keystr + keylist[value] + ","
    keystr = keystr+keylist[len(keylist)-1]

    # program_exit()

    var_string = '%s,' * (len(res)-1) + '%s'

    #var_string = ', '.join('%s' * len(res))
    sql = 'INSERT INTO getBrExposPubuseAreaInfo (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(res))

    curs = conn.cursor()
    curs.execute(sql,vallist)
    conn.commit()



def mdb_insert_bulk(res):

    # each dict length check
    reslen = len(res)
    exeptcnt = []
    for o in range(0,reslen):
        if len(res[o]) != 40:
            exeptcnt.append(o)

    for u in range(len(exeptcnt)-1,-1,-1):
        exeptres = res.pop(exeptcnt[u])
        mdb_insert_one(exeptres)

    vallist = []
    for b in range(0,len(res)):
        vallist.append(tuple(res[b].values()))


    # vallist = tuple(res[0].values())
    # print(vallist)

    keylist = list(res[0].keys())
    keystr = ""
    for value in range(0,len(keylist)-1):
        keystr = keystr + keylist[value] + ","
    keystr = keystr+keylist[len(keylist)-1]

    # program_exit()

    var_string = '%s,' * (len(res[0])-1) + '%s'

    #var_string = ', '.join('%s' * len(res))
    sql = 'INSERT INTO getBrExposPubuseAreaInfo (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(res))

    curs = conn.cursor()
    curs.executemany(sql,vallist)
    conn.commit()



def call_timer() :
    print("time out .. stop")
    program_exit()



def rest_call(BJD_code):

    global curr_key

    in_sgg = str(BJD_code[:5])
    in_bjd = str(BJD_code[5:])

    url = 'http://apis.data.go.kr/1611000/BldRgstService/getBrExposPubuseAreaInfo?'

    params = 'sigunguCd='+ in_sgg +'&bjdongCd='+ in_bjd +'&numOfRows=0&ServiceKey='+ curr_key

    request = urllib.request.Request(url+params)

    request.get_method = lambda: 'GET'

    while 1:
        try:
            response_body = urllib.request.urlopen(request, timeout=30).read()
        except timeout:
            continue
        else:
            break

        ##########
    response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
    reqnum = int(response_body['response']['body']['totalCount'])

    if reqnum >= 40000:
        divcount = 3

        if reqnum >= 100000:
            divcount = 8

        if reqnum >= 300000:
            divcount = 12

        numrow = reqnum/divcount+10
        numrow = int(numrow)

        for i in range(1,divcount+1):
            params = 'sigunguCd='+ in_sgg +'&bjdongCd='+ in_bjd +'&pageNo='+ str(i) +'&numOfRows='+ str(numrow) +'&ServiceKey='+ curr_key

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

            response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
            res = response_body['response']['body']['items']['item']

            mdb_insert_bulk(res)
            print('divide' + str(i) + ' bulk insert complete : '+ str(len(res)))

        print(str(reqnum) + " insert complete")

        return 0
    else :

        params = 'sigunguCd='+ in_sgg +'&bjdongCd='+ in_bjd +'&numOfRows=200000&ServiceKey='+ curr_key

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

def grab_data(BJD_code) :       #REST API

    global curr_key

    response_body = rest_call(BJD_code)

    if response_body == 0:
        return 1

    tempstr = str(response_body)

    response_body = json.loads(json.dumps(xmltodict.parse(response_body)))

    if tempstr[3] != '?' :

        errtxt = ''

        rest_err_code = response_body['OpenAPI_ServiceResponse']['cmmMsgHeader']['returnReasonCode']
        #print(rest_err_code)
        if rest_err_code == '22' :
            curr_key = key_container.pop(0)
            errtxt = BJD_code+' change key , spare key : '+ str(len(key_container))
            err_log(errtxt)
            print(errtxt)

            if curr_key == '0' :
                errtxt = BJD_code+' 남은 키가 없습니다.'
                print(errtxt)

                #로그파일에 쓰기 , db 등 닫고 프로그램 종료
                err_log(errtxt)
                program_exit()

            response_body = rest_call(BJD_code)
            response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
        else :
            errtxt = BJD_code+' REST_ERROR_number : '+rest_err_code
            print(errtxt)

            #로그파일에 쓰기 , db 등 닫고 프로그램 종료
            err_log(errtxt)
            program_exit()

    if(response_body['response']['body']['totalCount'] == '0') :
        return 0
    else :
        return response_body['response']['body']['items']['item']

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
# sidos = ['"서울특별시"','"부산광역시"','"대구광역시"',
# '"인천광역시"','"광주광역시"','"대전광역시"','"울산광역시"',
# '"세종특별자치시"','"경기도"','"강원도"','"충청북도"','"충청남도"',
# '"전라북도"','"전라남도"','"경상북도"','"경상남도"','"제주특별자치도"'];
#sidos = ['"경상북도"','"경상남도"','"제주특별자치도"'];

    # / Place List
# dellist = ['regstrGbCd','regstrKindCd','regstrKindCdNm','splotNm','block',
# 'lot','naRoadCd','naBjdongCd','naUgrndCd','naMainBun','naSubBun','mainAtchGbCd',
# 'strctCd','mainPurpsCd','roofCd','pmsnoYear','pmsnoKikCd','pmsnoKikCdNm','pmsnoGbCd',
# 'pmsnoGbCdNm','itgBldGrade','itgBldCert',]

# pjblist = ['emgenUseElvtCnt', 'lot', 'bcRat', 'engrEpi', 'hhldCnt', 'newPlatPlc',
#     'engrRat', 'archArea', 'etcRoof', 'mainAtchGbCd', 'pmsnoGbCd', 'gnBldGrade',
#     'mainAtchGbCdNm', 'bylotCnt', 'atchBldArea', 'indrMechArea', 'stcnsDay',
#     'mgmBldrgstPk', 'roofCdNm', 'ugrndFlrCnt', 'bjdongCd', 'oudrAutoArea',
#     'regstrGbCdNm', 'indrAutoUtcnt', 'vlRatEstmTotArea', 'pmsDay', 'naSubBun',
#     'gnBldCert', 'oudrMechArea', 'indrAutoArea', 'useAprDay', 'naBjdongCd',
#     'crtnDay', 'totDongTotArea', 'pmsnoYear', 'grndFlrCnt', 'bun', 'rnum',
#     'regstrKindCdNm', 'totArea', 'heit', 'oudrAutoUtcnt', 'regstrGbCd',
#     'pmsnoGbCdNm', 'strctCdNm', 'dongNm', 'roofCd', 'mainPurpsCdNm', 'etcStrct',
#     'platGbCd', 'splotNm', 'ji', 'engrGrade', 'indrMechUtcnt', 'oudrMechUtcnt',
#     'platArea', 'etcPurps', 'naRoadCd', 'fmlyCnt', 'naUgrndCd', 'atchBldCnt',
#     'pmsnoKikCdNm', 'itgBldGrade', 'platPlc', 'bldNm', 'vlRat', 'sigunguCd',
#     'pmsnoKikCd', 'strctCd', 'regstrKindCd', 'hoCnt', 'mainPurpsCd', 'itgBldCert',
#     'block', 'rideUseElvtCnt', 'naMainBun']
#
# pjbstr = """emgenUseElvtCnt, lot, bcRat, engrEpi, hhldCnt, newPlatPlc,
#     engrRat, archArea, etcRoof, mainAtchGbCd, pmsnoGbCd, gnBldGrade,
#     mainAtchGbCdNm, bylotCnt, atchBldArea, indrMechArea, stcnsDay,
#     mgmBldrgstPk, roofCdNm, ugrndFlrCnt, bjdongCd, oudrAutoArea,
#     regstrGbCdNm, indrAutoUtcnt, vlRatEstmTotArea, pmsDay, naSubBun,
#     gnBldCert, oudrMechArea, indrAutoArea, useAprDay, naBjdongCd,
#     crtnDay, totDongTotArea, pmsnoYear, grndFlrCnt, bun, rnum,
#     regstrKindCdNm, totArea, heit, oudrAutoUtcnt, regstrGbCd,
#     pmsnoGbCdNm, strctCdNm, dongNm, roofCd, mainPurpsCdNm, etcStrct,
#     platGbCd, splotNm, ji, engrGrade, indrMechUtcnt, oudrMechUtcnt,
#     platArea, etcPurps, naRoadCd, fmlyCnt, naUgrndCd, atchBldCnt,
#     pmsnoKikCdNm, itgBldGrade, platPlc, bldNm, vlRat, sigunguCd,
#     pmsnoKikCd, strctCd, regstrKindCd, hoCnt, mainPurpsCd, itgBldCert,
#     block, rideUseElvtCnt, naMainBun"""

    # Key Setting
call_key1 = ''


key_container = []
#key_container.append(call_key1)
key_container.append(call_key2)
key_container.append(call_key3)
key_container.append('0')

curr_key = key_container.pop(0)
    # / Key Setting

    # MariaDB connection
conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )
    # / MariaDB connection
# / Initiating


#for i in sidos:
    #place = 'select BJD_code,BJD_sido,BJD_sigungu,BJD_dong,BJD_ri from insert_BJD where BJD_sido like ' + i
place = 'select BJD_code from insert_BJD'
cur = conn.cursor()
cur.execute(place)
totplc = cur.fetchall()

leng = len(totplc)

for j in range(2400,2460):
    plc = totplc[j]

    BJD_code = str(plc[0])
    print("현재 지역코드 : " + BJD_code + " / index : " + str(j))

    # curr_plc = plc[0]+" "+plc[1]+" "+plc[2]+" "+plc[3]+" "+plc[4] + '  index = '+ str(j)
    # print(curr_plc)

    res = grab_data(BJD_code)

    # print(len(res))
    # print(len(res[0]))
    # program_exit()

    # init except catch
    if res == 1 :
        # break
        continue

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
        mdb_insert_one(res)
        print('one insert complete')
    else :
        mdb_insert_bulk(res)
        print('bulk insert complete')

    print(str(totalcnt) + " insert complete")

    # break
#print(i + ' full inserted')

cur.close()
conn.close()
