#   총괄 표제부 삽입
from xml.etree.ElementTree import parse
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

def mdb_insert_one(res,bjdcd):
    #res = add_column(res,bjdcd)
    vallist = tuple(res.values())
    keylist = list(res.keys())
    keystr = ""
    for value in range(0,len(keylist)-1):
        keystr = keystr + keylist[value] + ","
    keystr = keystr+keylist[len(keylist)-1]

    # program_exit()

    var_string = '%s,' * (len(res)-1) + '%s'

    #var_string = ', '.join('%s' * len(res))
    sql = 'INSERT INTO getLandCharacteristics (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(res))

    curs = conn.cursor()
    curs.execute(sql,vallist)
    conn.commit()



def mdb_insert_bulk(res,bjdcd):

    # each dict length check
    reslen = len(res)
    exeptcnt = []
    for o in range(0,reslen):
        if len(res[o]) != 30:
            exeptcnt.append(o)
        # add column
        # else :
        #     res[o] = add_column(res[o],bjdcd)

    for u in range(len(exeptcnt)-1,-1,-1):
        exeptres = res.pop(exeptcnt[u])
        mdb_insert_one(exeptres,bjdcd)

    if len(res) == 0 :
        return 0

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
    sql = 'INSERT INTO getLandCharacteristics (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(res))

    curs = conn.cursor()
    curs.executemany(sql,vallist)
    conn.commit()


def db_insert(data):
    sql = 'INSERT INTO getLandPolygonText VALUES (%s,%s,%s,%s,%s,%s,%s,%s);'# % (data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7])
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()

def db_insert2(data):
    if data[5] != '':
        data[5] = 'polygon('+data[5]+')'
        ex_q = 'GeomFromText(%s)'
    else:
        ex_q = '%s'
    if data[6] != '':
        data[6] = 'polygon('+data[6]+')'
        in_q = 'GeomFromText(%s)'
    else :
        in_q = '%s'
    sql = 'INSERT INTO getLandPolygonGeometry VALUES (%s,%s,%s,%s,%s,'+ ex_q +','+in_q+',%s);'# % (data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7])
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()

def adjustData(res,bjd):

    sggCod = bjd[:5]
    bjdCod = bjd[5:]

    for i in range(0,len(res)):

        res[i]['sigunguCd'] = sggCod
        res[i]['bjdongCd'] = bjdCod

        bunji_str = res[i]['mnnmSlno']

        if len(bunji_str.split('-')) == 2:
            bunji = bunji_str.split('-')
            bun = bunji[0]
            ji = bunji[1]
        elif len(bunji_str.split('-')) == 1:
            bun = bunji_str
            ji = ''
        else :
            bun = ''
            ji = ''

        res[i]['bun'] = bun
        res[i]['ji'] = ji

    return res

def adjustData_one(res,bjd):

    sggCod = bjd[:5]
    bjdCod = bjd[5:]

    res['sigunguCd'] = sggCod
    res['bjdongCd'] = bjdCod

    bunji_str = res['mnnmSlno']

    if len(bunji_str.split('-')) == 2:
        bunji = bunji_str.split('-')
        bun = bunji[0]
        ji = bunji[1]
    elif len(bunji_str.split('-')) == 1:
        bun = bunji_str
        ji = ''
    else :
        bun = ''
        ji = ''

    res['bun'] = bun
    res['ji'] = ji

    return res

def call_timer() :
    print("time out .. stop")
    program_exit()

def rest_call(pnu):

    global curr_key
    #pnu = 1111010300100010002
    #print(pnu)
    url = 'http://openapi.nsdi.go.kr/nsdi/LandCharacteristicsService/wfs/getLandCharacteristicsWFS?'

    params = 'pnu=' + str(pnu) + '&maxFeatures=1&resultType=results&srsName=EPSG:5181&authkey='+ curr_key

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
    #tree = parse(response_body)
    #print(type(tree))
    #print(response_body['wfs:FeatureCollection']['gml:featureMember']['NSDI:F251']['NSDI:SHAPE'])
    return response_body


def grab_data(pnu) :       #REST API

    global curr_key

    response_body = rest_call(pnu)

    #response_body = response_body.find("NSDI:SHAPE")
    print(response_body['wfs:FeatureCollection']['gml:featureMember']['NSDI:F251']['NSDI:SHAPE'].keys())
    #response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
    program_exit()
    #print(response_body['wfs:FeatureCollection']['gml:featureMember']['NSDI:F251']['NSDI:SHAPE']['gml:Polygon']['gml:interior'][1])
    # print(str(response_body['wfs:FeatureCollection'].keys()).find('gml:featureMember'))
    if str(response_body['wfs:FeatureCollection'].keys()).find('gml:featureMember') == -1:
        # print('hello')
        return 0

    return response_body['wfs:FeatureCollection']['gml:featureMember']['NSDI:F251']['NSDI:SHAPE']

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

def pnu_group(code):
    sgg = code[:5]
    bjd = code[5:]

    q = 'select pnu,sigunguCd,bjdongCd,bun,ji from getLandCharacteristics where sigunguCd = '+sgg+' and bjdongCd = '+bjd+' group by bun,ji'
    cur = conn.cursor()
    cur.execute(q)
    pnu_arr = cur.fetchall()

    return pnu_arr

def arrange_data(pnu,geo):

    # print(pnu)
    # print(len(geo['gml:Polygon']['gml:exterior']))
    # print(geo['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList'].split())

    #res = shimpyo(geo['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList'])
    pnu = list(pnu)
    extem = ''
    intem = ''
    if len(geo['gml:Polygon']['gml:exterior']) > 1:
        # print('hello')

        for i in range(0,len(geo['gml:Polygon']['gml:exterior'])):
            tt = '('+str(shimpyo(geo['gml:Polygon']['gml:exterior'][i]['gml:LinearRing']['gml:posList']))+')'
            if i == 0:
                extem = tt
            else:
                extem = extem + ',' + tt
        #print(extem)
    else :
        extem = '('+str(shimpyo(geo['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']))+')'

    if str(geo['gml:Polygon'].keys()).find('gml:interior') != -1:
        if len(geo['gml:Polygon']['gml:interior']) > 1:
            # print('hello')

            for i in range(0,len(geo['gml:Polygon']['gml:interior'])):
                tt = '('+str(shimpyo(geo['gml:Polygon']['gml:interior'][i]['gml:LinearRing']['gml:posList']))+')'
                if i == 0:
                    intem = tt
                else:
                    intem = intem + ',' + tt
            #print(intem)
        else :
            intem = '('+str(shimpyo(geo['gml:Polygon']['gml:interior']['gml:LinearRing']['gml:posList']))+')'

    pnu.append(extem)
    pnu.append(intem)
    pnu.append(geo['gml:Polygon']['@srsDimension'])

    # print(len(pnu))
    # print(pnu)

    return pnu

def shimpyo(geo):
    result_string = ''
    split_geo = geo.split()

    for i in range(0,len(split_geo)):
        if i != 0 and i%2 == 0:
            result_string = result_string + ', ' + split_geo[i]
        elif i == 0:
            result_string = split_geo[i]
        else:
            result_string = result_string + ' ' + split_geo[i]
    #print(result_string)
    return result_string
    # Key Setting


call_key00 = ''


key_container = []
# key_container.append(call_key0)
# # key_container.append(call_key1)
# # key_container.append(call_key2)
# # key_container.append(call_key3)
key_container.append(call_key00)

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

for j in range(160,7000):   #index 287 divide 7 이제 8하면됨
    plc = totplc[j]
    BJD_code = str(plc[0])
    print("현재 지역코드 : " + BJD_code + " / index : " + str(j))
    # program_exit()
    # curr_plc = plc[0]+" "+plc[1]+" "+plc[2]+" "+plc[3]+" "+plc[4] + '  index = '+ str(j)
    # print(curr_plc)

    pnu_arr = pnu_group(BJD_code)
    for h in range(709,len(pnu_arr)):
        print("Processing : " + BJD_code + " / " + str(j) + " / " + str(h))
        res = grab_data(pnu_arr[h][0])
        if res == 0 :
            continue
        final_data = arrange_data(pnu_arr[h],res)
        db_insert(final_data)
        db_insert2(final_data)

        #print("Processing : " + BJD_code + " / " + str(j) + " / " + str(h))
        # program_exit()
        # res = json.loads(json.dumps(xmltodict.parse(res)))
        #print(res)
        # print(len(res[0]))
        # program_exit()

        # init except catch
        # if res == 1 :
        #     # break
        #     continue

        # if(res == 0) :
        #     print("0 insert complete")
        #     continue

        # if(isinstance(res, dict)) :
        #     totalcnt = 1
        #
        # else :
        #     totalcnt = len(res)


        # if totalcnt == 0 :
        #     print(str(totalcnt) + " insert complete")
        #     continue
        # elif totalcnt == 1 :
        #     res = adjustData_one(res,BJD_code)
        #     mdb_insert_one(res,BJD_code)
        #     print('one insert complete')
        # else :
        #     res = adjustData(res,BJD_code)
        #     mdb_insert_bulk(res,BJD_code)
        #     print('bulk insert complete')

        #print(str(totalcnt) + " insert complete")

    # program_exit()

    # break
#print(i + ' full inserted')

cur.close()
conn.close()
