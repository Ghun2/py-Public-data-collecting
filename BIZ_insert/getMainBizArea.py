import pymysql
import sys
import urllib.request
import json
import xmltodict
import re




def rest_call(BJD_code):

    url = 'http://api.vworld.kr/req/data?'
    params = 'service=data&version=2.0&request=getfeature&key=096DA1B4-9D1B-3127-B1BA-D763B3507A6B&format=xml&errorformat=xml&size=100&page=1&data=LT_C_DGMAINBIZ&attrfilter=emdCd:=:'+BJD_code+'&attribute=true&crs=epsg:4326&domain=api.vworld.kr'



    request = urllib.request.Request(url+params)

    request.get_method = lambda: 'GET'
    excnt = 0
    while 1:

        try:
            response_body = urllib.request.urlopen(request, timeout=30).read()
        except :
            if excnt == 10:
                print('error')
                program_exit()
            print('repeat')
            excnt += 1
            continue
        else:
            print('restcall successful.')
            break

    return response_body

def grab_data(BJD_code) :       #REST API

    response_body = rest_call(BJD_code)

    response_body = json.loads(json.dumps(xmltodict.parse(response_body)))

    print(response_body['response']['status'])

    if response_body['response']['status'] == 'OK':
        restemp = response_body['response']['result']['wfs:FeatureCollection']['gml:featureMember']
        if len(restemp) == 1:
            poly = restemp['LT_C_DGMAINBIZ']['ag_geom']['gml:MultiPolygon']['gml:polygonMember']['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']['#text']
            fname = restemp['LT_C_DGMAINBIZ']['fullnm']
            poly = shimpyo(poly)
            cd = BJD_code
            sgg = BJD_code[:5]
            bjd = BJD_code[5:]+'00'
            data = (cd,sgg,bjd,fname,poly)
            db_insert(data)
        else:
            for j in range(len(restemp)):
                poly = restemp[j]['LT_C_DGMAINBIZ']['ag_geom']['gml:MultiPolygon']['gml:polygonMember']['gml:Polygon']['gml:exterior']['gml:LinearRing']['gml:posList']['#text']
                fname = restemp[j]['LT_C_DGMAINBIZ']['fullnm']
                poly = shimpyo(poly)
                cd = BJD_code
                sgg = BJD_code[:5]
                bjd = BJD_code[5:]+'00'
                data = (cd,sgg,bjd,fname,poly)
                db_insert(data)

def db_insert(data):
    sql = 'INSERT INTO getMainBizArea VALUES (%s,%s,%s,%s,%s);'# % (data[0],data[1],data[2],data[3],data[4],data[5],data[6],data[7])
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()


def shimpyo(geo):
    result_string = ''
    split_geo = geo.split()

    # for i in range(0,len(split_geo)):
    #     if i != 0 and i%2 == 0:
    #         result_string = result_string + '), (' + split_geo[i]
    #     elif i == 0:
    #         result_string = result_string + split_geo[i]
    #     elif i == len(split_geo)-1:
    #         result_string = result_string + ',' + split_geo[i] + '))'
    #     else:
    #         result_string = result_string + ',' + split_geo[i]
    for i in range(0,len(split_geo)):
        if i == 0:
            result_string = split_geo[i]
        else:
            result_string = result_string + ',' + split_geo[i]

    return result_string


def program_exit(): # 프로그램 종료
    cur.close()     # MariaDB 커서 close
    conn.close()    # MariaDB 연결 close

    sys.exit()      #프로그램 종료




conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )
place = 'SELECT distinct left(BJD_code,8),BJD_sido,BJD_sigungu,BJD_dong FROM insert_BJD;'
cur = conn.cursor()
cur.execute(place)
totplc = cur.fetchall()

leng = len(totplc)



for i in range(leng):
    print(totplc[i][0],totplc[i][1],totplc[i][2],totplc[i][3],i)
    grab_data(totplc[i][0])
