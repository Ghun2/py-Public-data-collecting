import urllib.request
import json
import xmltodict
import pymysql
import pprint
import re
import sys
import time

sil_config = {'연립다세대' : 'getRTMSDataSvcRHRent',
                '단독다가구' : 'n',
                '아파트' : 'getRTMSDataSvcAptRent'}

tablename = ''

# mySQL connection
conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )
cur = conn.cursor()
# / mySQL connection

def dbcon():
    q = 'SELECT distinct substr(new_BJD.BJD_code,1,5) FROM new_BJD'
    cur.execute(q)
    totcd = cur.fetchall()
    return totcd

curr_key = ''

def rest_call(code,year):

    global curr_key

    url = 'http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/' + tablename + '?'
    params = 'LAWD_CD='+ code +'&DEAL_YMD='+ year +'&serviceKey='+ curr_key


    request = urllib.request.Request(url+params)

    request.get_method = lambda: 'GET'

    while 1:
        try:
            response_body = urllib.request.urlopen(request, timeout=30).read()
        except timeout:
            #print('timeout! repeat')
            continue
        except :
            continue
        else:
            #print('restcall successful.')
            break

    return response_body

def rest_call_2(code,year):

    global curr_key

    url = 'http://openapi.nsdi.go.kr/nsdi/LandCharacteristicsService/attr/getLandCharacteristics?'
    params = 'pnu=1111010100100010000&numOfRows=10&authkey=10c32e9e3a4e2cceca2554'
    #params = 'pnu='+ code +'&DEAL_YMD='+ year +'&authkey=10c32e9e3a4e2cceca2554'


    request = urllib.request.Request(url+params)

    request.get_method = lambda: 'GET'

    while 1:
        try:
            response_body = urllib.request.urlopen(request, timeout=30).read()
        except timeout:
            #print('timeout! repeat')
            continue
        except :
            continue
        else:
            #print('restcall successful.')
            break

    return response_body

    # pnu=1111010100100010000&stdrYear=2017&format=xml&numOfRows=10&pageNo=1
    # url = "http://openapi.nsdi.go.kr/nsdi/LandCharacteristicsService/attr/getLandCharacteristics"
    # queryParams = '?' + urlencode({ quote_plus('authkey') : '인증키'
    #                            , quote_plus('pnu') : '1111010100100010000' /* 고유번호(8자리 이상) */
    #                            , quote_plus('stdrYear') : '2017' /* 기준년도(YYYY: 4자리) */
    #                            , quote_plus('format') : 'xml' /* 응답결과 형식(xml 또는 json) */
    #                            , quote_plus('numOfRows') : '10' /* 검색건수 */
    #                            , quote_plus('pageNo') : '1' /* 페이지 번호 */
    #                             })



def grab_data(code,year,selected) :       #REST API

    global curr_key
    global tablename

    tablename = sil_config[selected]

    response_body = rest_call(code,year)

    tempstr = str(response_body)

    response_body = json.loads(json.dumps(xmltodict.parse(response_body)))

    if tempstr[3] != '?' :

        errtxt = ''

        rest_err_code = response_body['OpenAPI_ServiceResponse']['cmmMsgHeader']['returnReasonCode']
        #print(rest_err_code)
        if rest_err_code == '22' :
            curr_key = key_container.pop(0)
            errtxt = code+' change key , spare key : '+ str(len(key_container))
            err_log(errtxt)
            print(errtxt)

            if curr_key == '0' :
                errtxt = code+' 남은 키가 없습니다.'
                print(errtxt)

                #로그파일에 쓰기 , db 등 닫고 프로그램 종료
                err_log(errtxt)
                program_exit()

            response_body = rest_call(code,year)
            response_body = json.loads(json.dumps(xmltodict.parse(response_body)))
        else :
            errtxt = in_sgg+in_bjd+' REST_ERROR_number : '+rest_err_code
            print(errtxt)

            #로그파일에 쓰기 , db 등 닫고 프로그램 종료
            err_log(errtxt)
            program_exit()

    if(response_body['response']['body']['totalCount'] == '0') :
        return 0
    else :
        return response_body['response']['body']['items']['item']


def indata_01(container_list):  #연립다세대

    before_dong = ''
    container_list = sorted(container_list, key=lambda k: k['법정동'])


    for i in range(0,len(container_list)):

        bunji_str = ''
        deposit_str = '0'
        price_str = '0'

        sg_code = container_list[i]['지역코드']
        dong_str = container_list[i]['법정동']

        if '지번' in container_list[i]:
            bunji_str = container_list[i]['지번']
        if '보증금액' in container_list[i]:
            deposit_str = container_list[i]['보증금액']
        if '월세금액' in container_list[i]:
            price_str = container_list[i]['월세금액']


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

        if dong_str == before_dong:
            # print("위와 같음")
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc
        else :
            if dong_str.find(' ') != -1:
                # print("리 존재")
                before_dong = dong_str
                dri = dong_str.split()
                dong_ = dri[0]
                ri_ = dri[1]
                dres = dbcheck_2(sg_code,dong_,ri_)
                if len(dres) == 0 and dong_ == '신령면' :
                    dres = dbcheck_2(sg_code,'신녕면',ri_)
                if len(dres) == 0 and dong_ == '홍북읍' :
                    dres = dbcheck_2(sg_code,'홍북면',ri_)
                if len(dres) == 0 and dong_str == '진량읍 평사리' :
                    dres = dbcheck_2(sg_code,'진량읍','평사리(平沙)')
                if len(dres) == 0 and dong_ == '유가읍' :
                    dres = dbcheck_2(sg_code,'유가면',ri_)
                if len(dres) == 0 and dong_ == '청량면' :
                    dres = dbcheck_2(sg_code,'청량읍',ri_)
                # print(dres)
                # print(sg_code + ' ' + dong_ + ' ' + ri_)
                try:
                    new_code = int(str(dres[0][0])[5:])
                except :
                    print(dres, sg_code,dong_str)
                    program_exit()
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3] + ' ' + dres[0][4]
            else :
                before_dong = dong_str
                dres = dbcheck_1(sg_code,dong_str)
                if len(dres) == 0 :
                    dres = dbcheck_1(sg_code,dong_str[:-1])
                # print(sg_code + ' ' + dong_str)
                # print(dres)
                # print(len(dres))
                # print(container_list[i])
                try:
                    new_code = int(str(dres[0][0])[5:])
                except :
                    print(dres, sg_code,dong_str)
                    program_exit()
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3]
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc


        container_list[i]['번'] = bun
        container_list[i]['지'] = ji
        container_list[i]['보증금액'] = int(deposit_str.replace(",", ""))
        container_list[i]['월세금액'] = int(price_str.replace(",", ""))
        # print(container_list)
        # print(len(container_list[i]))
    print(container_list[0]['주소']+" 데이터 수정/결합 완료")
    #데이터 수정 완료 / 데이터베이스 삽입 코드 아래부터
    # print(container_list)
    fuck_data(container_list,16)
    print(str(len(container_list))+"개 데이터 삽입 완료")


def indata_02(container_list):  #아파트

    before_dong = ''
    container_list = sorted(container_list, key=lambda k: k['법정동'])


    for i in range(0,len(container_list)):

        bunji_str = ''
        deposit_str = '0'
        price_str = '0'

        sg_code = container_list[i]['지역코드']
        dong_str = container_list[i]['법정동']

        if '지번' in container_list[i]:
            bunji_str = container_list[i]['지번']
        if '보증금액' in container_list[i]:
            deposit_str = container_list[i]['보증금액']
        if '월세금액' in container_list[i]:
            price_str = container_list[i]['월세금액']


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

        if dong_str == before_dong:
            # print("위와 같음")
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc
        else :
            if dong_str.find(' ') != -1:
                # print("리 존재")
                before_dong = dong_str
                dri = dong_str.split()
                dong_ = dri[0]
                ri_ = dri[1]
                dres = dbcheck_2(sg_code,dong_,ri_)
                if len(dres) == 0 and dong_ == '신령면' :
                    dres = dbcheck_2(sg_code,'신녕면',ri_)
                if len(dres) == 0 and dong_ == '홍북읍' :
                    dres = dbcheck_2(sg_code,'홍북면',ri_)
                if len(dres) == 0 and dong_str == '진량읍 평사리' :
                    dres = dbcheck_2(sg_code,'진량읍','평사리(平沙)')
                if len(dres) == 0 and dong_ == '유가읍' :
                    dres = dbcheck_2(sg_code,'유가면',ri_)
                if len(dres) == 0 and dong_ == '청량면' :
                    dres = dbcheck_2(sg_code,'청량읍',ri_)
                # print(dres,sg_code,dong_,ri_)
                # print(sg_code + ' ' + dong_ + ' ' + ri_)
                # try:
                #     new_code = dres[0][0][5:]
                # except :
                #     print('index = ' + str(i) + ' / ')
                #     print(dres)
                #     program_exit()
                try:
                    new_code = int(str(dres[0][0])[5:])
                except :
                    print(dres, sg_code,dong_str)
                    program_exit()
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3] + ' ' + dres[0][4]
            else :
                before_dong = dong_str
                dres = dbcheck_1(sg_code,dong_str)
                if len(dres) == 0 :
                    dres = dbcheck_1(sg_code,dong_str[:-1])
                # print(sg_code + ' ' + dong_str)
                # print(dres)
                # print(len(dres))
                # print(container_list[i])
                try:
                    new_code = int(str(dres[0][0])[5:])
                except :
                    print(dres, sg_code,dong_str)
                    program_exit()
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3]
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc


        container_list[i]['번'] = bun
        container_list[i]['지'] = ji
        container_list[i]['보증금액'] = int(deposit_str.replace(",", ""))
        container_list[i]['월세금액'] = int(price_str.replace(",", ""))
        # print(container_list)
        # print(len(container_list[i]))
    print(container_list[0]['주소']+" 데이터 수정/결합 완료")
    #데이터 수정 완료 / 데이터베이스 삽입 코드 아래부터
    # print(container_list)
    fuck_data(container_list,16)
    print(str(len(container_list))+"개 데이터 삽입 완료")



def indata_03(container_list):  #단독 다가구

    before_dong = ''
    container_list = sorted(container_list, key=lambda k: k['법정동'])

    for i in range(0,len(container_list)):

        bunji_str = ''
        price_str = '0'

        sg_code = container_list[i]['지역코드']
        dong_str = container_list[i]['법정동']

        # if '지번' in container_list[i]:
        #     bunji_str = container_list[i]['지번']

        if '거래금액' in container_list[i]:
            price_str = container_list[i]['거래금액']

        # if len(bunji_str.split('-')) == 2:
        #     bunji = bunji_str.split('-')
        #     bun = bunji[0]
        #     ji = bunji[1]
        # elif len(bunji_str.split('-')) == 1:
        #     bun = bunji_str
        #     ji = ''
        # else :
        #     bun = ''
        #     ji = ''

        if dong_str == before_dong:
            # print("위와 같음")
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc
        else :
            if dong_str.find(' ') != -1:
                # print("리 존재")
                before_dong = dong_str
                dri = dong_str.split()
                dong_ = dri[0]
                ri_ = dri[1]
                dres = dbcheck_2(sg_code,dong_,ri_)
                if len(dres) == 0 and dong_ == '신령면' :
                    dres = dbcheck_2(sg_code,'신녕면',ri_)
                if len(dres) == 0 and dong_str == '진량읍 평사리' :
                    dres = dbcheck_2(sg_code,'진량읍','평사리(平沙)')
                if len(dres) == 0 and dong_ == '홍북읍' :
                    dres = dbcheck_2(sg_code,'홍북면',ri_)
                if len(dres) == 0 and dong_ == '유가읍' :
                    dres = dbcheck_2(sg_code,'유가면',ri_)
                if len(dres) == 0 and dong_ == '청량면' :
                    dres = dbcheck_2(sg_code,'청량읍',ri_)
                # print(dres)
                # print(sg_code + ' ' + dong_ + ' ' + ri_)
                new_code = dres[0][0][5:]
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3] + ' ' + dres[0][4]
            else :
                before_dong = dong_str
                dres = dbcheck_1(sg_code,dong_str)
                if len(dres) == 0 :
                    dres = dbcheck_1(sg_code,dong_str[:-1])
                # print(sg_code + ' ' + dong_str)
                # print(dres)
                # print(len(dres))
                # print(container_list[i])
                new_code = dres[0][0][5:]
                new_plc = dres[0][1] + ' ' + dres[0][2] + ' ' + dres[0][3]
            container_list[i]['법정동코드'] = new_code
            container_list[i]['주소'] = new_plc


        # 지번 찾기 (건축물 대장 표제부 데이터 불러오기 / 시군구,법정동,대지면적,연면적 -> )

        container_list[i]['번'] = bun
        container_list[i]['지'] = ji
        container_list[i]['거래금액'] = int(price_str.replace(",", ""))
        #print(container_list[i])
        # print(len(container_list[i]))
    print(container_list[0]['주소']+" 데이터 수정/결합 완료")
    #데이터 수정 완료 / 데이터베이스 삽입 코드 아래부터
    fuck_data(container_list,16)
    print(str(len(container_list))+"개 데이터 삽입 완료")


def dbcheck_1(code,d):
    #qur = 'SELECT * FROM new_BJD where BJD_code like ' + '""' + code
    if code == '36110':
        cur.execute("SELECT * FROM new_BJD WHERE BJD_code LIKE %s and BJD_sigungu = %s limit 1", ("%" + code + "%",d))
    else :
        cur.execute("SELECT * FROM new_BJD WHERE BJD_code LIKE %s and BJD_dong = %s limit 1", ("%" + code + "%",d))
    #cur.execute(qur)
    dres = cur.fetchall()
    return dres

def dbcheck_2(code,d,r):
    #qur = 'SELECT * FROM new_BJD where BJD_code like ' + '""' + code
    if code == '36110':
        cur.execute("SELECT * FROM new_BJD WHERE BJD_code LIKE %s and BJD_sigungu = %s and BJD_dong = %s limit 1", ("%" + code + "%",d,r))
    else :
        cur.execute("SELECT * FROM new_BJD WHERE BJD_code LIKE %s and BJD_dong = %s and BJD_ri = %s limit 1", ("%" + code + "%",d,r))
    #cur.execute(qur)
    dres = cur.fetchall()
    return dres

def fuck_data(lst,cnt):
    # each dict length check
    reslen = len(lst)
    exeptcnt = []

    # print(len(lst[0]))
    # program_exit()

    for o in range(0,reslen):
        if len(lst[o]) != cnt:
            exeptcnt.append(o)

    for u in range(len(exeptcnt)-1,-1,-1):
        exeptres = lst.pop(exeptcnt[u])
        fuck_data_one(exeptres)

    vallist = []
    for b in range(0,len(lst)):
        vallist.append(tuple(lst[b].values()))


    # vallist = tuple(lst[0].values())
    # print(vallist)
    # print(lst)
    keylist = list(lst[0].keys())
    keystr = ""
    for value in range(0,len(keylist)-1):
        keystr = keystr + keylist[value] + ","
    keystr = keystr+keylist[len(keylist)-1]

    # program_exit()

    var_string = '%s,' * (len(lst[0])-1) + '%s'

    #var_string = ', '.join('%s' * len(lst))
    sql = 'INSERT INTO ' + tablename + ' (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.executemany(sql,vallist)
    conn.commit()

def fuck_data_one(lst):

    vallist = tuple(lst.values())
    keylist = list(lst.keys())
    keystr = ""
    for value in range(0,len(keylist)-1):
        keystr = keystr + keylist[value] + ","
    keystr = keystr+keylist[len(keylist)-1]

    # program_exit()

    var_string = '%s,' * (len(lst)-1) + '%s'

    #var_string = ', '.join('%s' * len(lst))
    sql = 'INSERT INTO ' + tablename + ' (%s) VALUES (%s);' % (keystr,var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.execute(sql,vallist)
    conn.commit()

def err_log(txt):   # 에러로그 쓰기
    curr_time = time.asctime(time.localtime(time.time()))   #현재 시간
    f = open("insert_log.txt",'a')   #로그 file open
    f.write(curr_time + ' ' + txt + '\n')
    f.close()

def program_exit(): # 프로그램 종료
    cur.close()     # MariaDB 커서 close
    conn.close()    # MariaDB 연결 close

    sys.exit()      #프로그램 종료
