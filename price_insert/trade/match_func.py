import urllib.request
import json
import xmltodict
import pymysql
import pprint
import re
import sys
import time


# f = open("sangup_log.txt",'a')   #로그 file open
f = open("toji_log.txt",'a')   #로그 file open

    #init DB
conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )
# cur = conn.cursor()


def fetch_data(day):
    sql = 'select * from sang_up_price where 계약년월 = ' + str(day) + ' order by 시군구'
    curs = conn.cursor()
    curs.execute(sql)
    return curs.fetchall()

def fetch_data_toji(day):
    sql = 'select * from TOJI where YM = ' + str(day) + ' order by SIGUNGU'
    curs = conn.cursor()
    curs.execute(sql)
    return curs.fetchall()

def code_match(plc):
    sql = 'select code from tc where name = "' + str(plc) + '"'
    curs = conn.cursor()
    curs.execute(sql)
    result_cd = curs.fetchall()
    if len(result_cd) == 0:
        return 0
    else :
        return result_cd[0][0]

def bunji_match(code,item):
    scode = code[:5]
    bcode = code[5:]
    bunjicode = item[2][0]
    area = item[7]
    useyear = item[14]

    if item[1] == '일반':
        sql = 'SELECT distinct bun,ji FROM getBrTitleInfo '
        sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and (bun like "'+bunjicode+'%" or bun like "0'+bunjicode+'%" or bun like "00'+bunjicode+'%" or bun like "000'+bunjicode+'%") and useAprDay like "'+useyear+'%"'
        sql = sql + 'order by abs(totArea - '+str(area)+') limit 3'

        curs = conn.cursor()
        # print(sql)      #
        # program_exit()  #
        curs.execute(sql)
        result_match = curs.fetchall()
    else :
        sql = 'SELECT distinct bun,ji FROM getBrExposPubuseAreaInfo '
        sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and (bun like "'+bunjicode+'%" or bun like "0'+bunjicode+'%" or bun like "00'+bunjicode+'%" or bun like "000'+bunjicode+'%")'
        sql = sql + 'order by abs(area - '+str(area)+') limit 3'

        curs = conn.cursor()
        # print(sql)      #
        # program_exit()  #
        curs.execute(sql)
        result_match = curs.fetchall()

        # for i in range(0,len(result_match)):
        #     title_scode = result_match[i][1]
        #     title_bcode = result_match[i][2]
        #     title_bun = result_match[i][3]
        #     title_ji = result_match[i][4]
        #
        #     sql = 'SELECT distinct platPlc,sigunguCd,bjdongCd,bun,ji,totArea,useAprDay FROM getBrTitleInfo '
        #     sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and bun = '+title_bun+' and ji = '+title_ji+' and useAprDay = '+useyear+' order by abs(totArea - '+str(area)+') limit 3'


    return result_match

def bunji_match_toji(code,item):
    item[7] = item[7].rstrip(' \r')
    item[7] = item[7].rstrip('\r')
    # print(item)
    if item[7] == '지분':
        return 1
    scode = code[:5]
    bcode = code[5:]
    zimock = item[1]
    yongdo = item[2]
    area = item[5]
    #
    # if item[1] == '일반':
    sql = 'SELECT distinct bun,ji FROM getLandCharacteristics '
    sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and lndcgrCodeNm = \"'+zimock+'\"'

    if yongdo == '자연환경보전':
        sql = sql +' and prposArea1Nm = \"자연환경보전지역\"'
    elif yongdo == '일반주거지역' :
        sql = sql +' and (prposArea1Nm = \"제1종일반주거지역\" or prposArea1Nm = \"제2종일반주거지역\" or prposArea1Nm = \"제3종일반주거지역\")'
    elif yongdo == '전용주거지역' :
        sql = sql +' and (prposArea1Nm = \"제1종전용주거지역\" or prposArea1Nm = \"제2종전용주거지역\")'
    elif yongdo == '기타':
        pass
    else :
        sql = sql +' and prposArea1Nm = \"' +yongdo+'\"'

    sql = sql + ' order by abs(lndpclAr - '+str(area)+') limit 3'


    curs = conn.cursor()
    curs.execute(sql)
    result_match = curs.fetchall()
    # else :
    #     sql = 'SELECT distinct bun,ji FROM getBrExposPubuseAreaInfo '
    #     sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and (bun like "'+bunjicode+'%" or bun like "0'+bunjicode+'%" or bun like "00'+bunjicode+'%" or bun like "000'+bunjicode+'%")'
    #     sql = sql + 'order by abs(area - '+str(area)+') limit 3'
    #
    #     curs = conn.cursor()
    #     curs.execute(sql)
    #     result_match = curs.fetchall()

        # for i in range(0,len(result_match)):
        #     title_scode = result_match[i][1]
        #     title_bcode = result_match[i][2]
        #     title_bun = result_match[i][3]
        #     title_ji = result_match[i][4]
        #
        #     sql = 'SELECT distinct platPlc,sigunguCd,bjdongCd,bun,ji,totArea,useAprDay FROM getBrTitleInfo '
        #     sql = sql + 'where sigunguCd = '+scode+' and bjdongCd = '+bcode+' and bun = '+title_bun+' and ji = '+title_ji+' and useAprDay = '+useyear+' order by abs(totArea - '+str(area)+') limit 3'


    return result_match



def fuck_data(lst):

    #keystr = '시군구, 유형, 지번, 도로명, 용도지역, 건축물주용도, 도로조건, 전용/연면적(m2), 대지면적(m2), 거래금액(만원), 층, 계약년월, 계약일, 지분구분, 건축년도, sigunguCd, bjdongCd, bun_1, ji_1, bun_2, ji_2, bun_3, ji_3'

    var_string = '%s,' * 23 + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 5) + ('%s,' * 3) + ('%d' * 2) + ('%s,' * 5) + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 2) + ('%s,' * 13) + '%s'
    #var_string = ', '.join('%s' * len(lst))
    #sql = 'INSERT INTO getCommerceBusiness (%s) VALUES (%s);' % (keystr,var_string)
    sql = 'INSERT INTO getCommerceBusiness VALUES (%s);' % (var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.executemany(sql,lst)
    conn.commit()

def func_data_one(lst):
    #keystr = '시군구, 유형, 지번, 도로명, 용도지역, 건축물주용도, 도로조건, 전용/연면적(m2), 대지면적(m2), 거래금액(만원), 층, 계약년월, 계약일, 지분구분, 건축년도, sigunguCd, bjdongCd, bun_1, ji_1, bun_2, ji_2, bun_3, ji_3'

    var_string = '%s,' * 23 + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 5) + ('%s,' * 3) + ('%d' * 2) + ('%s,' * 5) + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 2) + ('%s,' * 13) + '%s'
    #var_string = ', '.join('%s' * len(lst))
    #sql = 'INSERT INTO getCommerceBusiness (%s) VALUES (%s);' % (keystr,var_string)
    sql = 'INSERT INTO getCommerceBusiness VALUES (%s);' % (var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.execute(sql,lst)
    conn.commit()

def func_data_one_toji(lst):
    #keystr = '시군구, 유형, 지번, 도로명, 용도지역, 건축물주용도, 도로조건, 전용/연면적(m2), 대지면적(m2), 거래금액(만원), 층, 계약년월, 계약일, 지분구분, 건축년도, sigunguCd, bjdongCd, bun_1, ji_1, bun_2, ji_2, bun_3, ji_3'

    var_string = '%s,' * 15 + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 5) + ('%s,' * 3) + ('%d' * 2) + ('%s,' * 5) + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 2) + ('%s,' * 13) + '%s'
    #var_string = ', '.join('%s' * len(lst))
    #sql = 'INSERT INTO getCommerceBusiness (%s) VALUES (%s);' % (keystr,var_string)
    sql = 'INSERT INTO getLandTrade VALUES (%s);' % (var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.execute(sql,lst)
    conn.commit()

def strip_one(s):
    if s.endswith(" ") : s = s[:-1] #마지막이 " "임을 검사
    if s.startswith(" ") : s = s[1:] #첫번째가 " "임을 검사
    return s


def write_log(txt):   # 에러로그 쓰기
    curr_time = time.asctime(time.localtime(time.time()))   #현재 시간
    f.write(curr_time + ' ' + txt + '\n')


def program_exit(): # 프로그램 종료
    # cur.close()     # MariaDB 커서 close
    conn.close()    # MariaDB 연결 close
    f.close()
    sys.exit()      #프로그램 종료
