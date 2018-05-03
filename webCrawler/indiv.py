import pandas as pd
import dateutil
import urllib
import requests
import random
import sys
import pymysql
from bs4 import BeautifulSoup

proxy_list = pd.read_table('proxy.txt',sep='\t', header=-1)
header_list = pd.read_table('header.txt',sep='\n', header=-1)


conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )

def exit(): # 프로그램 종료
    conn.close()    # MariaDB 연결 close
    sys.exit()      #프로그램 종료

def indb(data):    # Txt

    sql = 'INSERT INTO auction_mm VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    curs = conn.cursor()
    curs.executemany(sql,data)
    conn.commit()

def reset_proxy():
    rand_samp = random.sample(range(len(proxy_list)),1)
    rand_ip = proxy_list.ix[rand_samp][0]
    rand_port = proxy_list.ix[rand_samp][1]

    proxy_target = (str(*rand_ip) + ':' + str(*rand_port))

    return proxy_target

def reset_header():
    rand_samp = random.sample(range(len(header_list)),1)

    rand_header = header_list.ix[rand_samp][0]


    header_target = str(*rand_header)

    return header_target

def get_page(url):

    headers = {'User-Agent' : reset_header()}
    proxy_dict = {'http' : reset_proxy()}

    print(headers,proxy_dict)

    result = requests.get(url,headers=headers)#,proxies=proxy_dict)

    if result.status_code == 200:
        navi = BeautifulSoup(result.text,'html.parser',from_encoding='utf-8')
        table = navi.find('table', class_="table0202")

        print(table)
        exit()
        records = []

        for i,r in enumerate(table.find_all('tr')):
            chk = 1
            list_records = ['','','','','','','','','','']
            for j,c in enumerate(r.find_all('td')):
                print(c)
                exit()
                if j == 1:
                    try :
                        list_records[9] = c.find('img').get('src')
                    except :
                        pass
                elif j == 2:    #용도 사건번호 법원명
                        list_records[1] = str(c.find_all('p')[0].text.strip())
                        list_records[2] = str(c.find_all('p')[1].text.strip().split('\r')[0])
                        list_records[3] = str(c.find('br').text.strip())
                elif j == 3:    #소재지 면적 특이사항
                    try:
                        list_records[4] = str(c.find_all('p')[0].text.strip()).replace('[','').replace(']','')
                    except :
                        chk = 0
                        break
                    list_records[0] = str(c.text.split('[')[0].strip())

                    list_records[5] = str(c.find_all('p')[1].text.strip())
                elif j == 4:    #감정가 최저가 최저가율
                    list_records[6] = str(c.find_all('p')[0].text.strip()).replace(str(c.find_all('p')[1].text.strip()),"").replace(',','')
                    list_records[7] = str(c.find_all('p')[1].text.strip()).replace(str(c.find_all('p')[2].text.strip()),"").replace(',','')
                    list_records[8] = str(c.find_all('p')[2].text.strip()).replace('(','').replace(')','')
            if chk == 1:
                records.append(list_records)
        return records
    else :
        print(str(result.status_code) + 'error')
        return None



def fetch_addrdata():
    sql = 'SELECT distinct pnu from raiz.getLandPolygonText where sigunguCd = 41465;'
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    return data







base_url = ''
pnu = fetch_addrdata()

for i in range(0,len(pnu)):
    print(str(pnu[9][0]))
    # exit()
    records = get_page(base_url)#+str(4146510100100590010))
    exit()
    records.pop(0)
    records.pop()

    if len(records) == 0 or records is None or i == 3:
        print(str(i)+'page END!')
        exit()

    # for value in records:
    #     print(value)

    # db insert
    # indb(records)
    #   ...

    # if i == 3:
    #     exit()
# print(result.content[1000:2000])
# req = rq.Request(url)
# req.add_header('User-agent','Mozila/5.0 (compatible; MSIE 5.5; Windows NT)')
#
# data = rq.urlopen(req)
# text = data.read().decode('utf-8')
# print(text[:300])
