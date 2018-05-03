import pymysql
import json
from pprint import pprint

with open('SIGUNGU.json') as data_file:
    data = json.load(data_file)

conn = pymysql.connect(
    host='14.63.182.14', user='raiz', passwd='raiz3338', db='raiz', charset='utf8'
    )

for i in range(0,len(data['features'])):
    insert_arr = []

    coord = json.dumps(data['features'][i]['geometry']['coordinates'][0]) #coord
    CD = data['features'][i]['properties']['SIG_CD']    #int
    ENG_NM = data['features'][i]['properties']['SIG_ENG_NM']
    KOR_NM = data['features'][i]['properties']['SIG_KOR_NM']

    insert_arr.append(coord)
    insert_arr.append(CD)
    insert_arr.append(ENG_NM)
    insert_arr.append(KOR_NM)

    var_string = '%s,%s,%s,%s'
    #var_string = ('%s,' * 7) + ('%d,' * 5) + ('%s,' * 3) + ('%d' * 2) + ('%s,' * 5) + '%s'
    #var_string = ('%s,' * 7) + ('%d,' * 2) + ('%s,' * 13) + '%s'
    #var_string = ', '.join('%s' * len(lst))
    #sql = 'INSERT INTO getCommerceBusiness (%s) VALUES (%s);' % (keystr,var_string)
    sql = 'INSERT INTO gisSIGUNGU VALUES (%s);' % (var_string)
    # print(sql)
    #sql = "insert into getLegaldongAptList values %r;" % (tuple(lst))

    curs = conn.cursor()
    curs.execute(sql,insert_arr)
    conn.commit()
