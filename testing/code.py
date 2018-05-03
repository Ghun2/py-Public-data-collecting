import sys
import pymysql

conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )
cur = conn.cursor()


def exit(): # 프로그램 종료
    cur.close()     # MariaDB 커서 close
    conn.close()    # MariaDB 연결 close

    sys.exit()      #프로그램 종료

def dbcon():
    q = 'SELECT code, name from tc;'# where name like "%세종특별자치시%";'
    cur.execute(q)
    totcd = cur.fetchall()
    return totcd

def exept():
    q = 'SELECT BJD_sigungu from raiz.insert_BJD where BJD_sigungu like "% %" group by BJD_sigungu;'
    cur = conn.cursor()
    cur.execute(q)
    totcd = cur.fetchall()
    return totcd


def indata(data):
    sql = 'INSERT INTO new_BJD VALUES (%s,%s,%s,%s,%s);'
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()




def cngdb():
    sql = 'select code from raiz.new_BJD where code IN ('
    sql += 'select b.code from '
    sql += '(SELECT left(code,8) as sub, dong FROM raiz.new_BJD where ri != "" group by dong) as a,'
    sql += 'new_BJD as b '
    sql += 'where b.code = concat(a.sub,"00") );'
    cur.execute(sql)
    totcd = cur.fetchall()
    return totcd




# cng = cngdb()
#
#
# for value in cng:
#     q = 'DELETE from new_BJD where code = '+str(value[0])
#     cur.execute(q)
#     conn.commit()
#
# exit()

res = dbcon()
exp = exept()




for i in range(0,len(res)):
    arr = ['','','','','']
    spt = []
    for val in exp :
        if res[i][1].find(val[0]) != -1 :
            spt = res[i][1].split()
            spt[1] += ' ' + spt.pop(2)
            break
            # exit()

    if not spt:
        spt = res[i][1].split()

    # print(spt)

    if len(spt) <= 2 and spt[0] != '세종특별자치시':
        # print(spt)
        continue

    cnt = 1
    arr[0] = str(res[i][0])
    for v in spt :
        arr[cnt] = v
        cnt += 1

    print(arr)
    indata(arr)
