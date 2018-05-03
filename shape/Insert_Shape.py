# -*- coding: utf-8 -*-

# 토지(특성정보) GIS 폴리곤 데이터 삽입

import os
import pyproj
import shapefile
import pymysql
import sys

def exit():
    sys.exit()

def indb1(data):    # Txt
    #ingeo = 'GeomFromText(%s)'
    sql = 'INSERT INTO getLandPolygonText VALUES (%s,%s,%s,%s,%s,%s);'
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()

# def indb2(data):    # Geo
#     ingeo = 'GeomFromText(%s)'
#     sql = 'INSERT INTO getLandPolygonGeometry VALUES (%s,%s,%s,%s,%s,'+ingeo+');'
#     curs = conn.cursor()
#     curs.execute(sql,data)
#     conn.commit()

conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )

path_dir = './shp/insert_file'
file_list = os.listdir(path_dir)
file_list.sort()


projparam = pyproj.Proj("+proj=tmerc +lat_0=38.002781 +lon_0=127.000788 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs")

for value in file_list:
    filename_shp = path_dir+'/'+value+'/'+value+'.shp'
    filename_dbf = path_dir+'/'+value+'/'+value+'.dbf'

    print(value)

    myshp = open(filename_shp,"rb")
    mydbf = open(filename_dbf,"rb")
    sf = shapefile.Reader(shp=myshp,dbf=mydbf)
    shapes = sf.shapes()
    # print(len(shapes))
    # print(sf.shapeType)

    shapeRecs = sf.shapeRecords()

    for i in range(len(shapeRecs)):
        # pnu , sigunguCd , bjdongCd , bun , ji , polygon(geo,text)

        # print(shapeRecs[42].shape.points)
        # print(shapeRecs[41].shape.points)

        in_arr = ['','','','','','']

        in_arr[0] = shapeRecs[i].record[1]
        in_arr[1] = shapeRecs[i].record[2][:5]
        in_arr[2] = shapeRecs[i].record[2][5:]
        tmpsplit = shapeRecs[i].record[6]
        if tmpsplit.find('-') != -1:
            tmpsplit = tmpsplit.split('-')
            in_arr[3] = tmpsplit[0]
            in_arr[4] = tmpsplit[1]
        else:
            in_arr[3] = tmpsplit

        print(in_arr,i)
        tmppoly = shapeRecs[i].shape.points
        polystr = 'polygon(('
        polytxt = '(('
        stpoint = tmppoly[0]
        stindex = 0
        # 폴리곤 작업 - 좌표변환 , 문자열 포맷팅(geo/txt)
        for i in range(len(tmppoly)):
            lat,lon  = projparam(tmppoly[i][0], tmppoly[i][1], inverse=True)
            polystr += str(lon) + ' ' + str(lat)
            polytxt += str(lon) + ', ' + str(lat)
            if i != 0 and stpoint == tmppoly[i] and i != stindex and i != len(tmppoly)-1:
                polystr += '), ('
                polytxt += ')), (('
                stpoint = tmppoly[i+1]
                stindex = i+1
            elif i == len(tmppoly)-1:
                polystr += '))'
                polytxt += '))'
            else:
                polystr += ', '
                polytxt += '), ('
        in_arr[5] = polystr
        indb1(in_arr)
        #in_arr[5] = polytxt
        # indb2(in_arr)

    print('insert '+str(len(shapeRecs)))
