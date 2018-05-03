# -*- coding: utf-8 -*-
import os
import pyproj
import shapefile
import pymysql
import sys

# global initialize
conn = pymysql.connect(
    host='', user='', passwd='', db='', charset='utf8'
    )

def exit():
    conn.close()
    sys.exit()

def indb(data,name):    # Txt
    snum = len(data)
    s = '%s,'*(snum-1)
    sql = 'INSERT INTO '+name+' VALUES ('+s+'%s);'
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()

def indb2(data,name):    # Geo
    ingeo = 'GeomFromText(%s)'
    snum = len(data)
    s = '%s,'*(snum-1)
    sql = 'INSERT INTO '+name+' VALUES ('+ s + ingeo+');'
    curs = conn.cursor()
    curs.execute(sql,data)
    conn.commit()

def fileList(path_dir):
    file_list = os.listdir(path_dir)
    return file_list

def readSHP(path_dir,value):
    filename_shp = path_dir+'/'+value+'/'+value+'.shp'
    filename_dbf = path_dir+'/'+value+'/'+value+'.dbf'

    myshp = open(filename_shp,"rb")
    mydbf = open(filename_dbf,"rb")
    sf = shapefile.Reader(shp=myshp,dbf=mydbf)

    return sf.shapeRecords()

def proj4instc(arg,ch=0):
    #projparam = pyproj.Proj(init='epsg:5179')
    if ch != 0:
        projparam = pyproj.Proj(arg)
    else:
        projparam = pyproj.Proj(init=arg)
    return projparam

def arrangeGeom(data,type,proj=0):
    if type == 5:     # polygon
        return polygon(data,proj)
    elif type == 3:     # polyline
        return polyline(data,proj)
    elif type == 1:       # point
        return point(data,proj)
    else:
        print(type,'unknown type error')
        exit()

def polygon(tmppoly,proj):
    # 폴리곤 작업 - 좌표변환 , 문자열 포맷팅(geo/txt)
    polystr = 'polygon(('
    stpoint = tmppoly[0]
    stindex = 0
    for i in range(len(tmppoly)):
        if proj != 0:
            lat,lon  = proj(tmppoly[i][0], tmppoly[i][1], inverse=True)
            polystr += str(lon) + ' ' + str(lat)
        else:
            polystr += str(tmppoly[i][1]) + ' ' + str(tmppoly[i][0])
        if i != 0 and stpoint == tmppoly[i] and i != stindex and i != len(tmppoly)-1:
            polystr += '), ('
            stpoint = tmppoly[i+1]
            stindex = i+1
        elif i == len(tmppoly)-1:
            polystr += '))'
        else:
            polystr += ', '

    return polystr

def polyline(tmppoly,proj):
    polystr = 'LINESTRING('
    for i in range(len(tmppoly)):
        if proj != 0:
            lat,lon  = proj(tmppoly[i][0], tmppoly[i][1], inverse=True)
        else:
            lat,lon  = tmppoly[i][1],tmppoly[i][0]
        if i == len(tmppoly)-1:
            polystr += str(lon) + ' ' + str(lat)
        else:
            polystr += str(lon) + ' ' + str(lat) + ','
    polystr += ')'

    return polystr

def point(tmppoly,proj):
    for i in range(len(tmppoly)):
        pointstr = 'POINT('
        if proj != 0:
            lat,lon  = proj(tmppoly[i][0], tmppoly[i][1], inverse=True)
        else:
            lat,lon  = tmppoly[i][1],tmppoly[i][0]
        pointstr += str(lon) + ' ' + str(lat) + ')'

    return pointstr


def testGeom(poly,proj):
    for i in range(len(poly)):
        lat,lon  = proj(poly[i][0], poly[i][1], inverse=True)
        polystr = str(lon) + ',' + str(lat)
        txt = 'new daum.maps.LatLng('+polystr+'),'
        print(txt)
    # exit()
