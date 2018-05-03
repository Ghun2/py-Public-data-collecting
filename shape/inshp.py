# -*- coding: utf-8 -*-

# 사용 가이드 -  1. /shp/insert_file 경로에 올바른 데이터 저장
#               2. proj4 좌표계 확인
#               3. fieldnum 수정 (테이블 필드수 확인)
#               4. in_arr 추가 혹은 삭제 할부분 및 한글 decode 확인
#               5. 마지막으로 테이블 이름 수정

import md
from math import floor
# epsg:5181 골목상권

path_dir = './shp/insert_file'

flist = md.fileList(path_dir)

data = md.readSHP(path_dir,flist[0])
#proj4prameter = '+proj=tmerc +lat_0=38 +lon_0=127.5 +k=0.9996 +x_0=1000000 +y_0=2000000 +ellps=GRS80 +units=m +no_defs'
proj4prameter = '+proj=tmerc +lat_0=38 +lon_0=127 +k=1 +x_0=200000 +y_0=500000 +ellps=GRS80 +units=m +no_defs'
proj = md.proj4instc(proj4prameter,1)

for i in range(len(data)):
    fieldnum = 6
    in_arr = ['']*fieldnum

    # md.exit()

    in_arr[0] = data[i].record[0]
    if data[i].record[1] != '':
        try:
            in_arr[1] = data[i].record[1].decode('EUC-KR')
        except:
            in_arr[1] = data[i].record[1]
    # in_arr[1] = data[i].record[1]
    # in_arr[2] = data[i].record[2]
    # # if data[i].record[3] != '':
    # #     in_arr[3] = data[i].record[3].decode('EUC-KR')
    # in_arr[3] = data[i].record[3]
    if data[i].record[4] is not None:
        in_arr[2] = data[i].record[4]
    if data[i].record[5] is not None:
        in_arr[3] = data[i].record[5]
    if data[i].record[6] is not None :
        in_arr[4] = floor(data[i].record[6])
    # in_arr[7] = data[i].record[7]
    # in_arr[8] = data[i].record[8]
    # in_arr[9] = data[i].record[9]
    # in_arr[10] = data[i].record[10]
    # in_arr[11] = data[i].record[11]

    # print(in_arr,i)
    # md.exit()
    tmppoly = data[i].shape.points

    # print(tmppoly[0])
    # print(data[i].shape.shapeType)
    # md.exit()
    md.testGeom(tmppoly,proj)

    if len(tmppoly) == 0:
        continue
    in_arr[fieldnum-1] = md.arrangeGeom(tmppoly,data[i].shape.shapeType,proj)

    print(i,in_arr)
    # md.exit()
    # md.indb(in_arr,'AggregZone2')
    md.indb2(in_arr,'youdong_seoul_locationCd')
