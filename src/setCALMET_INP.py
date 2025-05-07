"""
2025/03/21
작성자: colabbear
1. setCALMET_INP 함수의 output_path 인자를 키워드 인자로 변경
2. NSSTA 설정하도록 수정

"""

import os
from datetime import datetime, timedelta
import pandas as pd
import pyproj



def set_INPUT_GROUP_0(content, GEO_DAT_path="", SRF_DAT_path="", UP_DAT_path="", CALMET_LST_path="", CALMET_DAT_path=""):
    if len(GEO_DAT_path) * len(SRF_DAT_path) * len(UP_DAT_path) * len(CALMET_LST_path) * len(CALMET_DAT_path) == 0:
        print("Please set the path")
        return 0

    content = content.replace(
        "! GEODAT = GEO.DAT !", "! GEODAT = " + GEO_DAT_path+" !"
    ).replace(
        "! SRFDAT = SURF.DAT !", "! SRFDAT = " + SRF_DAT_path+" !"
    ).replace(
        "! METLST = CALMET.LST !", "! METLST = " + CALMET_LST_path+" !"
    ).replace(
        "! METDAT = CALMET.DAT !", "! METDAT = " + CALMET_DAT_path+" !"
    ).replace(
        "! UPDAT=UP_47138.DAT!", "! UPDAT=" + UP_DAT_path+"!"
    )

    return content



def set_INPUT_GROUP_1(content, startDt="", endDt=""):
    if len(startDt) * len(endDt) == 0:
        print("Please set the date")
        return 0

    startDt = datetime.strptime(startDt, '%Y%m%d%H%M')
    endDt = datetime.strptime(endDt, '%Y%m%d%H%M')

    startYYYY = startDt.year
    startMM = startDt.month
    startDD = startDt.day
    startHH = startDt.hour

    IRLG = endDt - startDt

    content = content.replace(
        "! IBYR = 2024 !", "! IBYR = {:04d} !".format(startYYYY)
    ).replace(
        "! IBMO = 3 !", "! IBMO = {:d} !".format(startMM)
    ).replace(
        "! IBDY = 2 !", "! IBDY = {:d} !".format(startDD)
    ).replace(
        "! IBHR = 0 !", "! IBHR = {:d} !".format(startHH)
    ).replace(
        "! IBTZ = -9 !", "! IBTZ = {:d} !".format(-9)
    ).replace(
        "! IRLG = 24 !", "! IRLG = {:d} !".format(int(IRLG.total_seconds() // 3600))
    )

    return content



# INPUT GROUP 4의 NSSTA도 함께 설정함
def set_INPUT_GROUP_7(content, SRF_DAT_path="", startDt=""):
    if len(SRF_DAT_path) == 0:
        print("Please set the path")

    target_nnsta = "Number of surface stations   (NSSTA)  No default     ! NSSTA = 9 !"

    target = """-------------------------------------------------------------------------------

INPUT GROUP: 7 -- Surface meteorological station parameters
--------------

     SURFACE STATION VARIABLES
     (One record per station --  12  records in all)


             1     2
         Name     ID     X coord.   Y coord.    Time   Anem.
                          (km)       (km)       zone   Ht.(m)
       ----------------------------------------------------------\n"""
    rowTemp = "! SS1  ='{:s}'  {:>6d}  {:>8.3f}   {:>8.3f}       {:>d}    {:>8.3f}  !"

    # surf.dat으로부터 지상관측지점 번호 얻기
    stations = []
    station_total_num = 0
    with open(SRF_DAT_path, 'r') as f:
        line_count = 0
        for line in f:
            line_count += 1
            if line_count == 5:
                station_total_num = int(line.strip().split()[-1])
                break

        if station_total_num == 0:
            print("Surface station number is 0")
            return 0

        station_count = 0
        for line in f:
            station_count += 1
            stations.append(int(line))
            if station_count == station_total_num:
                break
    # NNSTA 설정
    content = content.replace(
        target_nnsta,
        "Number of surface stations   (NSSTA)  No default     ! NSSTA = {:d} !".format(len(stations))
    )

    # 울산광역시 관측지점정보 불러오기
    df_asos = pd.read_csv("./static/asos_META_관측지점정보_20250204112738.csv", encoding="utf-8")
    df_aws = pd.read_csv("./static/aws_META_관측지점정보_20250204112439.csv", encoding="utf-8")
    df_awos = pd.read_csv("./static/울산광역시_공공기관_관측지점정보.csv", encoding="utf-8")

    stnInfo_tot = pd.concat([df_asos, df_aws, df_awos], axis=0, ignore_index=True)
    # 지점번호를 index로 설정
    stnInfo_tot = stnInfo_tot.set_index("지점")
    # 시작일, 종료일 datetime 형식으로 변경
    stnInfo_tot['시작일'] = pd.to_datetime(stnInfo_tot['시작일'])
    stnInfo_tot['종료일'] = pd.to_datetime(stnInfo_tot['종료일'])

    row_stnInfo = ""
    startDt = datetime.strptime(startDt, '%Y%m%d%H%M')

    wgs84 = pyproj.CRS("EPSG:4326")  # WGS84 (위도, 경도)
    utm = pyproj.CRS("EPSG:32652")  # UTM Zone 52N
    # 위도, 경도를 UTM 좌표로 변환
    transformer = pyproj.Transformer.from_crs(wgs84, utm, always_xy=True)

    for idx, stnID in enumerate(stations):
        # 반환 결과가 하나라도 DataFrame 형태로 반환하도록 stnInfo_tot.loc[[stnID]] 와 같이 참조
        df = stnInfo_tot.loc[[stnID]]
        condition = (
                        (df["시작일"] <= startDt) & (df["종료일"] > startDt)
                    ) | (
                        (df["시작일"] <= startDt) & df["종료일"].isna()
                    ) | (
                        (df["시작일"].isna()) & (df["종료일"].isna())
                    )

        # 지점명 임의 할당
        stnName = "S" + str(idx+1)

        # UTM 좌표 얻기 m 단위로 얻으므로 km로 변환 필요
        x_coord, y_coord = transformer.transform(df.loc[condition, "경도"].values[0], df.loc[condition, "위도"].values[0])

        row_stnInfo += rowTemp.format(
            stnName,
            stnID,
            x_coord/1000,
            y_coord/1000,
            -9,
            df.loc[condition, "노장해발고도(m)"].values[0]
        )

        if idx < len(stations)-1:
            row_stnInfo += "\n"

    content = content.replace(
        target, target + row_stnInfo
    )

    return content



def setCALMET_INP(output_path="./myCALMET.INP", startDt="", endDt=""):
    if len(startDt) * len(endDt) == 0:
        print("Please set the date")
        return 0

    with open("./static/CALMET.INP", 'r') as f:
        content = f.read()
        content = set_INPUT_GROUP_0(
            content,
            GEO_DAT_path="./static/GEO.DAT",
            SRF_DAT_path="./input_file/surf.dat",
            UP_DAT_path="./input_file/UP_47138.DAT",
            CALMET_LST_path="./output_file/CALMET.LST",
            CALMET_DAT_path="./output_file/CALMET.DAT"
        )
        content = set_INPUT_GROUP_1(content, startDt=startDt, endDt=endDt)
        content = set_INPUT_GROUP_7(content, SRF_DAT_path="./input_file/surf.dat", startDt=startDt)

    with open(output_path, 'w', encoding="ascii") as f:
        f.write(content)

if __name__ == "__main__":
    setCALMET_INP("./input_file/myCALMET.INP", startDt="202403071200", endDt="202403091200")

