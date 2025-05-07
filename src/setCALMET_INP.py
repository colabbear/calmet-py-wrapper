"""
2025/03/21
작성자: colabbear
1.  setCALMET_INP 함수의 output_path 인자를 키워드 인자로 변경
2.  NSSTA 설정하도록 수정



2025/03/27
작성자: colabbear
1.  static 폴더에 넣어주는 임의의 CALMET.INP 파일에 대해서도 잘 작동하도록 하기 위해
    기존 set_INPUT_GROUP_0 등등 함수도 아무 CALMET.INP 파일에도 대응할 수 있도록
    파일에 어떤 설정값이 들어가 있었든 찾아서 수정하게 변경함
2.  set_INPUT_GROUP_8 함수 추가 (상층 지점 위치 정보를 기입함)
3.  Length of run (hours) 값인 IRLG을 기입 할 때 마지막날 까지 포함되어야 하므로 1을 더하도록 수정함
4.  ! NSSTA =.*! 가 아니라 !NSSTA=.*! 와 같은 형식으로 파일에 작성되어 있는 경우에는 대응이 안 되어 있는 잠재적인 문제가 있음
5.  다음의 설정값으로 바꾸게 하는 기능 추가해야 함 그 설정값 목록은 다음과 같음
    BIAS (설정한 층 수에 따라 다르고 각 층마다 -1 ~ 1 의 값), TERRAD은 2km, RMAX1은 30km, RMAX2은 30km, R1은 10km, R2은 50km
6.  필요한 경로들을 키워드 인자로 받도록 setCALMET_INP 함수 변경



"""

import os
from datetime import datetime, timedelta
import pandas as pd
import pyproj
import re



def set_INPUT_GROUP_0(content, GEO_DAT_path="", SRF_DAT_path="", UP_DAT_path="", CALMET_LST_path="", CALMET_DAT_path=""):
    if len(GEO_DAT_path) * len(SRF_DAT_path) * len(UP_DAT_path) * len(CALMET_LST_path) * len(CALMET_DAT_path) == 0:
        print("Please set the path")
        return 0

    content = re.sub(r"! GEODAT =.*!", f"! GEODAT = {GEO_DAT_path} !", content)
    content = re.sub(r"! SRFDAT =.*!", f"! SRFDAT = {SRF_DAT_path} !", content)
    content = re.sub(r"! METLST =.*!", f"! METLST = {CALMET_LST_path} !", content)
    content = re.sub(r"! METDAT =.*!", f"! METDAT = {CALMET_DAT_path} !", content)
    content = re.sub(r"! UPDAT=.*!", f"! UPDAT={UP_DAT_path}!    !END!", content)

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

    content = re.sub(r"! IBYR =.*!", f"! IBYR = {startYYYY} !", content)
    content = re.sub(r"! IBMO =.*!", f"! IBMO = {startMM} !", content)
    content = re.sub(r"! IBDY =.*!", f"! IBDY = {startDD} !", content)
    content = re.sub(r"! IBHR =.*!", f"! IBHR = {startHH} !", content)
    content = re.sub(r"! IBTZ =.*!", f"! IBTZ = {-9} !", content)
    content = re.sub(r"! IRLG =.*!", f"! IRLG = {int(IRLG.total_seconds() // 3600) + 1} !", content)

    return content



# INPUT GROUP 4의 NSSTA도 함께 설정함
def set_INPUT_GROUP_7(content, SRF_DAT_path="", startDt=""):
    if len(SRF_DAT_path) == 0:
        print("Please set the path")

    target = """-------------------------------------------------------------------------------

INPUT GROUP: 7 -- Surface meteorological station parameters
--------------

     SURFACE STATION VARIABLES
     (One record per station --  12  records in all)


             1     2
         Name     ID     X coord.   Y coord.    Time   Anem.
                          (km)       (km)       zone   Ht.(m)
       ----------------------------------------------------------\n"""

    # 기존 파일에 기입되어 있는 지점 정보 삭제하기
    # "! SS1  =" 은 고정적임
    # 상층지점도 SS 대신 US로 고정적임 (CALMET.FOR Version: 5.8.5 Level: 151214의 20957~20959번째 줄을 참고함)
    content = re.sub(r"! SS1  =.*!", "", content)


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
    content = re.sub(
        r"! NSSTA =.*!",
        f"! NSSTA = {len(stations)} !",
        content
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
            10.
        )

        if idx < len(stations)-1:
            row_stnInfo += "\n"

    content = content.replace(
        target, target + row_stnInfo
    )

    return content



# 반드시 상층 파일 이름이 UP_지점번호.DAT 형식이어야 함
def set_INPUT_GROUP_8(content, UP_DAT_path="", startDt=""):
    target = """-------------------------------------------------------------------------------

INPUT GROUP: 8 -- Upper air meteorological station parameters
--------------

     UPPER AIR STATION VARIABLES
     (One record per station --  3  records in all)

             1     2
         Name    ID      X coord.   Y coord.  Time zone
                           (km)       (km)
        -----------------------------------------------\n"""

    rowTemp = "! US1  ='{:s}'  {:>6d}  {:>8.3f}   {:>8.3f}       {:>d} !"

    # 기존 파일에 기입되어 있는 지점 정보 삭제하기
    # "! SS1  =" 은 고정적임
    # 상층지점도 SS 대신 US로 고정적임 (CALMET.FOR Version: 5.8.5 Level: 151214의 20957~20959번째 줄을 참고함)
    content = re.sub(r"! US1  =.*!", "", content)

    # 관측지점정보 불러오기
    df_sonde = pd.read_csv("./static/sonde_META_관측지점정보_20250327125531.csv", encoding="cp949")
    stnInfo_tot = df_sonde

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

    idx = 0
    # group(1) 이 첫번째 괄호를 의미한다고 함
    stnID = int(re.search(r'UP_(.*?)\.DAT', UP_DAT_path).group(1))
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
    stnName = "U" + str(idx + 1)

    # UTM 좌표 얻기 m 단위로 얻으므로 km로 변환 필요
    x_coord, y_coord = transformer.transform(df.loc[condition, "경도"].values[0], df.loc[condition, "위도"].values[0])

    row_stnInfo += rowTemp.format(
        stnName,
        stnID,
        x_coord / 1000,
        y_coord / 1000,
        0,
    )

    row_stnInfo += "\n"

    content = content.replace(
        target, target + row_stnInfo
    )

    return content



def setCALMET_INP(
        output_path="./myCALMET.INP",
        input_CALMET_INP_path="",
        GEO_DAT_path="",
        SRF_DAT_path="",
        UP_DAT_path="",
        CALMET_LST_path="",
        CALMET_DAT_path="",
        startDt="",
        endDt=""
):
    if len(input_CALMET_INP_path) == 0:
        print("Please set input CALMET.INP file path")
        return 0
    if len(GEO_DAT_path) * len(SRF_DAT_path) * len(UP_DAT_path) * len(CALMET_LST_path) * len(CALMET_DAT_path) == 0:
        print("Please set the file path")
        return 0
    if len(startDt) * len(endDt) == 0:
        print("Please set the date")
        return 0

    with open(input_CALMET_INP_path, 'r') as f:
        content = f.read()
        content = set_INPUT_GROUP_0(
            content,
            GEO_DAT_path=GEO_DAT_path,
            SRF_DAT_path=SRF_DAT_path,
            UP_DAT_path=UP_DAT_path,
            CALMET_LST_path=CALMET_LST_path,
            CALMET_DAT_path=CALMET_DAT_path
        )
        content = set_INPUT_GROUP_1(content, startDt=startDt, endDt=endDt)
        content = set_INPUT_GROUP_7(content, SRF_DAT_path=SRF_DAT_path, startDt=startDt)
        content = set_INPUT_GROUP_8(content, UP_DAT_path=UP_DAT_path, startDt=startDt)

    with open(output_path, 'w', encoding="ascii") as f:
        f.write(content)



if __name__ == "__main__":
    setCALMET_INP("./input_file/myCALMET.INP", startDt="202403071200", endDt="202403091200")

