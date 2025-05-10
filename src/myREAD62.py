"""
2025/03/21
작성자: colabbear
1.  write_up_dat 함수의 output_path 인자를 키워드 인자로 변경



2025/03/27
작성자: colabbear
1.  한 지점에 대한 여러 csv 파일을 읽을 때 중복을 제거하여 datafrmae을 합치는 과정에서 문제가 있음를 확인함
    이 문제를 해결하기 위해 일시 칼럼을 index로 바꾸는 과정을 모든 csv파일을 읽은 후로 변경하고
    (일시(UTC), 기압(hPa)) 두 칼럼이 같은 경우를 중복으로 처리하고 제거하도록 함으로써 해결하고자 함
2.  칼럼별로 dtype 변경을 하고 NaN 처리된 빈칸에 대해 결측치를 넣는 것을
    반대로 NaN 처리된 빈칸에 9999. 결측값을 넣고 형변환을 진행하도록 변경



2025/03/28
작성자: colabbear
1.  write_up_dat 함수에서 " 6201 ..." 로 출력되던 것을 " 9999 ..." 로 출력되도록 변경
    원본 데이터의 format이 non-NCDC data 인 경우 9999라고 함
    CALMET_UsersGuide.pdf 내용 중 READ56/READ62 Output File Format (Upn.DAT) 부분을 참고하였음
2.  맨 밑 등압면 관측값들 중에서 하나라도 결측 처리된 값이 있는 경우 모델이 돌아가다가 다음과 같은 오류를 발생시킴을 확인함
    ERROR IN SUBR. VERTAV -- cell face height not found in height array
    원래 맨 밑 등압면과 맨 위 등압면 관측값들 중에서 하나라도 결측 처리된 값이 있는 경우 오류가 발생하게 해야 하지만
    myREAD62.py 코드를 작성할 때 생략하였음
    이 생략한 부분을 구현할 필요가 있음



"""

import os
import pandas as pd
from datetime import datetime, timedelta

header = """UP.DAT          2.0             Header structure with coordinate parameters                     
   1
Produced by myREAD62 Version: 5.54  Level: 070627                                 
NONE    """

# df_tot 은 {지점1 : df, ...} 임
df_tot = {}

# 기상자료개방포털에서 모든 칼럼을 선택하여 다운받은 레윈존데 파일인 경우를 가정함
def read_SONDE(source_folder):
    global df_tot
    tmin = 175.
    tmax = 322.
    pmin = 0.0
    pmax = 1040.

    # 소스 폴더 내에 있는 모든 csv 파일 찾아서 read
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")

                df = pd.read_csv(csv_file_path, encoding="cp949")

                # datetime 형식으로 변경
                df["일시(UTC)"] = pd.to_datetime(df["일시(UTC)"])

                # 칼럼의 특정 행 값이 빈칸인 경우 결측 처리
                # 결측 처리 값은 READ62 소스코드 방식에 따름 (READ62.FOR v5.54 1021번째 줄부터 참고)
                df.loc[df["고도(gpm)"].isna(), "고도(gpm)"] = 9999.
                df.loc[df["기온(°C)"].isna(), "기온(°C)"] = 999.9
                df.loc[df["풍향(deg)"].isna(), "풍향(deg)"] = 999.
                df.loc[df["풍속(knot)"].isna(), "풍속(knot)"] = 999.9

                # 칼럼별로 dtype 적절하게 변경
                df = df.astype({
                    "기압(hPa)": "float64",
                    "풍향(deg)": "int64"
                })
                # 기온 및 최저운고 단위 각각 K, ft로 변경하고 칼럼이름에도 반영
                df.loc[df["기온(°C)"] < 999.9, "기온(°C)"] += 273.15
                df.loc[df["풍속(knot)"] < 999.9, "풍속(knot)"] *= 0.514791
                # 칼럼 이름 변경
                df = df.rename(columns={"기온(°C)": "기온(K)", "풍속(knot)": "풍속(m/s)"})

                for stnID in df["지점"].unique():
                    # 같은 지점 다른 데이터가 있는 경우 같은 인덱스는 덮어쓰는 형태로 합치기
                    if stnID in df_tot:
                        # 수직으로 합치기
                        df_tot[stnID] = pd.concat([df_tot[stnID], df], axis=0)
                        # 중복 인덱스(일자) 제거
                        df_tot[stnID] = df_tot[stnID][~df_tot[stnID].duplicated(subset=["일시(UTC)", "기압(hPa)"], keep='last')]
                    else:
                        df_tot[stnID] = df.loc[df["지점"] == stnID]

    # 모든 지점 df에 대해 일시(UTC) 칼럼을 index로 변경
    for stnID in df_tot:
        # 일시 칼럼을 인덱스로 설정
        # 파일 내에 지점이 여러개일 때 인덱스에 중복이 발생하지만 지점별로 저장하기에 문제 없을 것임
        df_tot[stnID] = df_tot[stnID].set_index("일시(UTC)")



def write_up_dat(output_path="./UP.DAT", pstop=500.0, startDt="", endDt="", sonde_path=""):
    # Check that first level is at the ground
    # Check that the pressure is decreasing with height
    # Check that the elevation is increasing with height
    # Check for missing height values
    # Check range of non-missing wind directions
    # Check range of non-missing wind speeds
    # Check range of non-missing temperatures
    # Check range of non-missing pressures (missing value = -99.9)
    # Check for short sounding
    # Check for missing data at top of sounding -> 가장 상층 데이터 어느 하나라도 결측(9999. or 999 or 999.9)이면 안 됨
    # Check for missing data at bottom of sounding -> 가장 하층 데이터 어느 하나라도 결측(9999. or 999 or 999.9)이면 안 됨
    # 마지막 두개만 체크하고 다른 것들은 모두 만족한다고 가정 (기상청 자료이므로)
    global df_tot
    global header
    iform = 1 # lash delimiter
    jdat = 3 # kma csv format 문제될 시 2로 변경하기
    LHT = "F" # Drop Sounding level if no height
    LTEMP = "F" # Drop Sounding level if no temperature
    LWD = "F" # Drop Sounding level if no direction
    LWS = "F" # Drop Sounding level if no speed



    if len(sonde_path) == 0:
        print("Please set the path that there is data")
        return 0
    if len(startDt) * len(endDt) == 0:
        print("Please set the date(YYYYMMDDHHmm) that you want to set")
        return 0

    read_SONDE(sonde_path)



    startDt = datetime.strptime(startDt, '%Y%m%d%H%M')
    endDt = datetime.strptime(endDt, '%Y%m%d%H%M')

    currentDt = startDt
    time_series = []
    while currentDt <= endDt:
        time_series.append(currentDt)
        currentDt += timedelta(hours=1)  # 1시간 간격

    startYYYY = startDt.year
    startJJJ = startDt.timetuple().tm_yday
    startHH = startDt.hour
    endYYYY = endDt.year
    endJJJ = endDt.timetuple().tm_yday
    endHH = endDt.hour

    # Header without location data
    header2 = " {:5d}{:5d}{:5d}{:5d}{:5d}{:5d}{:4.0f}.{:5d}{:5d}".format(startYYYY, startJJJ, startHH, endYYYY, endJJJ, endHH, pstop, jdat, iform)
    header3 = "     {:1s}    {:1s}    {:1s}    {:1s}".format(LHT, LTEMP, LWD, LWS)


    idx = output_path.rfind('.')
    # fortran 의 formatted 출력의 인코딩이 ascii로 알려져 있음
    for stnID in df_tot:
        with open(output_path[:idx] + "_" + str(stnID) + output_path[idx:], "w", encoding="ascii") as f:
            f.write(header + "\n")
            f.write(header2 + "\n")
            f.write(header3 + "\n")

            for t in time_series:
                if t in df_tot[stnID].index:
                    YYYY = t.year
                    MM = t.month
                    DD = t.day
                    HH = t.hour
                    # NUMLEV:               NUMBER OF REPEATING GROUPS -- THIS REPRESENTS
                    #                       THE NUMBER OF DATA LEVELS FOUND IN THE CURRENT
                    #                       OBSERVATION (79 IS THE MAXIMUM NUMBER STORED)
                    mlev = len(df_tot[stnID].at[t, "기압(hPa)"])

                    # istop 할당 READ62.for v5.54 1119번째 줄 참고함
                    istop = 0
                    # 기압 list 역순으로 얻기
                    pres = df_tot[stnID].at[t, "기압(hPa)"].tolist()[::-1]
                    for idx, i in enumerate(pres):
                        if i <= pstop:
                            istop = idx + 1
                            break
                    if istop == 0:
                        print("error: istop = 0")
                        return
                    elif istop == 1: # READ62.for v5.54 1129번째 줄 참고함
                        continue

                    # 원본 데이터의 format이 non-NCDC data 인 경우 9999라고 함
                    # CALMET_UsersGuide.pdf 내용 중 READ56/READ62 Output File Format (Upn.DAT) 부분을 참고하였음
                    rowTime = ("   9999     {:5s}   {:4d}{:2d}{:2d}{:2d}  {:5d}" + ' '*28 + "{:5d}").format(str(stnID), YYYY, MM, DD, HH, mlev, istop)
                    f.write(rowTime + "\n")

                    rowTemp = "   {:6.1f}/{:4.0f}./{:5.1f}/{:3d}/{:3d}"
                    temp = ""
                    for i in range(istop):
                        temp += rowTemp.format(
                            df_tot[stnID].at[t, "기압(hPa)"].tolist()[::-1][i],
                            df_tot[stnID].at[t, "고도(gpm)"].tolist()[::-1][i],
                            df_tot[stnID].at[t, "기온(K)"].tolist()[::-1][i],
                            df_tot[stnID].at[t, "풍향(deg)"].tolist()[::-1][i],
                            int(df_tot[stnID].at[t, "풍속(m/s)"].tolist()[::-1][i]),
                        )
                        # 한 줄에 4개씩 출력함
                        if i != istop-1 and i % 4 == 3:
                            temp += "\n"
                    f.write(temp + "\n")



if __name__ == "__main__":
    # read_SONDE("./test_data/sonde")
    # print(df_tot[47138].at[df_tot[47138].index[0], ])
    # print((999., 999,) == (999., 999,))
    write_up_dat("./UP.DAT", sonde_path="./test_data/sonde", pstop=500, startDt="202402250000", endDt="202403312300")
