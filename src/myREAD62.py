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

                # 일시 칼럼을 인덱스로 설정
                # 파일 내에 지점이 여러개일 때 인덱스에 중복이 발생하지만 지점별로 저장하기에 문제 없을 것임
                df = df.set_index("일시(UTC)")
                # datetime 형식으로 변경
                df.index = pd.to_datetime(df.index)

                # 칼럼별로 dtype 적절하게 변경
                df = df.astype({"기압(hPa)": "float64",
                                "풍향(deg)": "Int64"})

                # 칼럼의 특정 행 값이 빈칸인 경우 결측 처리
                # 결측 처리 값은 READ62 소스코드 방식에 따름 (READ62.FOR v5.54 1021번째 줄부터 참고)
                df.loc[df["고도(gpm)"].isna(), "고도(gpm)"] = 9999.
                df.loc[df["기온(°C)"].isna(), "기온(°C)"] = 999.9
                df.loc[df["풍향(deg)"].isna(), "풍향(deg)"] = 999
                df.loc[df["풍속(knot)"].isna(), "풍속(knot)"] = 999.9

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
                        df_tot[stnID] = df_tot[stnID].loc[~df_tot[stnID].index.duplicated(keep='last')]
                    else:
                        df_tot[stnID] = df.loc[df["지점"] == stnID]



def write_up_dat(output_path, pstop=700.0, startDt="", endDt="", sonde_path=""):
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
    jdat = 3 # kma csv format
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

    istop =

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
    header2 = " {:5d}{:5d}{:5d}{:5d}{:5d}{:5d}{:5.0f}{:5d}{:5d}".format(startYYYY, startJJJ, startHH, endYYYY, endJJJ, endHH, pstop, jdat, iform)
    header3 = "     {:1s}    {:1s}    {:1s}    {:1s}".format(LHT, LTEMP, LWD, LWS)



    idx = output_path.rfind('.')
    # fortran 의 formatted 출력의 인코딩이 ascii로 알려져 있음
    for stnID in df_tot:
        with open(output_path[:idx] + "_" + str(stnID) + output_path[idx:], "w", encoding="ascii") as f:
            f.write(header + "\n")
            f.write(header2 + "\n")
            f.write(header3 + "\n")
            for t in time_series:
                YYYY = t.year
                JJJ = t.timetuple().tm_yday
                HH = t.hour
                rowTime = "   6201  {:8s}   {:4d}{:2d}{:2d}{:2d}  {:5d}{' ' * 66}{:5d}".format(str(stnID), YYYY, )
                f.write()

                rowTemp = "   {:6.1f}/{:5.0}/{:5.1f}/{:3d}/{:3d}"
                if t in df_tot[stnID].index:
                    temp = ""
                    for i in range(4):
                        temp += rowTemp.format()

                    rowTemp = rowTemp.format()

                f.write()







if __name__ == "__main__":
    write_up_dat("./UP.DAT", sonde_path="./test_data/sonde", startDt="202403010000", endDt="202403312300")
