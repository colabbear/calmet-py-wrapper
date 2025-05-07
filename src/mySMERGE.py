"""
2025/03/21
작성자: colabbear
1. 공공기관 자료(오직 분자료로만 제공됨) 전처리 함수 구현
2. 함수 주석 약간 수정 (aws, asos 자료는 시간 자료가 기준임을 명시함)
3. write_surf_dat 함수의 output_path 인자를 키워드 인자로 변경


"""

import os
import pandas as pd
from datetime import datetime, timedelta

header = """SURF.DAT        2.0             Header structure with coordinate parameters                     
   1
Produced by mySMERGE Version: 5.57  Level: 070627                                 
NONE"""

# df_tot 은 {지점1 : df, ...} 임
df_tot = {}

# 기상자료개방포털에서 모든 칼럼을 선택하여 다운받은 asos 시간 자료 파일인 경우를 가정함
def read_ASOS(source_folder):
    global df_tot

    # 소스 폴더 내에 있는 모든 csv 파일 찾아서 read
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")

                df = pd.read_csv(csv_file_path, encoding="cp949")

                # 일시 칼럼을 인덱스로 설정
                # 파일 내에 지점이 여러개일 때 인덱스에 중복이 발생하지만 지점별로 저장하기에 문제 없을 것임
                df = df.set_index("일시")
                # datetime 형식으로 변경
                df.index = pd.to_datetime(df.index)

                # 칼럼별로 dtype 적절하게 변경
                df = df.astype({"기온 QC플래그": "Int64",
                                "강수량 QC플래그": "Int64",
                                "풍속 QC플래그": "Int64",
                                "풍향 QC플래그": "Int64",
                                "습도(%)": "Int64",
                                "습도 QC플래그": "Int64",
                                "현지기압 QC플래그": "Int64",
                                "전운량(10분위)": "Int64",})

                # QC플래그가 9이거나 칼럼의 특정 행 값이 빈칸인 경우 결측 처리
                # 결측 처리 값은 SMERGE 소스코드 방식에 따름
                df.loc[(df["기온 QC플래그"] == 9) | (df["기온(°C)"].isna()), "기온(°C)"] = 9999.
                df.loc[(df["풍속 QC플래그"] == 9) | (df["풍속(m/s)"].isna()), "풍속(m/s)"] = 9999.
                df.loc[(df["풍향 QC플래그"] == 9) | (df["풍향(16방위)"].isna()), "풍향(16방위)"] = 9999.
                df.loc[(df["습도 QC플래그"] == 9) | (df["습도(%)"].isna()), "습도(%)"] = 9999
                df.loc[(df["현지기압 QC플래그"] == 9) | (df["현지기압(hPa)"].isna()), "현지기압(hPa)"] = 9999.

                # 강수량은 QC플래그가 9일 때만 결측 처리 빈칸은 0으로 처리 SMERGE 방식을 따름
                df.loc[df["강수량 QC플래그"] == 9, "강수량(mm)"] = 9999.
                df.loc[df["강수량(mm)"].isna(), "강수량(mm)"] = 0.

                # 전운량, 최저운고는 QC플래그가 없으므로 빈칸만을 결측으로 처리
                df.loc[df["전운량(10분위)"].isna(), "전운량(10분위)"] = 9999
                df.loc[df["최저운고(100m )"].isna(), "최저운고(100m )"] = 9999.

                # 기온 및 최저운고 단위 각각 K, ft로 변경하고 칼럼이름에도 반영
                df.loc[df["기온(°C)"] < 9999., "기온(°C)"] += 273.15
                df.loc[df["최저운고(100m )"] < 9999., "최저운고(100m )"] *= 3.280839895
                # 최저운고(100m ) 자료형 변환
                df = df.astype({"최저운고(100m )": "int64"})
                # 풍향 칼럼 이름도 aws자료와 통일을 위해 변경
                df = df.rename(columns={"기온(°C)": "기온(K)", "최저운고(100m )": "최저운고(100ft )", "풍향(16방위)": "풍향(deg)"})

                # 최저운고 결측인 경우 Cloud base height 공식으로 얻은 값으로 대체 (이슬점 온도, 기온이 결측이 아닌 경우에만 할당 함)
                condition = (df["최저운고(100ft )"] > 9998.) & (df["기온(K)"] < 9999.) & (~df["이슬점온도(°C)"].isna())
                df.loc[condition, "최저운고(100ft )"] = ((10 / 2.5) * (df.loc[condition, "기온(K)"] - 273.15 - df.loc[condition, "이슬점온도(°C)"])).astype(int)

                # IPCODE (Precipitation code) 추가 (SMERGE.for 코드를 참고함 잘 이해 안 되는 부분 있기 때문에 수정이 필요할 수 있음)
                df["IPCODE"] = 9999
                df.loc[df["강수량(mm)"] < 0.01, "IPCODE"] = 0
                df.loc[(df["강수량(mm)"] < 9999.) & (df["강수량(mm)"] >= 0.01) & (df["기온(K)"] >= 273.15), "IPCODE"] = 1
                df.loc[(df["강수량(mm)"] < 9999.) & (df["강수량(mm)"] >= 0.01) & (df["기온(K)"] < 273.15), "IPCODE"] = 20

                for stnID in df["지점"].unique():
                    # 같은 지점 다른 데이터가 있는 경우 같은 인덱스는 덮어쓰는 형태로 합치기
                    if stnID in df_tot:
                        # 수직으로 합치기
                        df_tot[stnID] = pd.concat([df_tot[stnID], df], axis=0)
                        # 중복 인덱스(일자) 제거
                        df_tot[stnID] = df_tot[stnID].loc[~df_tot[stnID].index.duplicated(keep='last')]
                    else:
                        df_tot[stnID] = df.loc[df["지점"] == stnID]



# 기상자료개방포털에서 모든 칼럼을 선택하여 다운받은 aws 시간 자료 파일인 경우를 가정함
def read_AWS(source_folder):
    global df_tot

    # 소스 폴더 내에 있는 모든 csv 파일 찾아서 read
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")

                df = pd.read_csv(csv_file_path, encoding="cp949")

                # 일시 칼럼을 인덱스로 설정
                # 파일 내에 지점이 여러개일 때 인덱스에 중복이 발생하지만 지점별로 저장하기에 문제 없을 것임
                df = df.set_index("일시")
                # datetime 형식으로 변경
                df.index = pd.to_datetime(df.index)

                # 칼럼별로 dtype 적절하게 변경
                df = df.astype({"습도(%)": "Int64"})

                # 칼럼의 특정 행 값이 빈칸인 경우 결측 처리
                # 결측 처리 값은 SMERGE 소스코드 방식에 따름
                df.loc[df["기온(°C)"].isna(), "기온(°C)"] = 9999.
                df.loc[df["풍속(m/s)"].isna(), "풍속(m/s)"] = 9999.
                df.loc[df["풍향(deg)"].isna(), "풍향(deg)"] = 9999.
                df.loc[df["강수량(mm)"].isna(), "강수량(mm)"] = 9999.
                df.loc[df["습도(%)"].isna(), "습도(%)"] = 9999
                df.loc[df["현지기압(hPa)"].isna(), "현지기압(hPa)"] = 9999.

                # 기온 단위 K로 바꾸고 칼럼 이름에도 반영
                df.loc[df["기온(°C)"] < 9999., "기온(°C)"] += 273.15
                df = df.rename(columns={"기온(°C)": "기온(K)"})

                # 전운량, 최저운고는 aws 자료에 없으므로 모두 결측 처리하여 칼럼 추가
                df["전운량(10분위)"] = 9999
                df["최저운고(100ft )"] = 9999

                # IPCODE (Precipitation code) 추가 (SMERGE.for 코드를 참고함 잘 이해 안 되는 부분 있기 때문에 수정이 필요할 수 있음)
                df["IPCODE"] = 9999
                df.loc[df["강수량(mm)"] < 0.01, "IPCODE"] = 0
                df.loc[(df["강수량(mm)"] < 9999.) & (df["강수량(mm)"] >= 0.01) & (df["기온(K)"] >= 273.15), "IPCODE"] = 1
                df.loc[(df["강수량(mm)"] < 9999.) & (df["강수량(mm)"] >= 0.01) & (df["기온(K)"] < 273.15), "IPCODE"] = 20

                for stnID in df["지점"].unique():
                    # 같은 지점 다른 데이터가 있는 경우 같은 인덱스는 덮어쓰는 형태로 합치기
                    if stnID in df_tot:
                        # 수직으로 합치기
                        df_tot[stnID] = pd.concat([df_tot[stnID], df], axis=0)
                        # 중복 인덱스(일자) 제거
                        df_tot[stnID] = df_tot[stnID].loc[~df_tot[stnID].index.duplicated(keep='last')]
                    else:
                        df_tot[stnID] = df.loc[df["지점"] == stnID]



# 기상자료개방포털에서 모든 칼럼을 선택하여 다운받은 공공기관 기상관측 분 자료 파일인 경우를 가정함
def read_AWOS(source_folder):
    global df_tot

    df_temp = {}
    # 소스 폴더 내에 있는 모든 csv 파일 찾아서 read
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith('.csv'):
                csv_file_path = os.path.join(root, file)
                print(f"Processing {csv_file_path}")

                df = pd.read_csv(csv_file_path, encoding="cp949")

                # 일시 칼럼을 인덱스로 설정
                # 파일 내에 지점이 여러개일 때 인덱스에 중복이 발생하지만 지점별로 저장하기에 문제 없을 것임
                df = df.set_index("일시")
                # datetime 형식으로 변경
                df.index = pd.to_datetime(df.index)

                for stnID in df["지점"].unique():
                    # 같은 지점 다른 데이터가 있는 경우 같은 인덱스는 덮어쓰는 형태로 합치기
                    if stnID in df_temp:
                        # 수직으로 합치기
                        df_temp[stnID] = pd.concat([df_temp[stnID], df], axis=0)
                        # 중복 인덱스(일자) 제거
                        df_temp[stnID] = df_temp[stnID].loc[~df_temp[stnID].index.duplicated(keep='last')]
                    else:
                        df_temp[stnID] = df.loc[df["지점"] == stnID]

    for stnID in df_temp:
        """
        시간 단위로 묶어주기 (NaN 은 평균 계산할 때 제외됨)
        다음 사이트에서 확인할 수 있는 소스코드를 보면 내용을 찾을 수 있음
        https://github.com/pandas-dev/pandas/blob/v2.2.3/pandas/core/resample.py#L1342-L1384
        
        Compute mean of groups, excluding missing values.

        Parameters
        ----------
        numeric_only : bool, default False
            Include only `float`, `int` or `boolean` data.

            .. versionchanged:: 2.0.0

                numeric_only now defaults to ``False``.

        Returns
        -------
        DataFrame or Series
            Mean of values within each group.
        """
        df_temp[stnID] = df_temp[stnID].resample('h').mean()

        # 칼럼의 특정 행 값이 빈칸인 경우 결측 처리
        # 결측 처리 값은 SMERGE 소스코드 방식에 따름
        df_temp[stnID].loc[df_temp[stnID]["기온(℃)"].isna(), "기온(℃)"] = 9999.
        df_temp[stnID].loc[df_temp[stnID]["풍속(m/s)"].isna(), "풍속(m/s)"] = 9999.
        df_temp[stnID].loc[df_temp[stnID]["풍향(16방위)"].isna(), "풍향(16방위)"] = 9999.
        df_temp[stnID].loc[df_temp[stnID]["상대습도(%)"].isna(), "상대습도(%)"] = 9999.
        df_temp[stnID].loc[df_temp[stnID]["현지기압(hPa)"].isna(), "현지기압(hPa)"] = 9999.

        # 칼럼별로 dtype 적절하게 변경
        df_temp[stnID] = df_temp[stnID].astype({"상대습도(%)": "int"})

        # 기온 단위 K로 바꾸고 칼럼 이름에도 반영
        # 칼럼 이름 asos, aws와 통일
        df_temp[stnID].loc[df_temp[stnID]["기온(℃)"] < 9999., "기온(℃)"] += 273.15
        df_temp[stnID] = df_temp[stnID].rename(
            columns={
                "기온(℃)": "기온(K)",
                "풍향(16방위)": "풍향(deg)",
                "상대습도(%)": "습도(%)",
            }
        )

        # 전운량, 최저운고는 공공기관 기상관측 자료에 없으므로 모두 결측 처리하여 칼럼 추가
        df_temp[stnID]["전운량(10분위)"] = 9999
        df_temp[stnID]["최저운고(100ft )"] = 9999
        # 강수량(mm)도 없으므로(일 누적 강수량만 제공 됨) 모두 결측 처리 하여 칼럼 추가
        df_temp[stnID]["강수량(mm)"] = 9999

        # IPCODE (Precipitation code) 추가 (SMERGE.for 코드를 참고함 잘 이해 안 되는 부분 있기 때문에 수정이 필요할 수 있음)
        # 강수량(mm) 자료가 없으므로 모두 결측 처리
        df_temp[stnID]["IPCODE"] = 9999

    # df_temp를 df_tot에 병합
    df_tot.update(df_temp)



def write_surf_dat(output_path="./surf.dat", startDt="", endDt="", asos_path="", aws_path="", awos_path=""):
    global df_tot
    global header
    if len(asos_path) + len(aws_path) + len(awos_path) == 0:
        print("Please set the path that there is data")
        return 0
    if len(startDt) * len(endDt) == 0:
        print("Please set the date(YYYYMMDDHHmm) that you want to set")
        return 0
    if len(asos_path) > 0:
        read_ASOS(asos_path)
    if len(aws_path) > 0:
        read_AWS(aws_path)
    if len(awos_path) > 0:
        read_AWOS(awos_path)



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
    header2 = "{:6d}{:4d}{:4d}{:6d}{:4d}{:4d}{:5d}{:5d}".format(startYYYY, startJJJ, startHH, endYYYY, endJJJ, endHH,
                                                                -9, len(df_tot))


    # fortran 의 formatted 출력의 인코딩이 ascii로 알려져 있음
    with open(output_path, "w", encoding="ascii") as f:

        f.write(header + "\n")
        f.write(header2 + "\n")

        stations = []
        for stnID in df_tot:
            f.write("{:8d}".format(stnID) + "\n")
            stations.append(stnID)

        for t in time_series:
            YYYY = t.year
            JJJ = t.timetuple().tm_yday
            HH = t.hour
            rowTime = "{:4d}{:4d}{:4d}".format(YYYY, JJJ, HH)
            f.write(rowTime + "\n")

            # all data missing 있으면 출력하기
            isAllmissing = isAllDataMissing(t, stations)
            for i in isAllmissing:
                if isAllmissing[i] == 1:
                    print("There is all missing of " + i + " at " + str(t))

            for stnID in stations:
                # 특정 시간대 자료가 없는 경우 모두 결측 처리
                if t in df_tot[stnID].index:
                    rowTemp = " {:8.3f} {:8.3f} {:4d} {:4d} {:8.3f} {:4d} {:8.3f} {:4d}".format(
                        df_tot[stnID].at[t, "풍속(m/s)"],
                        df_tot[stnID].at[t, "풍향(deg)"],
                        df_tot[stnID].at[t, "최저운고(100ft )"],
                        df_tot[stnID].at[t, "전운량(10분위)"],
                        df_tot[stnID].at[t, "기온(K)"],
                        df_tot[stnID].at[t, "습도(%)"],
                        df_tot[stnID].at[t, "현지기압(hPa)"],
                        df_tot[stnID].at[t, "IPCODE"])
                else:
                    rowTemp = " {:8.3f} {:8.3f} {:4d} {:4d} {:8.3f} {:4d} {:8.3f} {:4d}".format(9999.,
                                                                                                9999.,
                                                                                                9999,
                                                                                                9999,
                                                                                                9999.,
                                                                                                9999,
                                                                                                9999.,
                                                                                                9999)

                f.write(rowTemp + "\n")



def isAllDataMissing(t, stations):
    global df_tot
    num_dataMissing = {
        "최저운고(100ft )": 0,
        "전운량(10분위)": 0,
        "기온(K)": 0,
        "습도(%)": 0,
        "현지기압(hPa)": 0,
    }
    for stnID in stations:
        if t in df_tot[stnID].index:
            if df_tot[stnID].at[t, "최저운고(100ft )"] == 9999: num_dataMissing["최저운고(100ft )"] += 1
            if df_tot[stnID].at[t, "전운량(10분위)"] == 9999: num_dataMissing["전운량(10분위)"] += 1
            if df_tot[stnID].at[t, "기온(K)"] == 9999.: num_dataMissing["기온(K)"] += 1
            if df_tot[stnID].at[t, "습도(%)"] == 9999: num_dataMissing["습도(%)"] += 1
            if df_tot[stnID].at[t, "현지기압(hPa)"] == 9999.: num_dataMissing["현지기압(hPa)"] += 1
        else:
            num_dataMissing["최저운고(100ft )"] += 1
            num_dataMissing["전운량(10분위)"] += 1
            num_dataMissing["기온(K)"] += 1
            num_dataMissing["습도(%)"] += 1
            num_dataMissing["현지기압(hPa)"] += 1

    for i in num_dataMissing:
        if num_dataMissing[i] >= len(stations):
            num_dataMissing[i] = 1
        else:
            num_dataMissing[i] = 0

    return num_dataMissing



if __name__ == "__main__":
    write_surf_dat(
        output_path="./surf.dat",
        asos_path="./test_data/asos",
        aws_path="./test_data/aws",
        awos_path="./test_data/awos",
        startDt="202403010000",
        endDt="202403312300"
    )
