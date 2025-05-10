"""
2025/04/01
작성자: colabbear
1.  변수 endtf 사용하지 않으므로 삭제
2.  read_binary_CALMET_DAT 추가
    fortran 은 unformatted 로 파일을 작성할 때 레코드마다 앞뒤로 레코드마커가 있다고 함 (ChatGPT의 답변)
    fortran 공식 문서에서 관련 내용 찾는 것이 좋을 듯



"""



import re
import struct



# CALMET-formatted output file (CALMET.DAT)인 경우를 가정함
# wrtr1d 또는 wrtr2d 서브루틴을 호출하여 작성되었으면 real
# wrti1d 또는 wrti2d 서브루틴을 호출하여 작성되었으면 integer
data_type = {
    "ZFACE": "real",
    "XSSTA": "real",
    "YSSTA": "real",
    "XUSTA": "real",
    "YUSTA": "real",
    "XPSTA": "real",
    "YPSTA": "real",
    "Z0": "real",
    "ILANDU": "integer",
    "ELEV": "real",
    "XLAI": "real",
    "NEARS": "integer",
    "XLAT": "real",
    "XLON": "real",
    "U-LEV": "real",
    "V-LEV": "real",
    "WFACE": "real",
    "T-LEV": "real",
    "IPGT": "integer",
    "USTAR": "real",
    "ZI": "real",
    "EL": "real",
    "WSTAR": "real",
    "RMM": "real",
    "TEMPK": "real",
    "RHO": "real",
    "QSW": "real",
    "IRH": "integer",
    "IPCODE": "integer",
}



def read_record(file, length=0):
    """
    :param file: 바이너리 파일 객체
    :param length: 읽을 bytes 수
    :return: 읽은 바이트 데이터, 레코드 마커
    """

    recordMarker = file.read(4 * 1)
    # EOF인 경우
    if not recordMarker:
        return b'', 0
    recordMarker = struct.unpack('i', recordMarker)[0]

    if length == 0:
        length = recordMarker
    data = file.read(length)

    # 뒤쪽 레코드 마커는 앞쪽 레코드 마커와 같음 따라서 읽기만 함
    file.read(4 * 1)

    return data, recordMarker



def read_binary_CALMET_DAT(file_path):
    # re 모듈 사용 부분은 chatGPT에게 질문하면서 작성함
    data = {}
    with open(file_path, 'rb') as f:
        # Record 1 - File Declaration -- 24 words
        hd1, _ = read_record(f, length=4 * 24)
        # print(hd1.decode('ascii'))

        # Record 2 - Number of comment lines -- 1 word
        ncom, _ = read_record(f, length=4 * 1)
        # print(struct.unpack('i', ncom)[0])
        ncom = struct.unpack('i', ncom)[0]

        # Record 3 - NCOM+2 까지 Comment record section -- 33 words each
        for i in range(ncom):
            line, _ = read_record(f, length=4 * 33)
            line = line.decode("ascii")

            # 각 변수 출력 이전 데이터 읽기 ncom+4부터 ZFACE 변수를 시작으로 출력되어있음 calmet.for 주석에 명시되어 있음

            # 각 줄마다 느낌표 사이에 있는 문자열 찾기
            matches = re.findall(r'!(.*?)!', line)

            # 느낌표 사이에 문자열이 있는 패턴이 존재하면
            if matches:
                for match in matches:

                    # END인 경우는 제외
                    if match.strip() == "END":
                        continue

                    # = 으로 구분
                    temp = match.split('=')

                    # 쉼표 또는 공백으로 구분
                    temp[1] = re.split(r'[,\s]+', temp[1].strip())

                    # 배열 길이가 2이상일 때만 배열로 할당
                    if len(temp[1]) == 1:
                        data[temp[0].strip()] = temp[1][0]
                    else:
                        data[temp[0].strip()] = temp[1]

        # record NCOM+3 - run control parameters -- 37 words
        data0, _ = read_record(f, length=4 * 37)

        # data record
        while True:
            temp, recordMarker = read_record(f)
            if not temp:
                break

            # CLABEL - character*8   - Variable name
            clabel = temp[:8].decode('ascii').strip()

            # NDATHR - integer       - Date and time of data (YYYYJJJHH)
            ndathr = struct.unpack('i', temp[8:12])[0]
            ndathr = str(ndathr)

            # 날짜별로 정리하기
            if ndathr not in data:
                data[ndathr] = {}

            # 자료형 지정
            if clabel not in data_type:
                filtered_keys = [key for key in data_type.keys() if key in clabel][0]
                theType = data_type[filtered_keys]
            else:
                theType = data_type[clabel]

            if theType == "real":
                record_data_len = (recordMarker-12) // 4
                data[ndathr][clabel] = struct.unpack(f'<{record_data_len}f', temp[12:])
            else:
                record_data_len = (recordMarker - 12) // 4
                data[ndathr][clabel] = struct.unpack(f'<{record_data_len}i', temp[12:])

    return data



def read_ascii_CALMET_DAT(file_path):
    # re 모듈 사용 부분은 chatGPT에게 질문하면서 작성함
    data = {}
    with open(file_path, 'r') as f:
        ncom = -999
        lines = f.readlines()
        for i, line in enumerate(lines):

            # 두 번째 줄의 ncom 저장
            if i == 1:
                ncom = int(line.strip())

            if ncom < 0 or (ncom > 0 and ncom+4 > i+1):
                # 각 변수 출력 이전 데이터 읽기 ncom+4부터 ZFACE 변수를 시작으로 출력되어있음 calmet.for 주석에 명시되어 있음

                # 각 줄마다 느낌표 사이에 있는 문자열 찾기
                matches = re.findall(r'!(.*?)!', line)

                # 느낌표 사이에 문자열이 있는 패턴이 존재하면
                if matches:
                    for match in matches:

                        # END인 경우는 제외
                        if match.strip() == "END":
                            continue

                        # = 으로 구분
                        temp = match.split('=')

                        # 쉼표 또는 공백으로 구분
                        temp[1] = re.split(r'[,\s]+', temp[1].strip())

                        # 배열 길이가 2이상일 때만 배열로 할당
                        if len(temp[1]) == 1:
                            data[temp[0].strip()] = temp[1][0]
                        else:
                            data[temp[0].strip()] = temp[1]
            else:
                # 각 변수 데이터 읽기

                line = line.strip()
                # 변수명
                # calmet.for 에서 clabel은 character*8 로 선언되어 있으므로 문자열 시작하는 곳 부터 8자리를 clabel로 읽어야 함
                clabel = line[0:8].strip()

                temp = line[8:].split()
                # Date and time of data (YYYYJJJHH)
                ndathr = temp[0]

                # 날짜별로 정리하기
                if ndathr not in data:
                    data[ndathr] = {}

                # 정수 값을 따로 구분하지 않고 일괄적으로 float으로 변환하여 할당
                data[ndathr][clabel] = list(map(float, temp[1:]))

    return data



if __name__ == "__main__":
    print(read_ascii_CALMET_DAT("./CALMET.DAT"))
    print(read_binary_CALMET_DAT("./samcheonpo/output/CALMET.DAT"))
