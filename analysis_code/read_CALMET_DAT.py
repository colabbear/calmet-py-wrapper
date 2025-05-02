import re



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
                # 각 변수 출력 이전 데이터 읽기 ncom+4부터 ZFACE 변수 부터 출력됨 calmet.for 주석에 명시되어 있음

                # 각 줄마다 느낌표 사이에 있는 문자열 찾기
                matches = re.findall(r'!(.*?)!', line)

                # 느낌표 사이에 문자열이 있는 패턴이 존재하면
                endtf = 0
                if matches:
                    for match in matches:

                        # END인 경우는 제외
                        if match.strip() == "END":
                            endtf = 1
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