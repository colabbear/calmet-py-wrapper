"""
2025/03/20
작성자: colabbear

1. 도메인은 울산지역 고정이며(UTM zone도 52로 고정), 상층 데이터는 포항 고정임

2. 공공기관관측지점 자료에 대해서는 아직 SMERGE 전처리 구현 안 되어 있음

3. 지점 위치는 시작일을 기준으로 함 (모델 종료일까지 지점이 바뀌는 경우 위치 반영 기능 구현 안 되어 있음)

4. static 폴더 파일들은 수정하면 안 됨. 특히 CALMET.INP 절대 건들면 안 됨. myCALMET.INP 파일 생성 기준이 되는 파일임

5. calmet.exe는 기존 소스코드(CALMET - Version 5.8.5 - Level 151214)를
CALMET.DAT 출력포맷을 ascii로 변경하고 격자별 위경도값을 추가적으로 출력하도록 수정하여 컴파일하여 얻은 실행파일임
컴파일의 경우, 구버전 우분투를 가상환경에서 부팅하여 gfortran 구버전을 install 하고
https://calpuff.org/calpuff/download/download.htm#EPA_VERSION 에서 얻은 CALMET_v5.8.5.zip 내의 컴파일 커맨드를 참조하여 진행함

"""

import subprocess

import mySMERGE as smerge
import myREAD62 as read62
import setCALMET_INP as setINP



def runCALMET(calmet_path="", CALMET_INP_path=""):
    if len(calmet_path) == 0:
        print("Please set calmet.exe path")
        return 0
    if len(CALMET_INP_path) == 0:
        print("Please set CALMET.INP path")
        return 0

    # subprocess 모듈 사용은 chatGPT 답변 코드를 활용함
    # 실행할 EXE 파일 경로와 인자 리스트
    exe_file = calmet_path
    args = [CALMET_INP_path]  # 넘기고 싶은 인자들

    with subprocess.Popen([exe_file] + args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as proc:
        # 표준 출력 실시간으로 처리
        for line in proc.stdout:
            print(exe_file + " (STDOUT):", line.strip())

        # 표준 오류 실시간으로 처리
        for line in proc.stderr:
            print(exe_file + " (STDERR):", line.strip())

        # 종료 코드 기다리기
        proc.wait()



if __name__ == "__main__":
    # smerge.write_surf_dat(
    #     "./input_file/surf.dat",
    #     asos_path="./test_data/asos",
    #     aws_path="./test_data/aws",
    #     awos_path="./test_data/awos",
    #     startDt="202402250000",
    #     endDt="202403312300"
    # )
    read62.write_up_dat(
        "./input_file/UP.DAT",
        sonde_path="./test_data/sonde",
        pstop=500,
        startDt="202402250000",
        endDt="202404020000"
    )
    setINP.setCALMET_INP(
        "./input_file/myCALMET.INP",
        startDt="202403010000",
        endDt="202404010000"
    )

    runCALMET(calmet_path="./calmet.exe", CALMET_INP_path="./input_file/myCALMET.INP")
