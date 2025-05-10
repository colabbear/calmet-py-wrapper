"""
2025/03/20
작성자: colabbear
1.  도메인은 울산지역 고정이며(UTM zone도 52로 고정), 상층 데이터는 포항 고정임
2.  공공기관관측지점 자료에 대해서는 아직 SMERGE 전처리 구현 안 되어 있음
3.  지점 위치는 시작일을 기준으로 함 (모델 종료일까지 지점이 바뀌는 경우 위치 반영 기능 구현 안 되어 있음)
4.  static 폴더 파일들은 수정하면 안 됨. 특히 CALMET.INP 절대 건들면 안 됨. myCALMET.INP 파일 생성 기준이 되는 파일임
5.  calmet.exe는 기존 소스코드(CALMET - Version 5.8.5 - Level 151214)를
    CALMET.DAT 출력포맷을 ascii로 변경하고 격자별 위경도값을 추가적으로 출력하도록 수정하여 컴파일하여 얻은 실행파일임
    컴파일의 경우, 구버전 우분투를 가상환경에서 부팅하여 gfortran 구버전을 install 하고
    https://calpuff.org/calpuff/download/download.htm#EPA_VERSION 에서 얻은
    CALMET_v5.8.5.zip 내의 컴파일 커맨드를 참조하여 진행함



2025/03/27
작성자: colabbear
1.  runModel 함수 작성함
    기상청 자료개방포털의 기상관측자료들이 저장된 경로, 모델을 돌리고자 하는 지역에 대해 CALPUFF VIEW 프로젝트를 생성하여 얻은
    GEO.DAT, CALMET.INP 경로, CALMET 모델을 돌릴 때 input, output 파일이 저장될 각 폴더 경로, 모델을 돌릴 기간 및
    상층 자료 전처리 대상 기간을 인자로 넘겨 주면 전달 받은 인자에 따라 CALMET 모델을 실행하는 함수임



"""

import os

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



def runModel(
        input_folder="",
        output_folder="",
        asos_path="",
        aws_path="",
        awos_path="",
        sonde_path="",
        GEO_DAT_path="",
        input_CALMET_INP_path="",
        UP_startDt="",
        UP_endDt="",
        startDt="",
        endDt="",
        pstop=500,
        output_format="ascii"
):
    if len(input_folder) * len(output_folder) == 0:
        print("Please set input output folder")
        return 0
    if len(asos_path) + len(aws_path) + len(awos_path) == 0:
        print("Please set the surface data file path")
        return 0
    if len(sonde_path) == 0:
        print("Please set the upper air data file path")
        return 0
    if len(GEO_DAT_path) == 0:
        print("Please set GEO.DAT path")
        return 0
    if len(input_CALMET_INP_path) == 0:
        print("Please set input CALMET.INP file path")
        return 0
    if len(startDt) * len(endDt) * len(UP_startDt) * len(UP_endDt) == 0:
        print("Please set the date")
        return 0

    if not os.path.exists(input_folder):
        os.makedirs(input_folder)
        print(f"'{input_folder}' 폴더 생성")
    elif not os.path.isdir(input_folder):
        print("Please set the correct input folder path")
        return 0
    else:
        print(f"'{input_folder}' 폴더 이미 존재")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"'{output_folder}' 폴더 생성")
    elif not os.path.isdir(output_folder):
        print("Please set the correct output folder path")
        return 0
    else:
        print(f"'{output_folder}' 폴더 이미 존재")


    if input_folder[-1] != "/":
        input_folder += "/"
    if output_folder[-1] != "/":
        output_folder += "/"

    SRF_DAT_path = input_folder + "surf.dat"
    UP_DAT_path = input_folder + "UP.DAT"
    CALMET_INP_path = input_folder + "myCALMET.INP"
    CALMET_LST_path = output_folder + "CALMET.LST"
    CALMET_DAT_path = output_folder + "CALMET.DAT"

    smerge.write_surf_dat(
        output_path=SRF_DAT_path,
        asos_path=asos_path,
        aws_path=aws_path,
        awos_path=awos_path,
        startDt=startDt,
        endDt=endDt
    )
    read62.write_up_dat(
        output_path=UP_DAT_path,
        sonde_path=sonde_path,
        pstop=pstop,
        startDt=UP_startDt,
        endDt=UP_endDt
    )

    # 폴더 내의 파일과 폴더 이름을 리스트로 반환
    files_and_folders = os.listdir(input_folder)
    # 폴더 내의 파일만 필터링
    files = [f for f in files_and_folders if os.path.isfile(os.path.join(input_folder, f))]
    for i in files:
        if i[:2] == "UP":
            UP_DAT_path = input_folder + i

    setINP.setCALMET_INP(
        output_path=CALMET_INP_path,
        input_CALMET_INP_path=input_CALMET_INP_path,
        GEO_DAT_path=GEO_DAT_path,
        SRF_DAT_path=SRF_DAT_path,
        UP_DAT_path=UP_DAT_path,
        CALMET_LST_path=CALMET_LST_path,
        CALMET_DAT_path=CALMET_DAT_path,
        startDt=startDt,
        endDt=endDt
    )

    if output_format == "binary":
        runCALMET(
            calmet_path="./calmet_binary.exe",
            CALMET_INP_path=CALMET_INP_path
        )
    else:
        runCALMET(
            calmet_path="./calmet_ascii.exe",
            CALMET_INP_path=CALMET_INP_path
        )



if __name__ == "__main__":
    # smerge.write_surf_dat(
    #     "./input_file/surf.dat",
    #     asos_path="./test_data/asos_하동",
    #     aws_path="./test_data/aws_하동",
    #     # awos_path="./test_data/awos",
    #     startDt="202112310000",
    #     endDt="202301010000"
    # )
    # read62.write_up_dat(
    #     "./input_file/UP.DAT",
    #     sonde_path="./test_data/sonde_하동",
    #     pstop=500,
    #     startDt="202111010000",
    #     endDt="202302010000"
    # )
    # setINP.setCALMET_INP(
    #     "./input_file/myCALMET.INP",
    #     startDt="202112310000",
    #     endDt="202301010000"
    # )
    # runCALMET(calmet_path="./calmet_old.exe", CALMET_INP_path="./input_file/myCALMET.INP")

    runModel(
        input_folder="./hadong/input",
        output_folder="./hadong/output",
        asos_path="./test_data/asos_하동",
        aws_path="./test_data/aws_하동",
        sonde_path="./test_data/sonde_하동",
        GEO_DAT_path="./static/GEO.DAT",
        input_CALMET_INP_path="./static/CALMET.INP",
        UP_startDt="202111010000",
        UP_endDt="202302010000",
        startDt="202112310000",
        endDt="202301010000",
        output_format="binary",
    )
