import os

import requests
import numpy as np

import cartopy
import cartopy.crs as ccrs
from cartopy.io.shapereader import Reader
from cartopy.feature import ShapelyFeature
from shapely.geometry import shape

import matplotlib.pyplot as plt

import pickle
from datetime import datetime, timedelta

import read_CALMET_DAT as rcd



# ulsan
deg_step = 0.05
lonl = 128.95
lonr = 129.50
latl = 35.30
latu = 35.75



if __name__ == "__main__":
    startDt = datetime.strptime("202403010000", '%Y%m%d%H%M')
    endDt = datetime.strptime("202403312300", '%Y%m%d%H%M')

    currentDt = startDt
    time_series = []
    while currentDt <= endDt:
        time_series.append(currentDt)
        currentDt += timedelta(hours=1)  # 1시간 간격

    data = rcd.read_ascii_CALMET_DAT("./output_file/CALMET.DAT")
    nx = int(data['NX'])
    ny = int(data['NY'])
    lat_sample = np.array(data['0']['XLAT'])
    lat_sample = lat_sample.reshape(nx, ny)
    lon_sample = np.array(data['0']['XLON'])
    lon_sample = lon_sample.reshape(nx, ny)



    # shapefile 경로 지정
    # wgs84로 변환하고 사용해야 함
    shp_file = "./shp/sig.shp"

    # shapefile 읽기
    reader = Reader(shp_file)

    # geometries를 Shapely 형태로 변환
    geometries = [shape(geom) for geom in reader.geometries()]

    for t in time_series:
        YYYY = t.year
        JJJ = t.timetuple().tm_yday
        HH = t.hour
        date = "{:04d}{:03d}{:02d}".format(YYYY, JJJ, HH)

        u_sample = np.array(data[date]['U-LEV  1'])
        u_sample = u_sample.reshape(nx, ny)
        v_sample = np.array(data[date]['V-LEV  1'])
        v_sample = v_sample.reshape(nx, ny)

        ax1 = plt.axes(projection=ccrs.PlateCarree())
        ax1.set_extent([lonl, lonr, latl, latu], ccrs.PlateCarree())
        shape_feature = ShapelyFeature(geometries, ccrs.PlateCarree(), edgecolor='black', facecolor='none')
        ax1.add_feature(shape_feature)
        # ax1.scatter(grid_x.flatten(), grid_y.flatten())
        # ax1.contour(grid_x, grid_y, grid_z, levels=np.arange(-10, 30, 1))
        # ax1.quiver(grid_xx, grid_yy, grid_u, grid_v, scale=100)
        ax1.quiver(lon_sample, lat_sample, u_sample, v_sample, scale=150)
        ax1.set_title(t.strftime("%Y-%m-%d %H:%M:%S"))

        # plt.show()
        plt.pause(0.5)
        plt.clf()
