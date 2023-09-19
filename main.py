import pandas as pd
import re
import time
from functools import wraps
import os
import geopandas as gpd
from shapely.geometry import Polygon
import matplotlib.pyplot as plt


def read_data_file(file_path: str) -> pd.DataFrame:
    with open(file_path, 'r') as f:
        raw_file = f.readlines()

    list_dados = [line.split() for line in raw_file]
    float_raw_lines = [list(map(float, raw_line)) for raw_line in list_dados]
    return pd.DataFrame(float_raw_lines, columns=['lat', 'long', 'data_value'])


def read_contour_file(file_path: str) -> pd.DataFrame:
    line_split_comp = re.compile(r'\s*,')

    with open(file_path, 'r') as f:
        raw_file = f.readlines()

    l_raw_lines = [line_split_comp.split(raw_file_line.strip()) for raw_file_line in raw_file]
    l_raw_lines = list(filter(lambda item: bool(item[0]), l_raw_lines))
    float_raw_lines = [list(map(float, raw_line))[:2] for raw_line in l_raw_lines]
    header_line = float_raw_lines.pop(0)
    assert len(float_raw_lines) == int(header_line[0])
    return pd.DataFrame(float_raw_lines, columns=['lat', 'long'])


def read_full_data(file_path: str) -> pd.DataFrame:
    data_df_acc = pd.DataFrame()
    for filename in os.listdir(file_path):
        f = os.path.join(file_path, filename)
        # checking if it is a file
        if os.path.isfile(f):
            data_df = read_data_file(f)
            data_df['forecasted_date'] = f[-10:-4]
            data_df['forecast_date'] = f[-17:-11]
            data_df_acc = pd.concat([data_df,data_df_acc])
    data_df_acc
    data_df_acc['forecasted_date'] = pd.to_datetime(data_df_acc['forecasted_date'])
    data_df_acc['forecasted_date'] = data_df_acc['forecasted_date'].apply(lambda x: x.strftime('%d/%m/%Y'))
    data_df_acc['forecast_date'] = pd.to_datetime(data_df_acc['forecast_date'])
    data_df_acc['forecast_date'] = data_df_acc['forecast_date'].apply(lambda x: x.strftime('%d/%m/%Y'))
    return pd.DataFrame(data_df_acc)


def apply_contour(contour_df: pd.DataFrame, data_df_acc: pd.DataFrame) -> pd.DataFrame:
    data_df_acc.drop(['forecasted_date', 'forecast_date'], axis=1)
    gdf1 = gpd.GeoDataFrame(countour_df, geometry=gpd.points_from_xy(countour_df['lat'], countour_df['long']))
    polygon = gdf1.unary_union
    gdf2 = gpd.GeoDataFrame(data_df_acc, geometry=gpd.points_from_xy(data_df_acc['lat'], data_df_acc['long']))
    clipped_gdf2 = gdf2.cx[polygon.bounds[0]:polygon.bounds[2], polygon.bounds[1]:polygon.bounds[3]]
    clipped_gdf2 = clipped_gdf2.groupby(by=['forecast_date','forecasted_date','lat','long','geometry'],as_index=False).sum()
    clipped_gdf2['precipitacao_acumulada'] = clipped_gdf2.groupby(['geometry'])['data_value'].cumsum()
    return pd.DataFrame(clipped_gdf2)

    
def first_look(clipped_gdf2: pd.DataFrame,countour_df: pd.DataFrame,file_path: str):
    points_gdf = gpd.GeoDataFrame(clipped_gdf2, geometry=clipped_gdf2['geometry'])
    gdf1 = gpd.GeoDataFrame(countour_df, geometry=gpd.points_from_xy(countour_df['lat'], countour_df['long']))
    fig, ax = plt.subplots(figsize=(10, 6))
    gdf1.plot(ax=ax, color='none', edgecolor='black')
    points_gdf.plot(column='precipitacao_acumulada', cmap='YlGnBu', legend=True, ax=ax, markersize=1000)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Mapa de Calor de Precipitação')
    plt.tight_layout()
    name = file_path + 'first'
    plt.savefig(name)
    plt.show()
    pass


def second_look(clipped_gdf2: pd.DataFrame,file_path: str):
    fig, ax = plt.subplots(figsize=(10, 6))
    for forecasted_date, forecast_date in clipped_gdf2.groupby('forecasted_date'):
        forecast_date.plot(x='forecasted_date', y='precipitacao_acumulada', label=f'forecasted_date {forecasted_date}', ax=ax)
    plt.xlabel('Data')
    plt.ylabel('Precipitação Acumulada')
    plt.title('Precipitação Acumulada ao Longo do Tempo')
    plt.legend(loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    name = file_path + 'second'
    plt.savefig(name)
    plt.show()

def third_look(clipped_gdf2: pd.DataFrame,file_path: str):
    pivot_df = clipped_gdf2.pivot(index='forecasted_date', columns='geometry', values='data_value')
    plt.figure(figsize=(12, 6))
    pivot_df.plot(kind='bar', stacked=True)
    plt.xlabel('Data')
    plt.ylabel('Precipitação Acumulada')
    plt.title('Precipitação Acumulada ao Longo do Tempo')
    plt.legend(title='Pontos', loc='upper left')
    plt.grid(True)
    plt.tight_layout()
    name = file_path + 'third'
    plt.savefig(name)
    plt.show()
    pass

def main() -> None:
    files_path =  '/mnt/c/Users/pedro.ortega/Downloads/btg-energy-challenge/btg-energy-challenge/forecast_files/'
    save_path = '/mnt/c/Users/pedro.ortega/Downloads/btg-energy-challenge/btg-energy-challenge/result/'
    contour_path = '/mnt/c/Users/pedro.ortega/Downloads/btg-energy-challenge/btg-energy-challenge/PSATCMG_CAMARGOS.bln'
    contour_df = read_contour_file(contour_path)
    first_df= read_full_data(files_path)
    second_df = apply_contour(countour_df,first_df)
    first_look(second_df,countour_df,save_path)
    second_look(second_df,save_path)
    third_look(second_df,save_path)

if __name__ == '__main__':
    main()
