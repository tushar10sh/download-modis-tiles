import os
from datetime import datetime, timedelta
from multiprocessing import Pool

def download_modis_tcc_tile(*args, **kwargs):
    print("Args: ", *args, args[0])
    print("Kwargs: ", **kwargs)
    curr_date = None
    zoom_level = None
    inputs_parsed = False
    try:
        curr_date = args[0]['curr_date']
        zoom_level = args[0]['zoom_level']
        inputs_parsed = True
    except:
        try:
           curr_date = kwargs['curr_date']
           zoom_level = kwargs['zoom_level']
           inputs_parsed = True
        except:
            return
    if not inputs_parsed:
        return

    z = zoom_level
    x_range = 2**z -1
    for x in range(x_range+1):
        for y in range(x_range+1):
            url = f"https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Aqua_CorrectedReflectance_TrueColor/default/{curr_date}/250m/{z}/{y}/{x}.jpg"
            dir_creat_cmd = f"mkdir -p /data/modia-tcc-250m-tiles/{curr_date}/{z}/{y}/"
            download_cmd = f"wget {url} -O /data/modia-tcc-250m-tiles/{curr_date}/{z}/{y}/{x}.jpg -o /data/modia-tcc-250m-tiles/{curr_date}/{z}/{y}/{x}-http.log"
            output_dirname = f"/data/modia-tcc-250m-tiles/{curr_date}/{z}/{y}/"
            if not os.path.exists(output_dirname):
                print(dir_creat_cmd)
                os.system(dir_creat_cmd)
            print(download_cmd)
            os.system(download_cmd)
    return True

def parallel_download(start_date, end_date, zoom_levels, poolSize=4):
    input_set = []
    curr_date = start_date
    while curr_date < end_date:
        formatted_date_string = f"{curr_date.strftime('%Y-%m-%d')}"
        for zoom in zoom_levels:
            input_kwargs = { 'curr_date': formatted_date_string, 'zoom_level': zoom }
            input_set.append( input_kwargs )
        curr_date = curr_date + timedelta(days=1)
    
    with Pool(processes=poolSize) as pool:
        result = pool.map(download_modis_tcc_tile, input_set)
        for res in result:
            pass
    return True


def parallel_main():
    start_date = datetime(2022, 11, 27) # day ocm-3 launch
    curr_date = start_date
    today = datetime.now()
    zoom_levels = [3, 4, ]
    parallel_download(start_date, today, zoom_levels)

def main():
    start_date = datetime(2022, 11, 27) # day ocm-3 launch
    curr_date = start_date
    today = datetime.now()
    zoom_levels = [3, 4, 5, ]
    while curr_date < today:
        formatted_date_string = f"{curr_date.strftime('%Y-%m-%d')}"
        for zoom in zoom_levels:
            download_modis_tcc_tile(curr_date=formatted_date_string, zoom_level=zoom)
        curr_date = curr_date + timedelta(days=1)

if __name__ == '__main__':
    parallel_main()
        
