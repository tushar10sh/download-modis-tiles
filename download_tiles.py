import os
from datetime import datetime, timedelta
from multiprocessing import Pool
import shutil
import requests

data_download_dir = '/data/modia-tcc-250m-tiles/'
use_wget = False

def download_modis_tcc_tile(*args, **kwargs):
    curr_date = None
    zoom_level = None
    verbose = False
    inputs_parsed = False
    try:
        if 'curr_date' in args[0].keys():
            curr_date = args[0]['curr_date']
        if 'zoom_level' in args[0].keys():
            zoom_level = args[0]['zoom_level']
        if 'verbose' in args[0].keys():
            verbose = args[0]['verbose']
        inputs_parsed = True
    except:
        try:
           curr_date = kwargs['curr_date']
           zoom_level = kwargs['zoom_level']
           inputs_parsed = True
        except:
            return False
    if not inputs_parsed:
        print("inputs not parsed")
        return False

    if verbose:
        print("Args:" *args)
        print("KWargs:", **kwargs)

    z = zoom_level
    #x_range = 2**z -1
    x_range_dict = {'3': 9, '4': 19, '5': 39 }
    y_range_dict = {'3': 4, '4':  9, '5': 19 }
    if z > 5:
        return False
    for y in range(y_range_dict[str(z)]+1):
        for x in range(x_range_dict[str(z)]+1):
            url = f"https://gibs.earthdata.nasa.gov/wmts/epsg4326/best/MODIS_Aqua_CorrectedReflectance_TrueColor/default/{curr_date}/250m/{z}/{y}/{x}.jpg"
            output_dirname = os.path.join(data_download_dir, f"{curr_date}/{z}/{y}/")
            output_filename = os.path.join(output_dirname, f"{x}.jpg")
            output_log = os.path.join(output_dirname, f"{x}-http.log")
            download_cmd = f"wget {url} -O {output_filename} -o {output_log}"
            if not os.path.exists(output_dirname):
                if verbose:
                    print(f"Creating directory: {output_dirname}")
                os.makedirs(output_dirname)
            if use_wget:
                if verbose:
                    print(download_cmd)
                os.system(download_cmd)
            else:
                try:
                    res = requests.get(url, stream=True)
                    if res.status_code == 200:
                        with open(output_filename, "wb") as of:
                            shutil.copyfileobj(res.raw, of)
                    else:
                        print(f"{url} -> status-code: {res.status_code}")
                except Exception as e:
                    print(e)
                    return False
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
        results = pool.map(download_modis_tcc_tile, input_set)
        total_inputs = len(input_set)
        successful_downloads = 0
        for res in results:
            if res:
                successful_downloads = successful_downloads + 1
        print(f"Download stats: {successful_downloads} completed out of {total_inputs}.")
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
        
