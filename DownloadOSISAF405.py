from ftplib import FTP
import os
import sys
import argparse
from multiprocessing import Pool, cpu_count

def get_file_url(year, month=None):
    if year < 2009:
        return None
    base_url = 'ftp://osisaf.met.no/'
    ftp = FTP('osisaf.met.no')      # connect to host, default port
    ftp.login()                     # user anonymous, passwd anonymous@
    dir_path = 'archive/ice/drift_lr/merged/{}/'.format(year)
    ftp.cwd(dir_path)
    if month is not None:
        dir_path_list = ['{}{:0>2d}/'.format(dir_path, month)]
    else:
        month_list = []
        ftp.retrlines('NLST', month_list.append)
        dir_path_list = ['{}{}/'.format(dir_path, m) for m in month_list]
    
    file_url = []
    for month_dir in dir_path_list:
        ftp.cwd(month_dir.split('/')[-2])
        file_name = []
        ftp.retrlines('NLST', file_name.append)
        for f_n in file_name:
            file_url.append('{}{}{}'.format(base_url, month_dir, f_n))
        ftp.cwd('../')
    
    ftp.quit()
        
    return file_url

def download_from_url(file_url):
    print('downloading {}'.format(file_url))
    cmd = 'wget -c  {}'.format(file_url)
    os.system()
    pass


def batch_download_from_url(file_url_list):
    p = Pool(cpu_count())
    p.map(download_from_url, file_url_list)
    p.close()
    p.terminate()
    p.join()
    del p
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='manual to this script')
    parser.add_argument("--year", type=int, help='year>=2009', required=True)
    parser.add_argument("--month", type=int, default=0)
    args = parser.parse_args()
    _year = args.year
    _month = args.month

    if _month == 0:
        _month = None

    file_url = get_file_url(_year, _month)
    if file_url is None:
        print('input error')
        sys.exit()
    
    batch_download_from_url(file_url)
    pass

        