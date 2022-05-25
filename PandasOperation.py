import pandas as pd


class PandasOperation:
    def __init__(self):
        pass

    def read_downloaded_file(self, file_name):
        print('file to read')
        df = pd.read_csv(file_name)
        return df

    def write_csv_date(self, file_to_write, data):
        print('Method Called: write_csv_date')
        data.to_csv('upload_'+file_to_write,index=False)