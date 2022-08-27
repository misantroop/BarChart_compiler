'''
    Compile BarChart CSV files, remove duplicate dates, use ISO datetime
    
    File structure:
    ---CONTRACT_DIR
    ---------------FILE1
    ---------------FILE2
    ---compile_contract.py
'''

import os, re, csv
import pandas as pd
import numpy as np

class DataHandler():
    def __init__(self):
        super().__init__()


    def get_dirs(self):
        return [dir_ for _, dir_, _ in os.walk('.') if dir_][0]


    def get_files(self, subdir):
        return [files for _, _, files in os.walk(subdir) if files]

    '''
    def format_lines(self, loc, skip=2):
        result = []
        with open(loc, 'r') as file_open:
            for n, line in enumerate(file_open):
                if n == 1:
                    header = line.rstrip().split(',')

                if n >= skip:
                    cell = re.sub(r'"(.+?)"',
                                lambda x: re.sub(',|"', '', x.group()),
                                line.rstrip()
                    ).split(',')

                    result.append(dict(zip(header, cell)))
        return header, result


    def write_output(self, name, data, header):
        print(f'Writing output to {name}')
        with open(name, 'w+') as file_:
            dict_write = csv.DictWriter(file_, fieldnames=header)
            dict_write.writerows(data)


    def run_very_slow(self):
        from iteration_utilities import unique_everseen
        for dir_ in self.get_dirs():
            for files in self.get_files(dir_):
                print(f"Processing contract {dir_}")

                output = []
                for n, file_ in enumerate(files):
                    print(f"Segment {n+1} of {len(files)}")
                    header, result = self.format_lines(loc=os.sep.join((dir_, file_)))
                    output = output + result

                self.write_output(name=f"{dir_}.csv", data=list(unique_everseen(output)), header=header)
    '''

    def format_pandas(self, loc):
        df = pd.read_csv(loc,
                         skiprows=1,
                         quotechar='"',
                         thousands=',',
                         low_memory=False,
                         keep_default_na=False,
                         na_values='',
                         engine='c',
                         infer_datetime_format=True,
                         dtype = {
                                    'Open': np.float64, 'High': np.float64, 'Low': np.float64, 'Close': np.float64,
                                    'Volume': np.float32, 'Symbol': 'string'
                         }
             ).dropna().set_index(['Date'])

        df['Volume'] = df['Volume'].astype(np.uint32)
        return df


    def run(self):
        for dir_ in self.get_dirs():
            for files in self.get_files(dir_):

                ''' Hidden dir '''
                if dir_[:1] == '.':
                    continue

                print(f"Processing contract {dir_}")
                out = pd.DataFrame()

                for n, file_ in enumerate(files):
                    print(f"Segment {n+1} of {len(files)} ({file_})")
                    segment = self.format_pandas(loc=os.sep.join((dir_, file_)))
                    out = pd.concat([out, segment])

                print('Fixing index dates to uniform')
                out.index = pd.to_datetime(out.index, errors='coerce', infer_datetime_format=True)
                out.index = out.index.strftime('%Y-%m-%d %H:%M')

                #print('Removing thousand separators')
                #select = ['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
                #out[select] = out[select].replace(',','', regex=True)

                #output = output[~output.reset_index().duplicated().values]
                print('Dropping duplicates')
                out = out.reset_index().drop_duplicates().set_index(['Date']).sort_index()

                print('Writing output to CSV')
                out = out.to_csv(f"{dir_}.csv",
                                 columns=['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume'])


dh = DataHandler()
dh.run()
