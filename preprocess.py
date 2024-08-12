'''
This program is to extract, preprocess, and do simple analysis on the 
industrial census data.

Notice:
    1. only three columns were extracted in current version.
    2. current version only support data in ROC year 85, 90, and 95 (1996, 2001, 2006)

Author: KYK
Date: Jun.24.2024
'''

import pandas as pd
from os import listdir, getcwd
from os.path import isfile, join

class Processor():
    def __init__(self, upperlayer_folder="", folder="") -> None:
        self.upperlayer_folder = upperlayer_folder
        self.folder = folder
        self._extracted = False
        self.__data = None
        self.__warn_data_not_collected = "Not yet processed, please collect the data first."
    
    def __slash_maker(self):
        '''
        This program is to align the slashes of the provided data file path.
        (currently assumes user provide no mix of slashes and backslashes)

        Return
        ------
            Correctly slashed file path.
        '''
        uf = self.upperlayer_folder.strip("\\").strip("/")
        f = self.folder.strip("\\").strip("/")

        return "\\" + uf + "\\" + f + "\\"
    
    def __zd_convertor(self, zd_format_series: pd.Series) -> pd.Series:
        '''
        This zd format stands for zoned decimal format, developed by IBM.
        suggested reference: http://www.simotime.com/datazd01.htm
        Here is the brief explanation about how it works. There are positive
        and negative encoding from 0~9, and the signs is not for adding or 
        subtracting, it represents the sign of the entire figure.
        For example, 788D => 7884; 788} => -7880.
        
        Parameters
        ----------
        zd_format_series: pd.Series
            a series of numbers that is in the zd format.
        
        Return
        ------
            a series of numbers in the format we normally use.
        '''
        zd_map = {
            "{": [1, 0], "}": [-1, 0],
            "A": [1, 1], "J": [-1, 1],
            "B": [1, 2], "K": [-1, 2],
            "C": [1, 3], "L": [-1, 3],
            "D": [1, 4], "M": [-1, 4],
            "E": [1, 5], "N": [-1, 5],
            "F": [1, 6], "O": [-1, 6],
            "G": [1, 7], "P": [-1, 7],
            "H": [1, 8], "Q": [-1, 8],
            "I": [1, 9], "R": [-1, 9]
        }

        n = len(zd_format_series[0])

        front_digits = zd_format_series.str.slice(stop=n-1).astype('int64') * 10
        zd_code = zd_format_series.str.slice(start=n-1)
        zdc_mapped = zd_code.map(zd_map)

        # split the mapped thing
        tmp_df = pd.DataFrame(zdc_mapped.to_list(), columns=['sign','value'])

        return (front_digits + tmp_df['value']) * tmp_df['sign']

    def _extract(self, company_list: list) -> pd.DataFrame:
        '''
        The following are the column names and their position stated in the code.doc.
        (Our target columns in files "a" to "f" in year 85 and 90 share the same position and length.)
        85                | 90                  | 95
        x3103 (8)         | x310004 (12)        | scale (2)
        x3200 (14-17)     | x320000 (14-17)     | primary (3-6)
        x3613 (1139-1153) | x360013 (1069-1083) | x360019 (246-260)
        -----
        Parameters
        company_list: list. 
            the raw data read straight from the file, each element represents a company.
            and each element is a long string of info of the company.

        Return
        ------
            a dataframe with extracted and processed data of designated columns.
        '''
        comps = pd.Series(company_list)

        if self.folder[:2] == "85":
            scale = comps.str.slice(start=7, stop=8)
            primary = comps.str.slice(start=13, stop=17)
            primary_short = primary.str.slice(start=0, stop=2)
            asset = self.__zd_convertor(comps.str.slice(start=1138, stop=1153))
        elif self.folder[:2] == "90":
            scale = comps.str.slice(start=11, stop=12)
            primary = comps.str.slice(start=13, stop=17)
            primary_short = primary.str.slice(start=0, stop=2)
            asset = comps.str.slice(start=1068, stop=1083).astype('int64')
        elif self.folder[:2] == "95":
            scale = comps.str.slice(start=1, stop=2)
            primary = comps.str.slice(start=2, stop=6)
            primary_short = primary.str.slice(start=0, stop=2)
            asset = self.__zd_convertor(comps.str.slice(start=245, stop=260))

        # concat with axis=1 means each series will turn into a column
        return pd.concat([scale, primary, primary_short, asset], axis=1).reset_index(drop=True)

    def collect_data(self) -> None:
        '''
        This method will collect all the data from the specified folder.
        '''
        if not self.folder:
            print("Please specify a folder")
            return
        elif self.folder[:2] not in ["85", "90", "95"]:
            print("Destined year not yet finished.")
            return
        
        print(f'\nStart collection data from folder "{self.folder}"...')
        path = getcwd() + self.__slash_maker() # it assumes the code file is in the same path as the overall data folder

        # list all the files without extension (since the data are the files without extensions)
        target_files = [f for f in listdir(path) if isfile(join(path, f)) and "." not in f]
        data_out = self.__data # initiate data_out

        for cur_file in target_files:
            with open(join(path, cur_file), 'rb') as f:
                text = f.read()
                text = text.decode('ascii', 'ignore') # there are some bytes not encoded as ascii...
                companies_raw = text.split("\r\n") # 'rb' => "\r\n"; 'r' => "\n"

                # I found there is an empty element in the end, currently simply remove the last one.
                # TODO: might want to remove all empty from the back.
                companies_raw = companies_raw[:-1]

                # default axis=0, meaning we are stacking the data for both DataFrame and Series.
                data_out = pd.concat([data_out, self._extract(companies_raw)]).reset_index(drop=True)
                print(f"File {cur_file} finished.")

        data_out.columns = ['scale', 'primary', 'roc_sic', 'asset']

        self._extracted = True
        self.__data = data_out
        print(f'Data collection from folder "{self.folder}" is completed.')

        return

    def apply_conditions(self, keep_original_data=False, **conds) -> None:
        if not self._extracted:
            print(self.__warn_data_not_collected)
            return
        
        if keep_original_data:
            # make a copy
            # self.__data_origin = self.__data
            pass

        # TODO: still thinking how to automatically apply conditions. Currently manual.
        cond1 = (self.__data['scale'] != "8")

        self.__data = self.__data[cond1].reset_index(drop=True)

    def sic_mapping(self) -> None:
        '''
        This method maps the ROC SIC into ISIC.
        Notice that the mapping is hard-coded, ROC year 85: ROCSIC6 etc, 

        Will add a new column named "isic" after this method was called.
        '''
        if not self._extracted:
            print(self.__warn_data_not_collected)
            return  

        # read the mapping table in
        sic_tb = pd.read_excel("ISIC_to_ROCSIC.xlsx", sheet_name="Sheet2")

        # force all elements as string, so that latter when splitting won't 
        # turn single int into NaN
        if self.folder[:2] == "85":
            code_using = sic_tb['ROCSIC_6'].astype('string')
        elif self.folder[:2] == "90":
            code_using = sic_tb['ROCSIC_7'].astype('string')
        elif self.folder[:2] == "95":
            code_using = sic_tb['ROCSIC_8'].astype('string')
        
        # make the mapping with ROCSIC as key (in the table read in, ISIC is the key)
        # TODO: 不知道有沒有更好的辦法來對應這種數字但有可能變成前面帶0的文字
        rocsic_to_isic = {}
        code_using = code_using.str.split(",")
        for i in range(sic_tb.shape[0]):
            for k in code_using[i]:
                if k.isnumeric() and int(k) < 10:
                    k = "0" + k
                rocsic_to_isic[k] = sic_tb['ISIC_Rev3'][i]

        # print(rocsic_to_isic) # for checking the mapping result.
        self.__data['isic'] = self.__data['roc_sic'].map(rocsic_to_isic).fillna(rocsic_to_isic['Else'])

    def some_analysis(self) -> None:
        by_rocsic = self.__data.groupby(['roc_sic'])['asset'].sum().reset_index()
        by_isic = self.__data.groupby(['isic'])['asset'].sum().reset_index()

        print(by_isic)
        # by_rocsic.to_csv(f'{self.folder[:3]}_groupby_rocsic_asset.csv', index=False)
        # by_isic.to_csv(f'{self.folder[:3]}_groupby_isic_asset.csv', index=False)

    def show_data(self) -> None:
        if not self._extracted:
            print(self.__warn_data_not_collected)
            return
        print(self.__data)

    def get_data(self) -> pd.DataFrame:
        if not self._extracted:
            print(self.__warn_data_not_collected)

        return self.__data
    
    def output_CSV(self) -> None:
        if not self._extracted:
            print(self.__warn_data_not_collected)
        
        self.__data.to_csv(f'{self.folder[:3]}_extracted.csv', index=False)


def main():
    path = "" # 想一下要不要改成程式不管放哪都可以透過這個path抓資料
    folders = ["85年AA290005", "90年AA290006", "95年AA290007"]
    # folders = [folders[2]]

    for folder in folders:
        p = Processor(upperlayer_folder="工商普查原始", folder=folder)
        p.collect_data()
        p.sic_mapping()

        conds = {
            'scale': ["!=", "8"]
        }
        p.apply_conditions(**conds)
        p.some_analysis()
        # p.output_CSV()


if __name__ == "__main__":
    main()