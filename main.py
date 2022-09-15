"""

    """

import json
import pandas as pd
from githubdata import GithubData
from tqdm import tqdm


class GDUrl :
    with open('gdu.json' , 'r') as f :
        gj = json.load(f)

    src = gj['src']
    src0 = gj['src0']

gu = GDUrl()

class Params :
    min_pct_ch = .7
    min_pct_num = .7
    min_pct_nan_col = .3
    min_pct_nan_row = .7

p = Params()

def clean_row_character(df) :
    df1 = df.fillna(0)
    df1 = df1.reset_index(drop = True)

    for ind , _ in df1.iterrows() :
        df1.loc[ind , :] = df1.loc[ind , :].astype('string')
        pct_num = df1.loc[ind , :].str.isalpha().sum() / len(df1.columns)
        if pct_num >= p.min_pct_ch :
            df = df.drop(index = ind)

    return df

def clean_column_character(df) :
    df1 = df.fillna(0)
    df1 = df1.reset_index(drop = True)

    for i , cn in enumerate(df1.columns) :
        if i != 0:
            df1[cn] = df1[cn].astype('string')
            pct_num = df1[cn].str.isalpha().sum() / len(df1)
            if pct_num >= p.min_pct_ch :
                df = df.drop(columns = cn)

    return df

def clean_column_numeric(df) :
    df1 = df.fillna(0)

    for i , cn in enumerate(df1.columns) :
        if i <= 2 :
            df1[cn] = df1[cn].astype('string')
            pct_num = df1[cn].str.isnumeric().sum() / len(df1)
            if pct_num >= p.min_pct_num :
                df = df.drop(columns = cn)

    return df

def clean_row_nan(df) :
    pct_null = df.T.isnull().sum() / len(df.T)
    missing_features = pct_null[pct_null > p.min_pct_nan_col].index
    df = df.drop(missing_features , axis = 0)
    return df

def clean_column_nan(df) :
    pct_null = df.isnull().sum() / len(df)
    missing_features = pct_null[pct_null > p.min_pct_nan_row].index
    df = df.drop(missing_features , axis = 1)
    return df

def main() :
    pass

    ##
    gds = GithubData(gu.src)
    ##
    gds.overwriting_clone()
    ##
    fps = list(gds.local_path.glob('*.xlsx'))
    ##
    df_list0 = []
    for i in tqdm(range(1, 121)):
        df = pd.read_excel(fps[i], sheet_name=1, header=0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        df = clean_column_numeric(df)
        df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6]]
        df = df.T.reset_index(drop=True).T
        df['Date'] = fps[i].stem
        df_list0.append(df)

    df0 = pd.concat(df_list0, axis=0, ignore_index=False)
    df0 = df0.reset_index(drop=True)
    ##
    df = pd.read_excel(fps[136] , sheet_name = 1 , header = 0)
    df = df.fillna(0)
    df.dtypes
    ##
    df_list0 = []
    for i in tqdm(range(1,len(fps))) :
        df = pd.read_excel(fps[i] , sheet_name = 1 , header = 0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        # df.iloc[:,0] = df.iloc[:,0].astype(int)
        df = clean_column_numeric(df)
        df = clean_row_character(df)
        df = clean_column_character(df)
        df = df.iloc[: , [0 , 1 , 2 , 3 , 4 , 5 , 6]]
        df.columns = range(len(df.columns))
        df['Date'] = fps[i].stem
        df_list0.append(df)

    df0 = pd.concat(df_list0 , axis = 0 , ignore_index = False)
    df0 = df0.fillna(0)
    df0 = df0[df0.iloc[:,0]!=0]
    df0 = df0.reset_index(drop = True)
    ##
    df_list1 = []
    for i in tqdm(range(121 , len(fps))) :
        df = pd.read_excel(fps[i] , sheet_name = 1 , header = 0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        df = clean_column_numeric(df)
        df = df.iloc[: , [0 , 2 , 3 , 4 , 5 , 6 , 7]]
        df = df.T.reset_index(drop = True).T
        df['Date'] = fps[i].stem
        df_list1.append(df)

    df1 = pd.concat(df_list1 , axis = 0 , ignore_index = False)
    df1 = df1.reset_index(drop = True)
    ##
    df = pd.concat([df0 , df1] , axis = 0 , ignore_index = False)
    df = clean_row_character(df)
    # df.loc[0 , :]
    ##

##
if __name__ == "__main__" :
    main()
    print('Done!')
