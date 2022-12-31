"""

    """

import json
from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo as GHDR
from tqdm import tqdm

class GDUrl :
    with open('gdu.json' , 'r') as f :
        gj = json.load(f)

    slf = gj['slf']
    src = gj['src']
    src0 = gj['src0']
    trg = gj['trg']

gu = GDUrl()

class Params :
    min_pct_ch = .9
    min_pct_num = .9
    min_pct_nan_col = .3
    min_pct_nan_row = .7

p = Params()

def clean_row_character(df) :
    df = df.fillna(0)
    df = df.reset_index(drop = True)

    for ind , _ in df.iterrows() :
        df.loc[ind , :] = df.loc[ind , :].astype('string')
        pct_num = df.loc[ind , :].str.isalpha().sum() / len(df.columns)
        if pct_num >= p.min_pct_ch :
            df = df.drop(index = ind)

    df = df.fillna(0)
    df = df[df.iloc[: , 0] != 0]
    df = df[~df.iloc[: , 0].str.contains('نام صندوق')]
    return df

def clean_column_character(df) :
    df1 = df.fillna(0)
    df1 = df1.reset_index(drop = True)

    for i , cn in enumerate(df1.columns) :
        if i != 0 :
            df1[cn] = df1[cn].astype('string')
            pct_num = df1[cn].str.fullmatch(r'\D+').sum() / len(df1)
            if pct_num >= p.min_pct_ch :
                df = df.drop(columns = cn)

    return df

def clean_column_numeric(df , column_num) :
    df1 = df.fillna(0)

    for i , cn in enumerate(df1.columns) :
        df1[cn] = df1[cn].astype('string')
        pct_chr = df1[cn].str.fullmatch(r'\D+').sum() / len(df1)
        if i <= column_num :
            df1[cn] = df1[cn].astype('string')
            pct_num = df1[cn].str.fullmatch(r'\d*\.?\d+').sum() / len(df1)
            if pct_chr >= 0.5 :
                break
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
    gds = GHDR(gu.src)

    ##
    gds.clone_overwrite()

    ##
    fps = list(gds.local_path.glob('*.xlsx'))
    fps = sorted(fps)

    ##
    df_list0_0 = []
    for i in tqdm(range(1 , 119)) :
        df = pd.read_excel(fps[i] , sheet_name = 1 , header = 0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        df = clean_column_numeric(df , 2)
        df = clean_row_character(df)
        df = clean_column_character(df)
        df = df.iloc[: , [0 , 1 , 2 , 3 , 4 , 5 , 6]]
        df.columns = range(len(df.columns))
        df['Date'] = fps[i].stem
        df_list0_0.append(df)

    ##
    df0_0 = pd.concat(df_list0_0 , axis = 0 , ignore_index = False)

    ##
    df_list0_1 = []
    for i in tqdm(range(119 , len(fps))) :
        df = pd.read_excel(fps[i] , sheet_name = 1 , header = 0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        df = clean_column_numeric(df , 2)
        df = clean_row_character(df)
        df = clean_column_character(df)
        df = df.iloc[: , [0 , 2 , 3 , 4 , 5 , 6 , 7]]
        df.columns = range(len(df.columns))
        df['Date'] = fps[i].stem
        df_list0_1.append(df)

    ##
    df0_1 = pd.concat(df_list0_1 , axis = 0 , ignore_index = False)

    df0 = pd.concat([df0_0 , df0_1] , axis = 0 , ignore_index = False)
    df1 = df0.copy()

    ##
    df1 = df1.fillna(0)
    df1 = df1[df1.iloc[: , 0] != 0]
    df1 = df1[df1.iloc[: , 0] != "0"]
    df1 = df1.reset_index(drop = True)

    df1 = df1.replace('_' , 0)
    df1 = df1.replace('-' , 0)

    ##
    df1.columns = [str(x) for x in df1.columns]
    df1['0'] = df1['0'].astype('string')
    df1['6'] = df1['6'].astype('string')

    df1.to_parquet('t.prq' , index = False)

    ##
    gds.rmdir()

    ##

##


if __name__ == "__main__" :
    main()
    print(Path(__file__) , 'Done!')

##
