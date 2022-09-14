"""

    """
##
import json
import pandas as pd
from githubdata import GithubData
from tqdm import tqdm
##
class GDUrl:
    with open('gdu.json' , 'r') as f:
        gj = json.load(f)

    src = gj['src']

gu = GDUrl()
##
def clean_column_numeric(X):
    df = X.fillna(0)
    missing_numeric_features = []
    for i in range(len(df.columns)):
        pct_numeric = df[df.columns[i]].astype('string').str.isnumeric().sum() / len(df)
        if pct_numeric >= 0.8:
            missing_numeric_features.append(i)
        else:
            continue

    X = df.drop(df.columns[missing_numeric_features], axis=1)
    return X
 ##
def clean_row_nan(X):
    pct_null = X.T.isnull().sum() / len(X.T)
    missing_features = pct_null[pct_null > 0.3].index
    X = X.drop(missing_features, axis=0)
    return X

def clean_column_nan(X):
    pct_null = X.isnull().sum() / len(X)
    missing_features = pct_null[pct_null > 0.7].index
    X = X.drop(missing_features, axis=1)
    return X
 ##
def main():
    pass

    ##
    gds = GithubData(gu.src)
    ##
    gds.overwriting_clone()
    ##
    fps = list(gds.local_path.glob('*.xlsx'))
    ##
    i=93
    df_list=[]
    for i in tqdm(range(120)):
        df = pd.read_excel(fps[i], sheet_name=1, header=0)
        df = clean_row_nan(df)
        df = clean_column_nan(df)
        df = clean_column_numeric(df)
        df = df.iloc[:, [0,1,2,3,4,5,6]]
        df = df.T.reset_index(drop=True).T
        df['Date'] = fps[i]
        # df['Date'] = df['Date'].str.extract(r'(\d+)', expand=False)
        print(len(df.columns))
        df_list.append(df)


    df = pd.concat(df_list, axis=0, ignore_index=False)


##

    ##
    for a in tqdm(filenames):
        for h in reversed(H):
            for i in I:
                for k in reversed(K):

                    try:
                        # print(k,h)

                        df = pd.read_excel(a, sheet_name=i, header=0)
                        pct_null = df.isnull().sum() / len(df)
                        missing_features = pct_null[pct_null > k].index
                        df.drop(missing_features, axis=1, inplace=True)

                        perc = 80.0
                        min_count = int(((100 - perc) / 100) * df.shape[1] + 1)
                        df = df.dropna(axis=0, thresh=min_count)

                        pct_zero = df[df == 0].count(axis=0) / len(df.index)
                        missing_features_zero = pct_zero[pct_zero > h].index
                        df.drop(missing_features_zero, axis=1, inplace=True)

                        x = a.split("-")
                        df['Fund'] = x[0]
                        df['Period'] = x[1]
                        df['Managment'] = x[2].replace(".xlsx", '')

                        columns = ['Name', 'StartNum', 'StartCost', 'StartValue',
                                   'BuyNum', 'BuyCost', 'SellNum', 'SellCost', 'EndNum',
                                   'Market price', 'EndCost', 'EndValue', 'PercentOfAsset'
                            , 'Period', 'Fund', 'Managment']

                        df.columns = columns
                        df = df[~df.Name.str.contains(''.join("نام شرکت"), na=True)]

                        dfList.append(df)
                        # print(a)
                        b.append(a)
                        break
                    except:
                        continue
                else:
                    continue
                break
            else:
                continue
            break
        else:
            print(a)
            continue

    df = pd.concat(dfList, axis=0, ignore_index=False)

    df = df.fillna(0)
    ##

##
if __name__ == "__main__":
    main()
    print('Done!')