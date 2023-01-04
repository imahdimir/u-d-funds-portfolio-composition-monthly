"""

    """

from pathlib import Path

import pandas as pd
from githubdata import GitHubDataRepo as GHDR
from mirutil.str import normalize_completley_and_rm_all_whitespaces as ncr

from ex_data import gu

class SeoRegDataCols :
    reg = 'SEORegisterNo'
    name = 'Name'
    kind = 'InstituteKind'

sc = SeoRegDataCols()

class Cols :
    name = 'Name'
    val = 'Value'
    eqt = 'Equity'
    bond = 'Bond'
    depo = 'Deposit'
    cash = 'Cash'
    other = 'Other'
    jm = 'JMonth'
    wn = 'WName'

c = Cols()

def main() :
    pass

    ##
    gds0 = GHDR(gu.src0)

    ##
    gds0.clone_overwrite()

    ##
    df = gds0.read_data()

    ##
    c2k = {
            sc.reg  : None ,
            sc.name : None ,
            sc.kind : None ,
            }

    df = df[c2k.keys()]

    ##
    df1 = pd.read_parquet('t.prq')

    ##
    cols_naming_order = {
            c.name  : None ,
            c.val   : None ,
            c.eqt   : None ,
            c.bond  : None ,
            c.depo  : None ,
            c.cash  : None ,
            c.other : None ,
            c.jm    : None ,
            }

    df1.columns = cols_naming_order.keys()

    ##
    df1[c.wn] = df1[c.name].apply(ncr)

    ##
    assert df1[c.wn].nunique() == df1[c.name].nunique()

    ##
    df[c.wn] = df[c.name].apply(ncr)
    df = df.rename(columns = {
            c.name : 'RefName'
            })

    ##
    dff = df1.merge(df , how = "left")

    ##
    msk = dff[sc.reg].isna()

    df2 = dff[msk]

    err_msg = "There are missing SEO reg numbers or kinds, the SEO register data is not complete & must be updated."
    assert len(df2) == 0 , err_msg

    ##
    df2 = df2.drop_duplicates(subset = [c.name] , keep = 'first')

    ##
    # for now
    dff = dff.dropna()

    ##
    dff = dff.sort_values(c.jm)

    ##
    reorder = {
            c.jm    : None ,
            sc.reg  : None ,
            sc.kind : None ,
            c.val   : None ,
            c.eqt   : None ,
            c.bond  : None ,
            c.depo  : None ,
            c.cash  : None ,
            c.other : None ,
            }

    dff = dff[reorder.keys()]

    ##
    dff[sc.reg] = dff[sc.reg].astype(int)
    dff[sc.reg] = dff[sc.reg].astype('string')

    ##
    float_cols = {
            c.val   : None ,
            c.eqt   : None ,
            c.bond  : None ,
            c.depo  : None ,
            c.cash  : None ,
            c.other : None ,
            }

    for col in float_cols.keys() :
        dff[col] = dff[col].str.replace('***' , '0' , regex = False)

    ##
    dff[list(float_cols.keys())] = dff[list(float_cols.keys())].astype('float')

    ##
    pct_cols = {
            c.eqt   : None ,
            c.bond  : None ,
            c.depo  : None ,
            c.cash  : None ,
            c.other : None ,
            }

    dff['Sum'] = dff[list(pct_cols.keys())].agg('sum' , axis = 1)

    ##
    msk = dff["Sum"] <= 102
    msk &= dff["Sum"] >= 98

    dff = dff[msk]

    ##
    do = dff.drop(columns = ['Sum'])

    ##
    gdo = GHDR(gu.trg)
    gdo.clone_overwrite()

    ##
    dfp = gdo.local_path / 'data.prq'
    do.to_parquet(dfp , index = False)

    ##
    msg = 'updated by: '
    msg += gu.slf

    ##
    gdo.commit_and_push(msg)

    ##
    gdo.rmdir()
    gds0.rmdir()

    ##
    do.to_parquet('t1.prq' , index = False)

    ##
    do[sc.kind].unique()

    ##
    msk = do[sc.kind] == 'در اوراق بهادار با درآمد ثابت'
    do = do[msk]

    ##
    cou = do.groupby([c.jm])[sc.reg].nunique()
    cou.to_excel('coun.xlsx')

    ##
    val = do.groupby([c.jm])[c.val].sum() / 10 ** 4
    val.to_excel('val.xlsx')

    ##

    pct_cols = {
            c.eqt   : None ,
            c.bond  : None ,
            c.depo  : None ,
            c.cash  : None ,
            c.other : None ,
            }

    for col in pct_cols.keys() :
        do[col + 'Val'] = do[col] * do[c.val] / 100

    ##
    val_cols = [x + 'Val' for x in pct_cols.keys()]

    ##
    comp = do.groupby([c.jm])[val_cols].sum() / 10 ** 4
    comp.to_excel('comp.xlsx')

##


if __name__ == "__main__" :
    main()
    print(Path(__file__) , 'Done!')

##
