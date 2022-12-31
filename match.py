"""

"""

import pandas as pd
from githubdata import GithubData

from ex_data import gu

def main() :
    pass

    ##
    gds0 = GithubData(gu.src0)
    ##
    gds0.overwriting_clone()
    ##
    df = gds0.read_data()
    ##
    df = df[['SEORegisterNo' , 'Name' , 'InstituteKind']]
    ##
    df1 = pd.read_parquet('t.prq')
    df1.columns = ['Name' , 'Value' , 'Share' , 'Bond' , 'Deposit' , 'Cash' ,
                   'Other' , 'JMonth']
    ##
    Final = df.merge(df1 , on = ["Name"])
    Final = Final.sort_values(by = ["JMonth"])

    # Final = Final.sort_values(by = ["InstituteKind" , 'Name' , "JMonths"])
    Final = Final[
        ['JMonth' , 'SEORegisterNo' , 'InstituteKind' , 'Value' , 'Share' ,
         'Bond' , 'Deposit' , 'Cash' , 'Other']]

    Final.SEORegisterNo = Final.SEORegisterNo.astype('string')
    Final[['Value' , 'Share' , 'Bond' , 'Deposit' , 'Cash' , 'Other']] = Final[
        ['Value' , 'Share' , 'Bond' , 'Deposit' , 'Cash' , 'Other']].astype(
            'float')
    Final['Sum'] = Final['Share'] + Final['Bond'] + Final['Deposit'] + Final[
        'Cash'] + Final['Other']
    Final = Final[Final["Sum"] <= 110]
    Final = Final.reset_index(drop = True)

    ##
    cols = {
            'JMonth'        : None ,
            'SEORegisterNo' : None ,
            'Share'         : None ,
            'Bond'          : None ,
            'Deposit'       : None ,
            'Cash'          : None ,
            'Other'         : None ,
            }

    do = Final[cols.keys()]
    ##
    gdo = GithubData(gu.trg)
    gdo.overwriting_clone()
    ##
    dfp = gdo.local_path / 'data.prq'
    do.to_parquet(dfp , index = False)
    ##
    msg = 'Update data by: '
    msg += gu.slf

    ##
    gdo.commit_and_push(msg)

    ##

    gdo.rmdir()
    gds0.rmdir()

##


if __name__ == "__main__" :
    main()
    print(Path(__file__) , 'Done!')

##
