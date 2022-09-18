"""

"""
import pandas as pd
from ex_data import gu
from githubdata import GithubData


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
    df1.columns = ['Name' , 'Share' , 'Bond' , 'Deposit' , 'Cash' ,
                   'Other' , 'JMonths']
    ##
    Final = df.merge(df1 , on = ["Name"])
    Final = Final.sort_values(by = ["InstituteKind" , 'Name' , "JMonths"])
    Final = Final[
        ['JMonths' , 'SEORegisterNo', 'Share' , 'Bond' , 'Deposit' ,
         'Cash' , 'Other']]
    # Final.JMonths = Final.JMonths.str.replace('-' , '').astype(int)
    # Final = Final.astype(float)
    # Final.SEORegisterNo = Final.SEORegisterNo.astype('string')
    Final['Sum'] = Final['Share'] + Final['Bond'] + Final['Deposit'] + Final[
        'Cash'] + Final['Other']
    Final.dtypes

    ##


if __name__ == "__main__" :
    main()
    print('Done!')
