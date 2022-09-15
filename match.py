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
    df = df[['SEORegisterNo' , 'Name' , 'InstituteKindId']]

    ##
    df1 = pd.read_parquet('t.prq')


##
if __name__ == "__main__" :
    main()
    print('Done!')
