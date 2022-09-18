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
    df1.columns = ['Name' , 'Value' , 'Share' , 'Bond' , 'Deposit' , 'Cash' ,
                   'Other' , 'JMonths']
    ##
    Final = df.merge(df1 , on = ["Name"])
    Final = Final.sort_values(by = ["JMonths"])

    # Final = Final.sort_values(by = ["InstituteKind" , 'Name' , "JMonths"])
    Final = Final[
        ['JMonths' , 'SEORegisterNo' , 'InstituteKind' , 'Value' , 'Share' ,
         'Bond' , 'Deposit' , 'Cash' , 'Other']]

    Final.SEORegisterNo = Final.SEORegisterNo.astype('string')
    Final[['Share' , 'Bond' , 'Deposit' , 'Cash' , 'Other']] = Final[
        ['Share' , 'Bond' , 'Deposit' , 'Cash' , 'Other']].astype('float')
    Final['Sum'] = Final['Share'] + Final['Bond'] + Final['Deposit'] + Final[
        'Cash'] + Final['Other']
    Final = Final[Final["Sum"] <= 110]
    Final = Final.reset_index(drop = True)

    Final.columns
    ##
    Final.InstituteKind.unique()
    ##
    Result = Final[Final["InstituteKind"] == "در اوراق بهادار با درآمد ثابت"]
    Result = Result.groupby(["JMonths"]).mean()
    Result = Result.reset_index()
    Result = Result[Result['Share'] <= 100]
    ##
    import pandas_bokeh

    Result.plot(kind = 'line' ,
                x = 'JMonths' ,
                y = ['Share' , 'Bond' , 'Deposit' , 'Cash' , 'Other'])
    # Result.plot_bokeh.line(x = ['JMonths'] ,
    #                        y = ['Share' , 'Bond' , 'Deposit' , 'Cash' ,
    #                             'Other'] ,
    #                        figsize = (900 , 500))

    plt.show()

    ##


if __name__ == "__main__" :
    main()
    print('Done!')
