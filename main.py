"""

    """
import pandas as pd
import json
from pathlib import Path

import pandas as pd
from githubdata import GithubData

class GDUrl:
    with open('gdu.json' , 'r') as f:
        gj = json.load(f)

    src = gj['src']

gu = GDUrl()

def main():
    pass

    ##
    gds = GithubData(gu.src)
    ##
    gds.overwriting_clone()
    ##
    fps = list(gds.local_path.glob('*.xlsx'))
    ##
    df = pd.DataFrame()
    df['xlp'] = fps
    fps[50]
    df1 = pd.read_excel(fps[50], sheet_name=1, header=0)
    df1



    ##

    ##

##
if __name__ == "__main__":
    main()
    print('Done!')