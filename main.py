"""

    """

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



    ##

    ##

##
if __name__ == "__main__":
    main()
    print('Done!')