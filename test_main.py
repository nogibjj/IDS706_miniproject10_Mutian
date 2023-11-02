from main import *
import datetime
def test_loaddata():
    df = loadDf("./taker_buy_sell_volume.csv")
    ## check df type is pl.DataFrame
    assert isinstance(df, pl.DataFrame)
    ## check df shape
    assert df.shape==(13768,4)
    print(df)
def test_describedata():
    df = loadDf("./taker_buy_sell_volume.csv")
    descriptive_df = describeData(df)
    ## check descriptive_df shape
    assert descriptive_df.shape==(5,5)
    ## check mean std min max median included in descriptive info
    for stat in ["mean","std","min","max","median"]:
        assert stat in descriptive_df[:,0]
    print(descriptive_df)

