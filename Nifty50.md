```python
import sqlalchemy
import yfinance as yf
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import datetime as dt
```


```python
data =[]
```


```python
wiki = "https://en.wikipedia.org/wiki/BSE_SENSEX"
data = pd.read_html(wiki)[1].Symbol.to_list()
data
```




    ['ADANIPORTS.BO',
     'ASIANPAINT.BO',
     'AXISBANK.BO',
     'BAJFINANCE.BO',
     'BAJAJFINSV.BO',
     'BHARTIARTL.BO',
     'HCLTECH.BO',
     'HDFCBANK.BO',
     'HINDUNILVR.BO',
     'ICICIBANK.BO',
     'INDUSINDBK.BO',
     'INFY.BO',
     'ITC.BO',
     'JSWSTEEL.BO',
     'KOTAKBANK.BO',
     'LT.BO',
     'M&M.BO',
     'MARUTI.BO',
     'NESTLEIND.BO',
     'NTPC.BO',
     'POWERGRID.BO',
     'RELIANCE.BO',
     'SBIN.BO',
     'SUNPHARMA.BO',
     'TCS.BO',
     'TATAMOTORS.BO',
     'TATASTEEL.BO',
     'TECHM.BO',
     'TITAN.BO',
     'ULTRACEMCO.BO']




```python
stock_data = []
for ticker in data:
   stock_data.append(yf.download(ticker).reset_index())
```

    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    [*********************100%***********************]  1 of 1 completed
    


```python

```


```python
engine = sqlalchemy.create_engine("sqlite:///BSE_SENSEX.db")
for frame,symbol in zip(stock_data,data):
    frame.to_sql(symbol,engine,index=False)
    
```


```python
pd.read_sql(f'Select date,"Adj Close" as "INFY.BO"  from "INFY.BO"',engine)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>INFY.BO</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2000-01-03 00:00:00.000000</td>
      <td>-1.793299</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2000-01-04 00:00:00.000000</td>
      <td>-1.934303</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2000-01-05 00:00:00.000000</td>
      <td>-1.779561</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2000-01-06 00:00:00.000000</td>
      <td>-1.637201</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2000-01-07 00:00:00.000000</td>
      <td>-1.506228</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>6113</th>
      <td>2024-09-04 00:00:00.000000</td>
      <td>1922.050049</td>
    </tr>
    <tr>
      <th>6114</th>
      <td>2024-09-05 00:00:00.000000</td>
      <td>1933.000000</td>
    </tr>
    <tr>
      <th>6115</th>
      <td>2024-09-06 00:00:00.000000</td>
      <td>1902.199951</td>
    </tr>
    <tr>
      <th>6116</th>
      <td>2024-09-09 00:00:00.000000</td>
      <td>1894.699951</td>
    </tr>
    <tr>
      <th>6117</th>
      <td>2024-09-10 00:00:00.000000</td>
      <td>1912.900024</td>
    </tr>
  </tbody>
</table>
<p>6118 rows × 2 columns</p>
</div>




```python
df = pd.DataFrame()
```


```python
dfs = []

for name in data:
    query = f'SELECT date, "Adj Close" AS "{name}" FROM "{name}" WHERE date > "2014-01-01"'
    df = pd.read_sql(query, engine)
    if df.empty:
        print(f"No data returned for {name}")
    else:
        dfs.append(df)



df = pd.concat(dfs, axis=0)
df = df.reset_index(drop=True)
df 
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>ADANIPORTS.BO</th>
      <th>ASIANPAINT.BO</th>
      <th>AXISBANK.BO</th>
      <th>BAJFINANCE.BO</th>
      <th>BAJAJFINSV.BO</th>
      <th>BHARTIARTL.BO</th>
      <th>HCLTECH.BO</th>
      <th>HDFCBANK.BO</th>
      <th>HINDUNILVR.BO</th>
      <th>...</th>
      <th>POWERGRID.BO</th>
      <th>RELIANCE.BO</th>
      <th>SBIN.BO</th>
      <th>SUNPHARMA.BO</th>
      <th>TCS.BO</th>
      <th>TATAMOTORS.BO</th>
      <th>TATASTEEL.BO</th>
      <th>TECHM.BO</th>
      <th>TITAN.BO</th>
      <th>ULTRACEMCO.BO</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2014-01-01 00:00:00.000000</td>
      <td>148.608017</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2014-01-02 00:00:00.000000</td>
      <td>145.229492</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2014-01-03 00:00:00.000000</td>
      <td>142.850266</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2014-01-06 00:00:00.000000</td>
      <td>143.516418</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2014-01-07 00:00:00.000000</td>
      <td>138.995834</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>78345</th>
      <td>2024-09-04 00:00:00.000000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>11590.500000</td>
    </tr>
    <tr>
      <th>78346</th>
      <td>2024-09-05 00:00:00.000000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>11537.250000</td>
    </tr>
    <tr>
      <th>78347</th>
      <td>2024-09-06 00:00:00.000000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>11426.799805</td>
    </tr>
    <tr>
      <th>78348</th>
      <td>2024-09-09 00:00:00.000000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>11497.099609</td>
    </tr>
    <tr>
      <th>78349</th>
      <td>2024-09-10 00:00:00.000000</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>11541.799805</td>
    </tr>
  </tbody>
</table>
<p>78350 rows × 31 columns</p>
</div>




```python
df = df.groupby("Date").sum()
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ADANIPORTS.BO</th>
      <th>ASIANPAINT.BO</th>
      <th>AXISBANK.BO</th>
      <th>BAJFINANCE.BO</th>
      <th>BAJAJFINSV.BO</th>
      <th>BHARTIARTL.BO</th>
      <th>HCLTECH.BO</th>
      <th>HDFCBANK.BO</th>
      <th>HINDUNILVR.BO</th>
      <th>ICICIBANK.BO</th>
      <th>...</th>
      <th>POWERGRID.BO</th>
      <th>RELIANCE.BO</th>
      <th>SBIN.BO</th>
      <th>SUNPHARMA.BO</th>
      <th>TCS.BO</th>
      <th>TATAMOTORS.BO</th>
      <th>TATASTEEL.BO</th>
      <th>TECHM.BO</th>
      <th>TITAN.BO</th>
      <th>ULTRACEMCO.BO</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-01-01 00:00:00.000000</th>
      <td>148.608017</td>
      <td>457.888153</td>
      <td>237.651199</td>
      <td>132.478210</td>
      <td>71.218613</td>
      <td>317.704865</td>
      <td>208.925980</td>
      <td>291.359375</td>
      <td>482.071930</td>
      <td>167.935471</td>
      <td>...</td>
      <td>27.960808</td>
      <td>399.871674</td>
      <td>136.612396</td>
      <td>533.455017</td>
      <td>784.349548</td>
      <td>370.850983</td>
      <td>2.492016</td>
      <td>331.822601</td>
      <td>219.604156</td>
      <td>1694.514771</td>
    </tr>
    <tr>
      <th>2014-01-02 00:00:00.000000</th>
      <td>145.229492</td>
      <td>444.742828</td>
      <td>234.155655</td>
      <td>132.696854</td>
      <td>68.394867</td>
      <td>308.764648</td>
      <td>207.821655</td>
      <td>287.766968</td>
      <td>474.442108</td>
      <td>164.522919</td>
      <td>...</td>
      <td>28.706987</td>
      <td>393.864899</td>
      <td>134.847961</td>
      <td>534.852844</td>
      <td>787.481628</td>
      <td>367.932861</td>
      <td>2.459477</td>
      <td>329.643463</td>
      <td>215.322479</td>
      <td>1661.554199</td>
    </tr>
    <tr>
      <th>2014-01-03 00:00:00.000000</th>
      <td>142.850266</td>
      <td>448.315430</td>
      <td>231.929550</td>
      <td>131.931625</td>
      <td>68.465347</td>
      <td>309.470459</td>
      <td>209.390961</td>
      <td>290.614594</td>
      <td>476.591705</td>
      <td>163.283371</td>
      <td>...</td>
      <td>28.256466</td>
      <td>389.207825</td>
      <td>132.804947</td>
      <td>540.491272</td>
      <td>809.223450</td>
      <td>358.782776</td>
      <td>2.419022</td>
      <td>333.420654</td>
      <td>216.178802</td>
      <td>1659.299194</td>
    </tr>
    <tr>
      <th>2014-01-06 00:00:00.000000</th>
      <td>143.516418</td>
      <td>449.231506</td>
      <td>231.064865</td>
      <td>130.531494</td>
      <td>68.488838</td>
      <td>309.423340</td>
      <td>207.788452</td>
      <td>290.023193</td>
      <td>475.285065</td>
      <td>159.258667</td>
      <td>...</td>
      <td>28.059359</td>
      <td>384.663361</td>
      <td>130.603226</td>
      <td>548.133240</td>
      <td>815.633179</td>
      <td>362.887939</td>
      <td>2.393519</td>
      <td>330.079285</td>
      <td>216.511826</td>
      <td>1651.334839</td>
    </tr>
    <tr>
      <th>2014-01-07 00:00:00.000000</th>
      <td>138.995834</td>
      <td>449.048340</td>
      <td>227.081802</td>
      <td>130.766968</td>
      <td>68.625076</td>
      <td>312.105438</td>
      <td>208.436081</td>
      <td>291.227936</td>
      <td>471.533386</td>
      <td>160.674149</td>
      <td>...</td>
      <td>27.777779</td>
      <td>378.926453</td>
      <td>128.444138</td>
      <td>553.352234</td>
      <td>804.197632</td>
      <td>361.354645</td>
      <td>2.314663</td>
      <td>327.391602</td>
      <td>214.608841</td>
      <td>1622.836548</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2024-09-04 00:00:00.000000</th>
      <td>1472.300049</td>
      <td>3231.899902</td>
      <td>1177.500000</td>
      <td>7298.149902</td>
      <td>1871.949951</td>
      <td>1562.000000</td>
      <td>1785.050049</td>
      <td>1641.949951</td>
      <td>2841.100098</td>
      <td>1235.750000</td>
      <td>...</td>
      <td>332.799988</td>
      <td>3029.800049</td>
      <td>816.500000</td>
      <td>1832.699951</td>
      <td>4479.649902</td>
      <td>1080.300049</td>
      <td>151.300003</td>
      <td>1644.849976</td>
      <td>3609.000000</td>
      <td>11590.500000</td>
    </tr>
    <tr>
      <th>2024-09-05 00:00:00.000000</th>
      <td>1465.400024</td>
      <td>3237.800049</td>
      <td>1180.699951</td>
      <td>7243.399902</td>
      <td>1864.449951</td>
      <td>1547.050049</td>
      <td>1790.349976</td>
      <td>1645.250000</td>
      <td>2836.250000</td>
      <td>1235.900024</td>
      <td>...</td>
      <td>330.950012</td>
      <td>2987.149902</td>
      <td>818.599976</td>
      <td>1824.699951</td>
      <td>4480.350098</td>
      <td>1068.650024</td>
      <td>151.750000</td>
      <td>1641.300049</td>
      <td>3721.399902</td>
      <td>11537.250000</td>
    </tr>
    <tr>
      <th>2024-09-06 00:00:00.000000</th>
      <td>1441.900024</td>
      <td>3274.500000</td>
      <td>1159.150024</td>
      <td>7317.649902</td>
      <td>1856.750000</td>
      <td>1539.099976</td>
      <td>1754.150024</td>
      <td>1637.000000</td>
      <td>2838.449951</td>
      <td>1208.449951</td>
      <td>...</td>
      <td>329.700012</td>
      <td>2929.850098</td>
      <td>782.599976</td>
      <td>1823.349976</td>
      <td>4457.500000</td>
      <td>1048.650024</td>
      <td>151.250000</td>
      <td>1623.400024</td>
      <td>3693.800049</td>
      <td>11426.799805</td>
    </tr>
    <tr>
      <th>2024-09-09 00:00:00.000000</th>
      <td>1435.849976</td>
      <td>3281.449951</td>
      <td>1170.650024</td>
      <td>7346.750000</td>
      <td>1858.900024</td>
      <td>1542.599976</td>
      <td>1745.800049</td>
      <td>1647.500000</td>
      <td>2922.100098</td>
      <td>1237.849976</td>
      <td>...</td>
      <td>328.549988</td>
      <td>2926.050049</td>
      <td>784.299988</td>
      <td>1821.949951</td>
      <td>4453.000000</td>
      <td>1038.599976</td>
      <td>149.399994</td>
      <td>1579.949951</td>
      <td>3684.100098</td>
      <td>11497.099609</td>
    </tr>
    <tr>
      <th>2024-09-10 00:00:00.000000</th>
      <td>1453.099976</td>
      <td>3294.300049</td>
      <td>1187.000000</td>
      <td>7240.100098</td>
      <td>1825.300049</td>
      <td>1577.900024</td>
      <td>1779.199951</td>
      <td>1650.599976</td>
      <td>2898.500000</td>
      <td>1237.199951</td>
      <td>...</td>
      <td>334.200012</td>
      <td>2923.000000</td>
      <td>782.599976</td>
      <td>1836.849976</td>
      <td>4506.899902</td>
      <td>1035.449951</td>
      <td>149.449997</td>
      <td>1608.150024</td>
      <td>3727.949951</td>
      <td>11541.799805</td>
    </tr>
  </tbody>
</table>
<p>2631 rows × 30 columns</p>
</div>




```python
df.index = pd.to_datetime(df.index)
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ADANIPORTS.BO</th>
      <th>ASIANPAINT.BO</th>
      <th>AXISBANK.BO</th>
      <th>BAJFINANCE.BO</th>
      <th>BAJAJFINSV.BO</th>
      <th>BHARTIARTL.BO</th>
      <th>HCLTECH.BO</th>
      <th>HDFCBANK.BO</th>
      <th>HINDUNILVR.BO</th>
      <th>ICICIBANK.BO</th>
      <th>...</th>
      <th>POWERGRID.BO</th>
      <th>RELIANCE.BO</th>
      <th>SBIN.BO</th>
      <th>SUNPHARMA.BO</th>
      <th>TCS.BO</th>
      <th>TATAMOTORS.BO</th>
      <th>TATASTEEL.BO</th>
      <th>TECHM.BO</th>
      <th>TITAN.BO</th>
      <th>ULTRACEMCO.BO</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-01-01</th>
      <td>148.608017</td>
      <td>457.888153</td>
      <td>237.651199</td>
      <td>132.478210</td>
      <td>71.218613</td>
      <td>317.704865</td>
      <td>208.925980</td>
      <td>291.359375</td>
      <td>482.071930</td>
      <td>167.935471</td>
      <td>...</td>
      <td>27.960808</td>
      <td>399.871674</td>
      <td>136.612396</td>
      <td>533.455017</td>
      <td>784.349548</td>
      <td>370.850983</td>
      <td>2.492016</td>
      <td>331.822601</td>
      <td>219.604156</td>
      <td>1694.514771</td>
    </tr>
    <tr>
      <th>2014-01-02</th>
      <td>145.229492</td>
      <td>444.742828</td>
      <td>234.155655</td>
      <td>132.696854</td>
      <td>68.394867</td>
      <td>308.764648</td>
      <td>207.821655</td>
      <td>287.766968</td>
      <td>474.442108</td>
      <td>164.522919</td>
      <td>...</td>
      <td>28.706987</td>
      <td>393.864899</td>
      <td>134.847961</td>
      <td>534.852844</td>
      <td>787.481628</td>
      <td>367.932861</td>
      <td>2.459477</td>
      <td>329.643463</td>
      <td>215.322479</td>
      <td>1661.554199</td>
    </tr>
    <tr>
      <th>2014-01-03</th>
      <td>142.850266</td>
      <td>448.315430</td>
      <td>231.929550</td>
      <td>131.931625</td>
      <td>68.465347</td>
      <td>309.470459</td>
      <td>209.390961</td>
      <td>290.614594</td>
      <td>476.591705</td>
      <td>163.283371</td>
      <td>...</td>
      <td>28.256466</td>
      <td>389.207825</td>
      <td>132.804947</td>
      <td>540.491272</td>
      <td>809.223450</td>
      <td>358.782776</td>
      <td>2.419022</td>
      <td>333.420654</td>
      <td>216.178802</td>
      <td>1659.299194</td>
    </tr>
    <tr>
      <th>2014-01-06</th>
      <td>143.516418</td>
      <td>449.231506</td>
      <td>231.064865</td>
      <td>130.531494</td>
      <td>68.488838</td>
      <td>309.423340</td>
      <td>207.788452</td>
      <td>290.023193</td>
      <td>475.285065</td>
      <td>159.258667</td>
      <td>...</td>
      <td>28.059359</td>
      <td>384.663361</td>
      <td>130.603226</td>
      <td>548.133240</td>
      <td>815.633179</td>
      <td>362.887939</td>
      <td>2.393519</td>
      <td>330.079285</td>
      <td>216.511826</td>
      <td>1651.334839</td>
    </tr>
    <tr>
      <th>2014-01-07</th>
      <td>138.995834</td>
      <td>449.048340</td>
      <td>227.081802</td>
      <td>130.766968</td>
      <td>68.625076</td>
      <td>312.105438</td>
      <td>208.436081</td>
      <td>291.227936</td>
      <td>471.533386</td>
      <td>160.674149</td>
      <td>...</td>
      <td>27.777779</td>
      <td>378.926453</td>
      <td>128.444138</td>
      <td>553.352234</td>
      <td>804.197632</td>
      <td>361.354645</td>
      <td>2.314663</td>
      <td>327.391602</td>
      <td>214.608841</td>
      <td>1622.836548</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2024-09-04</th>
      <td>1472.300049</td>
      <td>3231.899902</td>
      <td>1177.500000</td>
      <td>7298.149902</td>
      <td>1871.949951</td>
      <td>1562.000000</td>
      <td>1785.050049</td>
      <td>1641.949951</td>
      <td>2841.100098</td>
      <td>1235.750000</td>
      <td>...</td>
      <td>332.799988</td>
      <td>3029.800049</td>
      <td>816.500000</td>
      <td>1832.699951</td>
      <td>4479.649902</td>
      <td>1080.300049</td>
      <td>151.300003</td>
      <td>1644.849976</td>
      <td>3609.000000</td>
      <td>11590.500000</td>
    </tr>
    <tr>
      <th>2024-09-05</th>
      <td>1465.400024</td>
      <td>3237.800049</td>
      <td>1180.699951</td>
      <td>7243.399902</td>
      <td>1864.449951</td>
      <td>1547.050049</td>
      <td>1790.349976</td>
      <td>1645.250000</td>
      <td>2836.250000</td>
      <td>1235.900024</td>
      <td>...</td>
      <td>330.950012</td>
      <td>2987.149902</td>
      <td>818.599976</td>
      <td>1824.699951</td>
      <td>4480.350098</td>
      <td>1068.650024</td>
      <td>151.750000</td>
      <td>1641.300049</td>
      <td>3721.399902</td>
      <td>11537.250000</td>
    </tr>
    <tr>
      <th>2024-09-06</th>
      <td>1441.900024</td>
      <td>3274.500000</td>
      <td>1159.150024</td>
      <td>7317.649902</td>
      <td>1856.750000</td>
      <td>1539.099976</td>
      <td>1754.150024</td>
      <td>1637.000000</td>
      <td>2838.449951</td>
      <td>1208.449951</td>
      <td>...</td>
      <td>329.700012</td>
      <td>2929.850098</td>
      <td>782.599976</td>
      <td>1823.349976</td>
      <td>4457.500000</td>
      <td>1048.650024</td>
      <td>151.250000</td>
      <td>1623.400024</td>
      <td>3693.800049</td>
      <td>11426.799805</td>
    </tr>
    <tr>
      <th>2024-09-09</th>
      <td>1435.849976</td>
      <td>3281.449951</td>
      <td>1170.650024</td>
      <td>7346.750000</td>
      <td>1858.900024</td>
      <td>1542.599976</td>
      <td>1745.800049</td>
      <td>1647.500000</td>
      <td>2922.100098</td>
      <td>1237.849976</td>
      <td>...</td>
      <td>328.549988</td>
      <td>2926.050049</td>
      <td>784.299988</td>
      <td>1821.949951</td>
      <td>4453.000000</td>
      <td>1038.599976</td>
      <td>149.399994</td>
      <td>1579.949951</td>
      <td>3684.100098</td>
      <td>11497.099609</td>
    </tr>
    <tr>
      <th>2024-09-10</th>
      <td>1453.099976</td>
      <td>3294.300049</td>
      <td>1187.000000</td>
      <td>7240.100098</td>
      <td>1825.300049</td>
      <td>1577.900024</td>
      <td>1779.199951</td>
      <td>1650.599976</td>
      <td>2898.500000</td>
      <td>1237.199951</td>
      <td>...</td>
      <td>334.200012</td>
      <td>2923.000000</td>
      <td>782.599976</td>
      <td>1836.849976</td>
      <td>4506.899902</td>
      <td>1035.449951</td>
      <td>149.449997</td>
      <td>1608.150024</td>
      <td>3727.949951</td>
      <td>11541.799805</td>
    </tr>
  </tbody>
</table>
<p>2631 rows × 30 columns</p>
</div>




```python
formation = dt.datetime(2024, 4, 1).date()
formation
```




    datetime.date(2024, 4, 1)




```python
df = df.resample('M').last()
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ADANIPORTS.BO</th>
      <th>ASIANPAINT.BO</th>
      <th>AXISBANK.BO</th>
      <th>BAJFINANCE.BO</th>
      <th>BAJAJFINSV.BO</th>
      <th>BHARTIARTL.BO</th>
      <th>HCLTECH.BO</th>
      <th>HDFCBANK.BO</th>
      <th>HINDUNILVR.BO</th>
      <th>ICICIBANK.BO</th>
      <th>...</th>
      <th>POWERGRID.BO</th>
      <th>RELIANCE.BO</th>
      <th>SBIN.BO</th>
      <th>SUNPHARMA.BO</th>
      <th>TCS.BO</th>
      <th>TATAMOTORS.BO</th>
      <th>TATASTEEL.BO</th>
      <th>TECHM.BO</th>
      <th>TITAN.BO</th>
      <th>ULTRACEMCO.BO</th>
    </tr>
    <tr>
      <th>Date</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2014-01-31</th>
      <td>139.471710</td>
      <td>432.605072</td>
      <td>205.970535</td>
      <td>128.239990</td>
      <td>63.936066</td>
      <td>296.577728</td>
      <td>245.955276</td>
      <td>275.522003</td>
      <td>480.849518</td>
      <td>151.270493</td>
      <td>...</td>
      <td>26.890808</td>
      <td>373.842010</td>
      <td>118.062561</td>
      <td>548.133240</td>
      <td>817.303284</td>
      <td>346.071503</td>
      <td>2.088063</td>
      <td>325.212524</td>
      <td>210.802887</td>
      <td>1638.141357</td>
    </tr>
    <tr>
      <th>2014-02-28</th>
      <td>159.171921</td>
      <td>433.246399</td>
      <td>232.895401</td>
      <td>131.910599</td>
      <td>67.629028</td>
      <td>270.557251</td>
      <td>264.644989</td>
      <td>292.432678</td>
      <td>462.681244</td>
      <td>159.717758</td>
      <td>...</td>
      <td>26.637388</td>
      <td>359.623535</td>
      <td>118.534660</td>
      <td>598.971802</td>
      <td>830.680115</td>
      <td>412.545746</td>
      <td>2.017123</td>
      <td>339.694855</td>
      <td>230.451157</td>
      <td>1765.425415</td>
    </tr>
    <tr>
      <th>2014-03-31</th>
      <td>178.443832</td>
      <td>500.622009</td>
      <td>268.687988</td>
      <td>150.528564</td>
      <td>74.253784</td>
      <td>300.106812</td>
      <td>233.495483</td>
      <td>328.072235</td>
      <td>508.923981</td>
      <td>190.591599</td>
      <td>...</td>
      <td>30.303892</td>
      <td>418.229706</td>
      <td>162.899719</td>
      <td>534.340210</td>
      <td>777.849121</td>
      <td>394.047699</td>
      <td>2.309094</td>
      <td>325.884399</td>
      <td>249.718903</td>
      <td>2100.355713</td>
    </tr>
    <tr>
      <th>2014-04-30</th>
      <td>179.205200</td>
      <td>462.743164</td>
      <td>279.496582</td>
      <td>157.613251</td>
      <td>77.782310</td>
      <td>308.341187</td>
      <td>238.361160</td>
      <td>314.666351</td>
      <td>477.940796</td>
      <td>190.492142</td>
      <td>...</td>
      <td>30.505833</td>
      <td>420.839386</td>
      <td>176.541916</td>
      <td>588.580505</td>
      <td>800.198364</td>
      <td>409.973846</td>
      <td>2.348668</td>
      <td>332.767029</td>
      <td>242.392395</td>
      <td>1945.868530</td>
    </tr>
    <tr>
      <th>2014-05-31</th>
      <td>214.941559</td>
      <td>466.682312</td>
      <td>337.807861</td>
      <td>169.936935</td>
      <td>80.145607</td>
      <td>323.727631</td>
      <td>240.687744</td>
      <td>347.896362</td>
      <td>506.647583</td>
      <td>217.058197</td>
      <td>...</td>
      <td>35.121368</td>
      <td>488.070496</td>
      <td>229.016449</td>
      <td>566.073669</td>
      <td>783.678284</td>
      <td>410.963013</td>
      <td>2.784866</td>
      <td>347.821320</td>
      <td>295.675903</td>
      <td>2288.283691</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>2024-05-31</th>
      <td>1431.557007</td>
      <td>2850.117188</td>
      <td>1160.854126</td>
      <td>6669.028809</td>
      <td>1527.486328</td>
      <td>1364.247314</td>
      <td>1312.740112</td>
      <td>1530.849976</td>
      <td>2306.232666</td>
      <td>1110.096313</td>
      <td>...</td>
      <td>307.344208</td>
      <td>2849.926758</td>
      <td>830.099976</td>
      <td>1455.538818</td>
      <td>3660.994629</td>
      <td>917.169739</td>
      <td>163.850082</td>
      <td>1206.508789</td>
      <td>3232.073975</td>
      <td>9842.994141</td>
    </tr>
    <tr>
      <th>2024-06-30</th>
      <td>1477.400024</td>
      <td>2917.300049</td>
      <td>1264.723999</td>
      <td>7115.750000</td>
      <td>1587.800049</td>
      <td>1437.339966</td>
      <td>1448.499634</td>
      <td>1683.550049</td>
      <td>2475.199951</td>
      <td>1189.562500</td>
      <td>...</td>
      <td>328.071838</td>
      <td>3121.255615</td>
      <td>848.849976</td>
      <td>1517.592285</td>
      <td>3895.898926</td>
      <td>990.099976</td>
      <td>174.000000</td>
      <td>1404.197998</td>
      <td>3406.100098</td>
      <td>11592.572266</td>
    </tr>
    <tr>
      <th>2024-07-31</th>
      <td>1569.650024</td>
      <td>3082.300049</td>
      <td>1166.199951</td>
      <td>6808.450195</td>
      <td>1653.300049</td>
      <td>1484.626465</td>
      <td>1641.949951</td>
      <td>1617.050049</td>
      <td>2706.050049</td>
      <td>1204.434448</td>
      <td>...</td>
      <td>345.725067</td>
      <td>3000.066895</td>
      <td>872.750000</td>
      <td>1717.849976</td>
      <td>4384.649902</td>
      <td>1156.349976</td>
      <td>165.350006</td>
      <td>1553.900024</td>
      <td>3457.850098</td>
      <td>11882.799805</td>
    </tr>
    <tr>
      <th>2024-08-31</th>
      <td>1482.650024</td>
      <td>3126.800049</td>
      <td>1175.500000</td>
      <td>7206.149902</td>
      <td>1782.449951</td>
      <td>1588.949951</td>
      <td>1752.150024</td>
      <td>1632.949951</td>
      <td>2778.100098</td>
      <td>1230.150024</td>
      <td>...</td>
      <td>337.399994</td>
      <td>3019.750000</td>
      <td>815.650024</td>
      <td>1820.750000</td>
      <td>4551.850098</td>
      <td>1109.400024</td>
      <td>152.800003</td>
      <td>1635.699951</td>
      <td>3567.149902</td>
      <td>11300.650391</td>
    </tr>
    <tr>
      <th>2024-09-30</th>
      <td>1453.099976</td>
      <td>3294.300049</td>
      <td>1187.000000</td>
      <td>7240.100098</td>
      <td>1825.300049</td>
      <td>1577.900024</td>
      <td>1779.199951</td>
      <td>1650.599976</td>
      <td>2898.500000</td>
      <td>1237.199951</td>
      <td>...</td>
      <td>334.200012</td>
      <td>2923.000000</td>
      <td>782.599976</td>
      <td>1836.849976</td>
      <td>4506.899902</td>
      <td>1035.449951</td>
      <td>149.449997</td>
      <td>1608.150024</td>
      <td>3727.949951</td>
      <td>11541.799805</td>
    </tr>
  </tbody>
</table>
<p>129 rows × 30 columns</p>
</div>




```python
begin_formation = formation - MonthEnd(12)
begin_formation = begin_formation.strftime('%Y-%m-%d')
print(begin_formation)
```

    2023-04-30
    


```python
end_formation = formation - MonthEnd(1)
end_formation = end_formation.strftime('%Y-%m-%d')
print(end_formation)

```

    2024-03-31
    


```python
begin_price = df.loc[begin_formation]
```


```python
begin_price
```




    ADANIPORTS.BO     678.289429
    ASIANPAINT.BO    2849.499756
    AXISBANK.BO       859.336853
    BAJFINANCE.BO    6222.596680
    BAJAJFINSV.BO    1349.786621
    BHARTIARTL.BO     790.937439
    HCLTECH.BO       1022.897034
    HDFCBANK.BO      1646.445435
    HINDUNILVR.BO    2393.476807
    ICICIBANK.BO      902.445801
    INDUSINDBK.BO    1127.049194
    INFY.BO          1196.185547
    ITC.BO            411.698914
    JSWSTEEL.BO       716.640442
    KOTAKBANK.BO     1933.983032
    LT.BO            2346.308350
    M&M.BO           1217.686035
    MARUTI.BO        8509.464844
    NESTLEIND.BO     2035.613403
    NTPC.BO           165.138550
    POWERGRID.BO      165.605103
    RELIANCE.BO      2403.522217
    SBIN.BO           557.678772
    SUNPHARMA.BO      978.110352
    TCS.BO           3145.545410
    TATAMOTORS.BO     482.163818
    TATASTEEL.BO      102.426781
    TECHM.BO          969.011719
    TITAN.BO         2622.833984
    ULTRACEMCO.BO    7475.904785
    Name: 2023-04-30 00:00:00, dtype: float64




```python
end_price = df.loc[end_formation]
```


```python
end_price
```




    ADANIPORTS.BO     1335.967163
    ASIANPAINT.BO     2818.721191
    AXISBANK.BO       1047.491699
    BAJFINANCE.BO     7204.192871
    BAJAJFINSV.BO     1642.613770
    BHARTIARTL.BO     1222.238770
    HCLTECH.BO        1511.288818
    HDFCBANK.BO       1428.701294
    HINDUNILVR.BO     2246.365967
    ICICIBANK.BO      1086.499390
    INDUSINDBK.BO     1538.621460
    INFY.BO           1469.365479
    ITC.BO             421.084839
    JSWSTEEL.BO        824.884460
    KOTAKBANK.BO      1783.840942
    LT.BO             3744.667480
    M&M.BO            1907.387085
    MARUTI.BO        12494.990234
    NESTLEIND.BO      2611.961182
    NTPC.BO            333.318420
    POWERGRID.BO       274.765106
    RELIANCE.BO       2966.730225
    SBIN.BO            740.185730
    SUNPHARMA.BO      1615.382446
    TCS.BO            3846.590332
    TATAMOTORS.BO      986.887939
    TATASTEEL.BO       152.822174
    TECHM.BO          1227.666992
    TITAN.BO          3792.340820
    ULTRACEMCO.BO     9687.616211
    Name: 2024-03-31 00:00:00, dtype: float64




```python
returns = end_price/begin_price -1
returns
```




    ADANIPORTS.BO    0.969612
    ASIANPAINT.BO   -0.010801
    AXISBANK.BO      0.218954
    BAJFINANCE.BO    0.157747
    BAJAJFINSV.BO    0.216943
    BHARTIARTL.BO    0.545304
    HCLTECH.BO       0.477459
    HDFCBANK.BO     -0.132251
    HINDUNILVR.BO   -0.061463
    ICICIBANK.BO     0.203950
    INDUSINDBK.BO    0.365177
    INFY.BO          0.228376
    ITC.BO           0.022798
    JSWSTEEL.BO      0.151044
    KOTAKBANK.BO    -0.077634
    LT.BO            0.595983
    M&M.BO           0.566403
    MARUTI.BO        0.468364
    NESTLEIND.BO     0.283132
    NTPC.BO          1.018417
    POWERGRID.BO     0.659158
    RELIANCE.BO      0.234326
    SBIN.BO          0.327262
    SUNPHARMA.BO     0.651534
    TCS.BO           0.222869
    TATAMOTORS.BO    1.046790
    TATASTEEL.BO     0.492014
    TECHM.BO         0.266927
    TITAN.BO         0.445894
    ULTRACEMCO.BO    0.295845
    dtype: float64




```python
winners = returns.nlargest(5)
winners
```




    TATAMOTORS.BO    1.046790
    NTPC.BO          1.018417
    ADANIPORTS.BO    0.969612
    POWERGRID.BO     0.659158
    SUNPHARMA.BO     0.651534
    dtype: float64




```python

```


```python

```
