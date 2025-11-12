import yfinance as yf, requests
from datetime import datetime, timedelta
import pandas as pd

s = requests.Session()
s.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
ticker = 'MSTR'
ann = datetime(2025, 10, 27)
as_of = ann + timedelta(days=30)

hist = yf.download(ticker,
                   start=(ann - timedelta(days=2)).date(),
                   end=(as_of + timedelta(days=1)).date(),
                   progress=False,
                   session=s)

def nearest(df, d):
    if df.empty: return None
    t = pd.Timestamp(d)
    for ts in df.index:
        if ts >= t:
            return float(df.loc[ts]['Close'])
    return float(df.iloc[-1]['Close'])

d  = nearest(hist, ann)
d1 = nearest(hist, ann + timedelta(days=1))
d7 = nearest(hist, ann + timedelta(days=7))
d30= nearest(hist, ann + timedelta(days=30))

pct = lambda a,b: 'N/A' if (a is None or b is None or a==0) else f'{(b-a)/a*100:.2f}%'
print({'price_D': d, '1D': pct(d,d1), '7D': pct(d,d7), '30D': pct(d,d30)})
