#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv('/Users/lpottier/research/projects/active/decaf-work/cori-runs-log/pegasus-io-1000genome.csv')

df['Size(B)'] = df['Size(B)']/(1024*1024)
df['MAXRSS(KB)'] = df['MAXRSS(KB)']/(1024*1024)
df = df.rename(columns={"Size(B)": "Size(MB)", "MAXRSS(KB)": "MAXRSS(GB)"})

df = df.groupby(['K','Trial']).max()
df = df.groupby('K').agg(['mean', 'std'])

df[('Size(MB)', 'mean')] = df[('Size(MB)', 'mean')].round(2)
df[('MAXRSS(GB)', 'mean')] = df[('MAXRSS(GB)', 'mean')].round(2)

pd.set_option('display.float_format', lambda x: format(x, "5.2e"))
print(df.to_latex(index=True))
print(df.to_markdown(index=True))
