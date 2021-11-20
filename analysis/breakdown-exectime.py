#!/usr/bin/env python3

import pandas as pd

def filter_job(row):
    data = row['ID'].split('_')
    if data[0] in ["chmod", "create", "cleanup"]:
        return "Auxiliary"
    if data[1].startswith('ID'):
        return data[0].capitalize() 
    else:
        return data[0].capitalize()+'_'+data[1].capitalize() 

df = pd.read_csv('/Users/lpottier/research/projects/active/decaf-integration-paper/data/breakdown-1000genome.csv')
df = df.rename(columns={"Job": "ID", "Duration": "Execution Time (s)"})
df['Job'] = df.apply(filter_job, axis=1)

df_sum = df.groupby('Job').max()
total_exec = df_sum['Execution Time (s)'].sum()
df_sum['Fraction (%)'] = 100*(df_sum['Execution Time (s)']/total_exec)

df_sum['Fraction (%)'] = df_sum['Fraction (%)'].round(2)
df_sum['Execution Time (s)'] = df_sum['Execution Time (s)'].round(0)

df_sum = df_sum.drop(columns=['ID'])
df_sum = df_sum.sort_values(by='Fraction (%)', ascending=False)

# df_sum = df_sum.append({'Job': 'Total', 'Execution Time (s)':total_exec.round(0), 'Fraction (%)': 100}, ignore_index=True)

print(df_sum.to_latex(index=True))
print(df_sum.to_markdown(index=True))

print("{} sec aprox {} hours".format(total_exec, total_exec/(60**2)))
