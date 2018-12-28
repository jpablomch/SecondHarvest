
import pandas as pd
import csv

DEBUG = False # TODO: Read from args.

dynamic = pd.read_csv("dynamic.csv")

with open('Config/columnsToKeepFromDynamic.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    columns_keep_dynamic = list(reader)[0]
    if DEBUG:
        print(columns_keep_dynamic)

dynamic = dynamic.loc[:, columns_keep_dynamic]

with open('Config/removeFromDynamic.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    remove_from_dynamic = list(reader)[0]
    if DEBUG:
        print("Removed from dynamic: ")
        print(remove_from_dynamic)

dynamic = dynamic.loc[~dynamic['Service Type'].isin(remove_from_dynamic)]
dynaDupli = dynamic[dynamic.duplicated(['Client Name'], keep=False)]

# No Duplicates
dynamic = dynamic.drop_duplicates(subset=['Client Name'], keep=False)

dynamic = dynamic.reset_index(drop=True)

# Fix Duplicates and add them to dynamic
dynaDupli = dynaDupli.sort_values(by=['Client Name'])

client = None
table = []
maxRepeat = 0
count = 0
if DEBUG:
    print("Dynadupli:")
    print(dynaDupli)
for index, row in dynaDupli.iterrows():
    if client == row['Client Name']: # Same Row
        table[len(table)-1].append(row['Referral Date'])
        table[len(table)-1].append(row['Referral Time'])
        table[len(table)-1].append(row['Site Name'])
        table[len(table)-1].append(row['Service Type'])
        count += 1
        if count > maxRepeat:
            maxRepeat = count
    else: # New Row
        toApend = []
        for i in columns_keep_dynamic:
            toApend.append(row[i])
        table.append(toApend)
        count = 0
    client = row['Client Name']

if DEBUG:
    print(maxRepeat)
# Add colums to dynamic
group = ['Referral Date', 'Referral Time', 'Site Name', 'Service Type']
newCols = []
for i in range(maxRepeat):
    for j in range(len(group)):
        newCols.append(group[j] + str(i+2))

dynamic = pd.concat([dynamic,pd.DataFrame(columns=newCols)], sort=False)

headers = dynamic.dtypes.index
if DEBUG:
    print(headers)

for i in range(len(table)):
    missing = len(list(headers)) - len(table[i])
    table[i].extend(['']*missing)
    if DEBUG:
        print(i)
    dynamic = dynamic.append(pd.Series(table[i], index=list(headers)), ignore_index=True)

# table is empty skip this
if table:
    dynamic.loc[len(dynamic)] = table[0]
    if DEBUG:
        print(table[0])


dynamic['Phone'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
# dynamic['Phone'] = '+1' + dynamic['Phone'].astype(str)
dynamic['Cell Phone'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')
# dynamic['Cell Phone'] = '+1' + dynamic['Cell Phone'].astype(str)

dynamic['Language'].replace('Spanish', 'spa', inplace=True)
dynamic['Language'].replace('Vietnamese', 'vie', inplace=True)
dynamic['Language'].replace('Chinese', 'chi', inplace=True)
dynamic['Language'].replace('Tagalog', 'tgl', inplace=True)
dynamic['Language'].replace('English', 'eng', inplace=True)

dynamic.to_csv('Results/result.csv')
dynaDupli.to_csv('Results/duplicates.csv')

print("Done!")