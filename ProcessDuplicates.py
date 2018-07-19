
import pandas as pd
import csv

DEBUG = False

dynamic = pd.read_csv("dynamic.csv")

with open('columnsToKeepFromDynamic.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    columns_keep_dynamic = list(reader)[0]
    if DEBUG:
        print(columns_keep_dynamic)

# dynamic = dynamic.ix[:, ['Referral Date', 'Referral Time', 'Client Name', 'Phone', 'Cell Phone', 'E-Mail', 'Comm Preference', 'Language', 'Site Name', 'Service Type']]
dynamic = dynamic.loc[:, columns_keep_dynamic]

with open('removeFromDynamic.csv', 'r') as f:
    reader = csv.reader(f, delimiter=',')
    remove_from_dynamic = list(reader)[0]
    if DEBUG:
        print(remove_from_dynamic)

# dynamic = dynamic.loc[~dynamic['Service Type'].isin(['CalFresh Application',
#                                                      'CalFresh Prescreen',
#                                                      'Calfresh Prescreen',
#                                                      'Calfresh Application',
#                                                      'Summer Meal',
#                                                      'Wellness Pantry',
#                                                      'Soup Kitchen'])]
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
        # table.append([row['Referral Date'],
        #               row['Referral Time'],
        #               row['Client Name'],
        #               row['Phone'],
        #               row['Cell Phone'],
        #               row['E-Mail'],
        #               row['Comm Preference'],
        #               row['Language'],
        #               row['Site Name'],
        #               row['Service Type']])
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

dynamic.loc[len(dynamic)] = table[0]

if DEBUG:
    print(table[0])

dynamic.to_csv('result.csv')
dynaDupli.to_csv('duplicates.csv')

print("Done!")
