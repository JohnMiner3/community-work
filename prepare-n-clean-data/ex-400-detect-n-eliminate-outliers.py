# -*- coding: utf-8 -*-
"""

Name:
    ex400-detect-n-eliminate-outliers.py
     
Design Phase:
    Author:   John Miner
    Date:     06-01-2018
    Purpose:  Read a delimited file.
              Show data using plotting & binning.
              Use sub-setting to elminate outliers.
              Write a delimited file.

    Note:     Data file is from data driven MPP competition.
    
"""

#
# 1 - Load data frame from csv file
#

# include the libraries
import pandas as pd

# load data set
fn = r'C:\prepare-n-clean-data\data\rate-spread-labels.csv'
df = pd.read_csv(fn) #, index_col="row_id")

# Display status
print('Read file into data frame')
print('')


#
# 2 - Use pandas to look at counts
#

# Frequency count
z = df.groupby('rate_spread').size()
z = z.reset_index(drop=False)
z.columns=['rate', 'count']
print('\n%s\n' % z)


# Quick summary stats
y = df['rate_spread'].describe()
y = y.reset_index(drop=False)
print('\n%s\n' % y)



#
# Step 3 - Function to bin data using pandas
#

def binvals(df = None, col = None, cnt=1):
    a = df[col].value_counts(bins=cnt)
    a = a.reset_index()
    a['l1'] = a['index'].apply(lambda x: x.left)
    a['r1'] = a['index'].apply(lambda x: x.right)
    a.drop(a.columns[0], axis=1, inplace=True)
    a = a.sort_values(by=['l1'])
    return a

# Bin the data
h = binvals(df, 'rate_spread', 10)
print('\n%s\n' % h)


#
# 4 - Plot data with matplotlib 
#

# Valid rate spreads
g1 = z[z['rate'] < 9]
ax = g1.plot.bar(x='rate', y='count', rot=0)

# Invalid rate spreads
g2 = z[z['rate'] >= 9]
ax = g2.plot.bar(x='rate', y='count', rot=0)



#
# 5 - Elminate bad records
#

df = df[df['rate_spread'] < 9]



#
# 6 - Save data to csv file
#

fn = r'C:\prepare-n-clean-data\data\miner-rate-spread-labels.csv'
o = df.to_csv(fn, index = False, header=True) 

print('Write data frame to file')
print('')


#
# 7 - Remove variables from memory
#

del df
del fn
del o
