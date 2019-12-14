# -*- coding: utf-8 -*-
"""

Name:
    ex300-gender-bmi-dataset.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Read a delimited file.
              Explore statistical functions (Pandas).
              Write a delimited file.

    Note:     Data file is from kaggle gender data set.
    
"""

#
# 1 - Load data frame from csv file
#

# include the libraries
import pandas as pd
import numpy as np

# load kaggle data set
fn = 'C:\prepare-n-clean-data\data\gender-vs-htwt.csv'
df = pd.read_csv(fn)

# Display status
print('Read file into data frame')
print('')


#                  
# 2 - Update data frame
#

# Calculate bmi
df["BMI"] = df["Weight"] / ( (df["Height"] / 100.0) * (df["Height"] / 100.0) )

# Encode gender category
df['Gender'] = df['Gender'].apply(lambda y: 1 if (y == 'Male') else 2)


# Display status
print('Update data frame')
print('')


#
# 3 - Statistics ~ male
#

# Male data
m = df[df["Gender"] == 1]

# Aggregate
mstat = m.agg(['min', 'max', 'mean', 'median', 'std', 'var'])

# Drop index
mstat.drop(mstat.columns[[3]], axis = 1, inplace = True)

# New data frame
n = pd.DataFrame([[0, m['Height'].quantile(.5), m['Weight'].quantile(.5), m['BMI'].quantile(.5)]], 
                   columns=['Gender', 'Height', 'Weight', 'BMI'], index=['ptile50'])

# Append to result
mstat = mstat.append(n)


#
# 4 - Statistics ~ female
#

# Male data
f = df[df["Gender"] == 2]

# Aggregate
fstat = m.agg(['min', 'max', 'mean', 'median', 'std', 'var'])

# Drop index
fstat.drop(fstat.columns[[3]], axis = 1, inplace = True)

# New data frame
n = pd.DataFrame([[0, f['Height'].quantile(.5), f['Weight'].quantile(.5), f['BMI'].quantile(.5)]], 
                   columns=['Gender', 'Height', 'Weight', 'BMI'], index=['ptile50'])

# Append to result
fstat = fstat.append(n)


#
# 5 - Correlation matrix
#

# Correlation matrix
cols = ['Gender', 'Height', 'Weight', 'BMI']
cmatrix = df[cols].corr(method='pearson')


#
# 6 - Save data to csv file
#

fn = 'C:\prepare-n-clean-data\data\miner-gender-vs-htwt.csv'
o = df.to_csv(fn, index = None, header=True) 

print('Write data frame to file')
print('')


#
# 7 - Removed variables from memory
#

del cols
del f
del fn
del m
del n
del o
