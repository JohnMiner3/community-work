# -*- coding: utf-8 -*-
"""

Name:
    ex402-handle-missing-data.py
     
Design Phase:
    Author:   John Miner
    Date:     06-01-2018
    Purpose:  Rread in titantic data set.
              Explore various ways to handle missing data.


    Note:     Data file is from Kaggle competition.
    
"""


#
# 1 - Load data frame from csv file
#

# include the libraries
import pandas as pd

# load data set
fn = r'C:\prepare-n-clean-data\data\titantic-data.csv'
df = pd.read_csv(fn)

# Display status
print('Read fil into data frame')
print('')


#
# 2- Find out which features have missing dsata
#

# Use info function
df.info()

# Creating a data frame for analysis
t = df.isnull().sum().sort_values(ascending=False)
p = (df.isnull().sum() / df.isnull().count()).sort_values(ascending=False)
missing = pd.concat([t, p], axis=1, keys=['Total', 'Percent'])



#
# 3 - How to drop a column or a record?
#

# Sometimes columns with sparse features are very important

# Make a copy of the dataset
j = df.copy()

# Drop the cabin feature (77%)
j.drop(columns=['Cabin'], inplace=True)

# Remove rows with missing age data
j = j[~j.Age.isnull()]


# Make a copy of the dataset
k = df.copy()

# Make sure dataset is not a duplicate
k.drop_duplicates()

# Yet another way
k = k.dropna(subset=['Age'])



#
# 4 - Replace with fixed value
#

# Make a copy of the dataset
l = df.copy()

# Back fill value
col = ['Age']
l.loc[:,col] = l.loc[:,col].bfill()


# Make a copy of the dataset
m = df.copy()

# Back fill value
col = ['Age']
m.loc[:,col] = m.loc[:,col].ffill()


# Make a copy of the dataset
o = df.copy()

# Set missing data to zero
o.Age.fillna(0, inplace=True)


# Make a copy of the dataset
p = df.copy()

# Set missing data to zero
p.Age.fillna(p.Age.mean(), inplace=True)



#
# 5 - Replace with random value between -std+mean to std+mean
#

# Include library
import numpy as np

# Make a copy of the dataset
q = df.copy()

# Replace missing data
avg = q['Age'].mean()
std = q['Age'].std()
ncnt = q['Age'].isnull().sum()
nlst = np.random.randint(avg - std, avg + std, size= ncnt)
q['Age'][np.isnan(q['Age'])] = nlst
q['Age'] = q['Age'].astype(int)