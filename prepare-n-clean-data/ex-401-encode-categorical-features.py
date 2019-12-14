# -*- coding: utf-8 -*-
"""

Name:
    ex401-encode-categorical-features.py
     
Design Phase:
    Author:   John Miner
    Date:     06-01-2018
    Purpose:  Read a delimited file #1.
              Explore how encode using Pandas.
              Read a delimited file #1.
              Explore how encode using other libraries.

    Note:     Data files include iris flowers and us housing prices.
    
"""


#
# 1 - Load data frame from csv file
#

# include the libraries
import pandas as pd

# load data set
fn = r'C:\prepare-n-clean-data\data\fishers-iris-data.csv'
df = pd.read_csv(fn)

# Display status
print('Read file #1 into data frame')
print('')


#
# 2 - Label encoding (auto number)
#

a = df.copy()
a["Species"] = a["Species"].astype('category')
a["c_species"] = a["Species"].cat.codes

print ("label encode #1 ~ species")
print('')


#
# 3 - Label encoding (custom number)
#

b = df.copy()
d = {'setosa': 3, 'versicolor': 2, 'virginica': 1}
b["c_species"] = b["Species"].map(d)

print ("label encode #2 ~ species")
print('')



#
# 4 -  Get dummies (each response is col)
#

c = df.copy()
d = pd.get_dummies(a["c_species"], prefix = 'c_species', prefix_sep='_')
e = pd.concat([c, d], axis=1, ignore_index=False)

print ("indicator encode ~ species")
print('')


#
# 5 -  Binary Encoding (int -> bin cols)
#

c = df.copy()
f = a["c_species"].apply(lambda x: pd.Series(list(bin(x)[2:].zfill(2))))
f.columns = ['species_1', 'species_0']
g = pd.concat([c, f], axis=1, ignore_index=False)

print ("binary encode ~ species")
print('')


#
# 6 - Load data frame from csv file
#

# include the libraries
import pandas as pd

# load data set
fn = r'C:\prepare-n-clean-data\data\usa-housing-prices.csv'
df = pd.read_csv(fn)

# Display status
print('Read file #2 into data frame')
print('')

# Make unknown empty string
df['State'] = df['State'].fillna('')


#
# 7 - Using other libraries
#

import numpy as np
import category_encoders as ce
from sklearn.preprocessing import LabelEncoder


# A - Label encoding
z = pd.DataFrame()
z['state'] = df['State']
le = LabelEncoder()
z['le_state'] = le.fit_transform(np.ravel(z))

print ("label encode ~ state")
print('')

# data quality issue
print('Max categorical value for state is %s.\n' % z['le_state'].max())


# B - Hot One encoding
y = pd.DataFrame()
y['state'] = df['State']
oh = ce.OneHotEncoder(cols = ['state'])
x = oh.fit_transform(y)
w = pd.concat([y, x], axis=1, ignore_index=False)

print ("hot one encode ~ state")
print('')


# C - Binary encoding
v = pd.DataFrame()
v['state'] = df['State']
be = ce.BinaryEncoder(cols = ['state'])
u = be.fit_transform(v)
t = pd.concat([v, u], axis=1, ignore_index=False)

print ("binary encode ~ state")
print('')


# D - Base N encoding
s = pd.DataFrame()
s['state'] = df['State']
bn = ce.BaseNEncoder(cols = ['state'], base=8)
r = bn.fit_transform(s)
q = pd.concat([s, r], axis=1, ignore_index=False)

print ("base n=8 encode ~ state")
print('')


# E - Hash encoding
p = pd.DataFrame()
p['state'] = df['State']
he = ce.HashingEncoder(cols = ['state'], verbose=1)
m = he.fit_transform(p)
n = pd.concat([p, m], axis=1, ignore_index=False)

print ("hash  encode ~ state")
print('')


#
#  Bug in 2.1 as result of parallelism
#

# pip install category_encoders==2.0.0




#
# 8 - Removed variables from memory
#

del c
del d
del f
del df
del fn

del p
del m

del s
del r

del v
del u

del y
del x