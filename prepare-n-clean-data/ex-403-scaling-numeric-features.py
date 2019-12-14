# -*- coding: utf-8 -*-
"""

Name:
    ex403-scaling-numeric-features.py
     
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

# load kaggle data set
fn = 'C:\prepare-n-clean-data\data\miner-gender-vs-htwt.csv'
df = pd.read_csv(fn)

# Display status
print('Read file into data frame')
print('')


#
# 2 - Plot BMI vs Gender - left skewed?
#

# Male DMI
f1 = df[df['Gender'] == 1]
ax = f1.plot.hist(y='BMI')


# Female BMI
m1 = df[df['Gender'] == 2]
ax = m1.plot.hist(y='BMI')


#
# 3 - Min / Max Scaler
#

# Scale data, result is np array
s1 = df[['Gender', 'BMI']]
from sklearn import preprocessing 
mm_scaler = preprocessing.MinMaxScaler()
s_minmax = mm_scaler.fit_transform(s1)

# Convert back to data frame
s2 = pd.DataFrame({'Gender': s_minmax[:, 0], 'BMI': s_minmax[:, 1]})

ax = df.plot.hist(y='BMI')
ax = s2.plot.hist(y='BMI')



#
# 4 - Standard Scaler
#

# Scale data, result is np array
s3 = df[['Gender', 'BMI']]
from sklearn import preprocessing 
std_scaler = preprocessing.StandardScaler()
s_standard = std_scaler.fit_transform(s3)

# Convert back to data frame
s4 = pd.DataFrame({'Gender': s_standard[:, 0], 'BMI': s_standard[:, 1]})

ax = df.plot.hist(y='BMI')
ax = s4.plot.hist(y='BMI')


#
# 5 - Max Abs Scaler
#

# Scale data, result is np array
s5 = df[['Gender', 'BMI']]
from sklearn import preprocessing 
ma_scaler = preprocessing.MaxAbsScaler()
s_maxabs = ma_scaler.fit_transform(s5)

# Convert back to data frame
s6 = pd.DataFrame({'Gender': s_maxabs[:, 0], 'BMI': s_maxabs[:, 1]})

ax = df.plot.hist(y='BMI')
ax = s6.plot.hist(y='BMI')


#
# 6 - Robust Scaler
#

# Scale data, result is np array
s7 = df[['Gender', 'BMI']]
from sklearn import preprocessing 
rb_scaler = preprocessing.RobustScaler()
s_robust = rb_scaler.fit_transform(s7)

# Convert back to data frame
s8 = pd.DataFrame({'Gender': s_robust[:, 0], 'BMI': s_robust[:, 1]})

ax = df.plot.hist(y='BMI')
ax = s8.plot.hist(y='BMI')
