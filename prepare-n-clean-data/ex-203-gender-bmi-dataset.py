# -*- coding: utf-8 -*-
"""

Name:
    ex-203-gender-bmi-dataset.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Read a delimited file.
              Explore statistical functions (NumPy).
              Write a delimited file.

    Note:     Data file is from kaggle gender data set.
    
"""

#
# 1 - Load data from csv file
#

# include the library
import numpy as np
import os

# load kaggle data set
fname = os.getcwd() + r'.\data\gender-vs-htwt.csv'
dataset = np.loadtxt(fname,
                  dtype={'names': ['Gender', 'Height', 'Weight', 'Index'], 'formats': ['S6', 'i4', 'i4', 'i4'] }, 
                  comments='#', delimiter=',', converters=None, skiprows=1, usecols=None, unpack=False, ndmin=0)
print('Read data from file\n')

                  
# Show data - 1 dimension array of tuples
print('Show top ten rows of n dimensional array.\n')    
print(dataset[:10])
print('')

# How many rows?
print('size of n dimensional array is %d.' % dataset.size)    
print('')

# What data type?
print (type(dataset))
print('')

#
# 2 - Fill vector arrays
#

# Create empty vectors
mf_array = np.empty(0)
ht_array = np.empty(0)
wt_array = np.empty(0)
idx_array = np.empty(0)

# Fill vectors
for i in range(0, dataset.size):
    
    # pull tupple
    t = dataset[i]
    
    # handle category (m = 1, f = 2)
    if (t[0] == b'Male'):
        mf_array = np.append(mf_array, [1])
    else:
        mf_array = np.append(mf_array, [2])

    # height        
    ht_array = np.append(ht_array, t[1])
    
    # weight
    wt_array = np.append(wt_array, t[2])
    
    # index
    idx_array = np.append(idx_array, t[3])


# calculate bmi
bmi_array = np.empty(0)
                     
# broadcasting - expands number to array
ht_array = ht_array / 100

# new bmi array
bmi_array = wt_array / (ht_array * ht_array)

# broadcasting - expands number to array
ht_array = ht_array * 100


#
# 3 - Statistics functions
#

# Min value in vector
bmi_min = np.amin(bmi_array)
print('The minimum bmi = %6.2f.\n' % bmi_min)

# Max value in vector
bmi_max = np.amax(bmi_array)
print('The maximum bmi = %6.2f.\n' % bmi_max)

# Any vals fall below this number
bmi_ptile = np.percentile(bmi_array, 50)
print('The 50%% percentile mark for bmi = %6.2f.\n' % bmi_ptile)

# Mean value in vector - reg average
wt_mean = np.mean(wt_array)
print('The mean weight = %6.2f.\n' % wt_mean)

# Median value in vector - rank order
wt_median = np.median(wt_array)
print('The median weight = %6.2f.\n' % wt_median)

# Standard deviation
wt_std = np.std(wt_array)
print('The standard deviation for weight = %6.2f.\n' % wt_std)

# Variance 
wt_var = np.var(wt_array)
print('The standard variance for weight = %6.2f.\n' % wt_var)

# Correlation coefficient - gender to height - weak pos
print('The correlation coefficent matrix for gender to height.\n')
print(np.corrcoef(mf_array, ht_array))
print('')

# Correlation coefficient - gender to weight - weak neg
print('The correlation coefficent matrix for gender to weight.\n')
print(np.corrcoef(mf_array, wt_array))
print('')


#
# 4 - Save data to csv file
#

# combine data into lists (mf, ht, wt, idx, bmi)
results = np.stack((mf_array, ht_array, wt_array, idx_array, bmi_array), axis=-1)

# output file name
fname = os.getcwd() + r'.\data\miner-gender-vs-htwt.csv'

# save the file
np.savetxt(fname, results, fmt='%d,%d,%d,%d,%4.3f', header='Gender,Height,Weight,Index,BMI')
print('Write data to file\n')


#
# 5 - Removed variables from memory
#

del i
del t

del fname
del results
del dataset

del wt_array
del ht_array
del bmi_array
del idx_array
del mf_array

"""

del bmi_max
del bmi_min
del bmi_ptile

del wt_mean
del wt_median
del wt_std
del wt_var

"""

