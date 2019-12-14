# -*- coding: utf-8 -*-
"""

Name:
    ex-202-slicing-arrays.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Explore slicing arrays.
    
    Note:     Data is from kaggle male bmi set.

"""


# include the library
import numpy as np

# weight in kg
wt_lst = [96,87,61,104,92,111,90,81,51,79]

# height in cm
ht_lst = [174,189,149,189,147,154,174,195,155,191]

# create 2d array
study = np.array([wt_lst, ht_lst])

# Show the data
print(study)
print('')

#
# Indexing
#

# index each item
w3 = study[0,2]
h3 = study[1,2]
print('1 - The third person has a wgt of {0} kg and height of {1} cm.\n'.format(w3, h3))

# index as a row
p3 = study[:,2]
print('2 - The third person has a wgt of %s kg and height of %s cm.\n' % (p3[0], p3[1]))


#
# Slicing 
#

# bmi = weight (kg) ÷ height2 (m2)

# pull out single dimension arrays
wt_array = study[0,:]
ht_array = study[1,:]

# broadcasting - expands number to array
ht_array = ht_array / 100

# new bmi array
bmi = wt_array / (ht_array * ht_array)

# show results
print('')
for i in range(0, bmi.size):
    print ("participant # %s" % str(i+1))
    print ("    weight (kg) %s" % wt_array[i])
    print ("    height (m) %s" % ht_array[i])
    print ("    bmi %0.2f\n" %bmi[i])

# results
males = np.stack((np.asfarray(wt_array), np.asfarray(ht_array * 100), bmi))
print(males)
print('')


#
# Subsetting 
#

print('Subsetting, finding elements where wt > 90')

# 4 items match this condition
tf_array = wt_array > 90
print ("True/False Array : %s" % tf_array)

# sub setting in action
sub_array = wt_array[wt_array > 90]
print ("Sub Array : %s" % sub_array)    

