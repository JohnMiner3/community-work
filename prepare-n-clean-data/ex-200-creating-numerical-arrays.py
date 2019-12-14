# -*- coding: utf-8 -*-
"""

Name:
    ex-100-creating-numerical-arrays.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Creating numerical arrays with numpy.


"""

# Load module
import numpy as np

# Show version number
print(np.__version__)
print('')

#
# Create an integer array
#

# Integer list
k = [2, 3, 5, 7]
print ("type = %s" % type(k))
print ("list = %s\n" %k)

# Integer array
a = np.array(k)
print ("type = %s" % type(a))
print ("int array = %s\n" % a)



#
# Create a floating point array
#

# Floating point list
l = [3.0, 4.0, 6.0, 8.0]
print ("type = %s" % type(l))
print ("list = %s\n" %l)

# Floating point array
b = np.array(l)
print ("type = %s" % type(b))
print ("float array = %s\n" % b)

# array of zeros
e = np.zeros(10)
print ("type = %s" % type(e))
print ("float array = %s\n" % e)

# array of ones
f = np.ones(10)
print ("type = %s" % type(f))
print ("float array = %s\n" % f)

# Int list 2 floating point
g = np.asfarray(k)
print ("type = %s" % type(g))
print ("float array = %s\n" % g)

# As array of type x
i = np.asarray(l, dtype=np.int16)
print(i)
print('')


#
# Create a empty of full array
#

# Create an empty array
x = np.empty((3,4))
print(x)
print('')

# Create a full array
y = np.full((3,3),6)
print(y)
print('')


#
# Create sample data
#

# Same as range() list function
c = np.arange(100, 0, -10) 
print ("type = %s" % type(c))
print ("sample int array = %s\n" % c)

# Split distance from 0 to 2 into 5 pts
d = np.linspace(0, 2, 5)
print ("type = %s" % type(d))
print ("sample float array = %s\n" % d)

# Random data
h = np.random.normal(60.5, 15, 5000)
print("Normal height distribution with mean 60.5 inches and std of 15.\n%s\n" % h)
print("The data set has %s points." % h.size)
