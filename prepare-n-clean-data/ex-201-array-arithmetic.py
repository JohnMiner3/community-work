# -*- coding: utf-8 -*-
"""

Name:
    ex-201-array-arithmetic.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Explore arithmetic with arrays.


"""

# include the library
import numpy as np

# Two same size integer arrays
x = np.array([1, 2, 3])
y = np.array([4, 5, 6])
print('x : %s' % x)
print('y : %s\n' % y)


# Array - Addition
r = x + y
print('x + y : %s\n' % r)


# Array - Substraction
r = x - y
print('x - y : %s\n' % r)


# Array - Multiplication
r = x * y
print('x * y : %s\n' % r)


# Array - Division
r = x / y
print('x / y : %s\n' % r)


# Array - Modulus
r = x % y
print('x % y : {0}\n'.format(r))


# Floor division
r = y // x
print('y // x : %s\n' % r)

