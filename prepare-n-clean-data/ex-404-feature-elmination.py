# -*- coding: utf-8 -*-
"""

Name:
    ex404-feature-elmination.py
     
Design Phase:
    Author:   John Miner
    Date:     12-01-2019
    Purpose:  Read in iris data set.
              Explore how PCA works

    
"""



#
# 1 - Load data frame from csv file
#

# include the libraries
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

# Ignore weird copy error - must research later
pd.set_option('mode.chained_assignment', None)


# load data set
fn = r'C:\prepare-n-clean-data\data\fishers-iris-data.csv'
df = pd.read_csv(fn)

# Display status
print('Read file into data frame')
print('')


#
# 2 - Plot width vs length vs species ( 4 dims )
#

a = df[['Sepal.Length', 'Sepal.Width', 'Species']]
a.columns=['length', 'width', 'species']

b = df[['Petal.Length', 'Petal.Width', 'Species']]
b.columns=['length', 'width', 'species']


d = {'setosa': 'r', 'versicolor': 'y', 'virginica': 'b'}
a['colors'] = a['species'].map(d)

d = {'setosa': 'r', 'versicolor': 'y', 'virginica': 'b'}
b['colors'] = b['species'].map(d)

print('')
ax = a.plot(kind='scatter', x=0, y=1, c=a['colors'], title='plot sepal length vs width')

print('')
ax = b.plot(kind='scatter', x=0, y=1, c=b['colors'], title='plot petal length vs width')


#
# 3 - Combine x's and y's, scale n plot
#

x1 = a.loc[:, ['length', 'width']].values
y1 = a.loc[:, ['species']].values
x1 = StandardScaler().fit_transform(x1)

x2 = b.loc[:, ['length', 'width']].values
y2 = b.loc[:, ['species']].values
x2 = StandardScaler().fit_transform(x2)


z1 = np.concatenate((x1, y1), axis=1)
z2 = np.concatenate((x2, y2), axis=1)
z3 = np.concatenate((z1, z2), axis=0)

z4 = pd.DataFrame(z3)
z4.columns = ['length', 'width', 'species']

z4['length'] = z4['length'].astype(float)
z4['width'] = z4['width'].astype(float)

d = {'setosa': 'r', 'versicolor': 'y', 'virginica': 'b'}
z4['colors'] = z4['species'].map(d)

print('')
ax = z4.plot(kind='scatter', x='length', y='width', c=z4['colors'], title = 'scale petal, scale septal, combine' )


#
# 3 - Scale all 4 features
#


# Features
features = ['Sepal.Length', 'Sepal.Width', 'Petal.Length', 'Petal.Width']
x = df.loc[:, features].values

# Labels
y = df.loc[:,['Species']].values

# Scale
x = StandardScaler().fit_transform(x)


#
# 4 - Reduce dimensions
#

from sklearn.decomposition import PCA
pca = PCA(n_components=2)
s = pca.fit_transform(x)
t = pd.DataFrame(data = s, columns = ['component1', 'component2'])
z = pd.concat([t, df[['Species']]], axis = 1)



#
# 5 - Plot new features
#

d = {'setosa': 'r', 'versicolor': 'y', 'virginica': 'b'}
z['colors'] = z['Species'].map(d)


print('')
ax = z.plot(kind='scatter', x=0, y=1, c=z['colors'], title = 'component 1 vs component 2')


print('Explained variance')
print(pca.explained_variance_ratio_)
print(pca.explained_variance_ratio_[0] + pca.explained_variance_ratio_[1])
