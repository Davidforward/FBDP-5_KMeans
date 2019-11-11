# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 10:53:07 2019

@author: Explorer
                          """

import pandas as pd
import matplotlib.pyplot as plt

K=3
colors = ['g','green','red','#054E9F','orange']


data=pd.read_csv('part-m-00000',header=None,sep='\t')
data['01']=data[0].map(lambda x:float(x.split(',')[0]))
data['02']=data[0].map(lambda x:float(x.split(',')[1]))


for i in range(1,K+1):
    num1 = data.loc[data[1] == i]['01']
    num2 = data.loc[data[1] == i]['02']
    plt.scatter(num1, num2, c=colors[i])

plt.title('K=3,iteration=18')
'''
plt.plot(data['01'],data['02'],'ro')
'''
plt.show()