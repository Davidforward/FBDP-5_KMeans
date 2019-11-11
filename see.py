# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 10:53:07 2019

@author: Explorer
                          """

import pandas as pd
import matplotlib.pyplot as plt


data=pd.read_csv('part-m-00000',header=None)


plt.title('Scatter Diagram')
plt.plot(data[0],data[1],'ro')
plt.show()