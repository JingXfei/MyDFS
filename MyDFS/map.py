import numpy as np
import pandas as pd

def map(data):
    # data是一个DataFrame，index为key，列为['value']
    result = pd.DataFrame(columns = ['key', 'value'])
    result.loc[0] = ['count', data.shape[0]]
    result.loc[1] = ['sum1', np.sum(data['value'].astype('float64'))]
    result.loc[2] = ['sum2', np.sum(data['value'].astype('float64')**2)]
    result = result.set_index('key')
    return result