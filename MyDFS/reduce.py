import pandas as pd
import numpy as np

def reduce(data):
    # data是一个DataFrame，index为key，列为['value']
    result = pd.DataFrame(columns=['key', 'value'])
    count = np.sum(data.loc['count', 'value'])
    sum1 = np.sum(data.loc['sum1', 'value'])
    sum2 = np.sum(data.loc['sum2', 'value'])
    result.loc[0] = ['mean', sum1/count]
    result.loc[1] = ['var', sum2/count-result.loc[0,'value']**2]
    result = result.set_index('key')
    return result