import batch_by_marcos as batch
import pandas as pd
from datetime import datetime
from deepdiff import DeepDiff


def dt(hour, minute, second=0):
    return datetime(2021, 1, 1, hour, minute, second)

def prepare_data():
    
    data = [
        (None, None, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, 1, dt(1, 2, 0), dt(1, 2, 50)),
        (1, 1, dt(1, 2, 0), dt(2, 2, 1)),        
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime']
    df = pd.DataFrame(data, columns=columns)

    return df

def prepare_result():

    data = [
        ('-1', '-1', dt(1, 2), dt(1, 10), 8.0),
        ('1', '1', dt(1, 2), dt(1, 10), 8.0),
    ]

    columns = ['PUlocationID', 'DOlocationID', 'pickup_datetime', 'dropOff_datetime', 'duration']
    df = pd.DataFrame(data, columns=columns)

    return df
  
def test_prepare_data():
    '''
    Test batch script function prepare_data.
    Note: The expected result may be generated be printed with  pytest ./test -s 
    '''

    df = prepare_data()
    categorical = ['PUlocationID', 'DOlocationID']
    actual_result = batch.prepare_data(df, categorical)
    expected_result = prepare_result()
    
    diff = DeepDiff(actual_result, expected_result,significant_digits=2)
    print(f'differences are: {diff}')
    
    assert 'type_changes' not in diff
    assert 'values_changes' not in diff




