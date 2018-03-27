import pandas as pd
import numpy as np
from utility import aggregate_periods

pairs = ['EURJPY', 'EURUSD', 'USDJPY']
times = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007',
         '2008', '2009', '2010', '2011', '2012', '2013', '2014', '2015',
         '2016', '2017', '201801', '201802']

for pair in pairs:
    print('Processing %s...' % pair)
    file_stem = 'fx_data/' + pair + '/raw/DAT_ASCII_' + pair + '_M1_'
    if pair == 'EURJPY':
        year_range = times[2:]
    else:
        year_range = times

    df = pd.read_csv(file_stem + year_range[0] + '.csv', sep=';', header=None)
    for year in year_range[1:]:
        print('\tReading %s data...' % year)
        new = pd.read_csv(file_stem + year + '.csv', sep=';', header=None)
        df = pd.concat([df, new], axis=0)

    df.columns = ['time', 'open', 'high', 'low', 'close', 'volume']
    df['time'] = pd.to_datetime(df['time'], infer_datetime_format=True)
    df = df.set_index('time')
    del df['volume']

    print('\tAggregating data...')
    agg1hr = aggregate_periods(df, period=60)
    agg1day = aggregate_periods(df, period=1440)

    print('\tSaving data...')
    save_name = 'fx_data/' + pair + '/' + pair + '_' \
                + year_range[0] + '-' + year_range[-1]
    df.to_csv(save_name + '_min.csv', index=True)
    agg1hr.to_csv(save_name + '_hour.csv', index=True)
    agg1day.to_csv(save_name + '_day.csv', index=True)
