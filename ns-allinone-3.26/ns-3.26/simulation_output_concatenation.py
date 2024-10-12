import os
import numpy as np
import sys
import pandas as pd
from collections import defaultdict

def read_output_files(directory_path):
    # List to store file data
    one_ue_data = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'MCS', 'Peak(Mbps)_1UE', 'Avg(Mbps)_1UE'])
    three_ue_files = defaultdict(list)
    three_ue_data = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'MCS', 'Peak(Mbps)_PerUE', 'Avg(Mbps)_PerUE','Peak(Mbps)_System', 'Avg(Mbps)_System'])

    # Check if the directory exists
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist.")
        return

    # Iterate over all files in the directory
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        if filename.endswith('.dat') and '1ue' in filename:
            mcs = 'HtMcs7' if 'HtMcs7' in filename else 'HtMcs1'
            rw = 64000 if 'rw64k' in filename else 1024000
            rtt = 30 if 'rtt30' in filename else 200
            peak = np.max(np.loadtxt(file_path)[:, 1])
            avg = np.mean(np.loadtxt(file_path)[:, 1])
            new_row = {
                'RTT(ms)': rtt,
                'RWND(Bytes)': rw,
                'MCS': mcs,
                'Peak(Mbps)_1UE': peak,
                'Avg(Mbps)_1UE': avg
            }
            one_ue_data = one_ue_data.append(new_row, ignore_index=True)
        if filename.endswith('.dat') and '3ue' in filename:
            mcs = 'HtMcs7' if 'HtMcs7' in filename else 'HtMcs1'
            rw = 64000 if 'rw64k' in filename else 1024000
            rtt = 30 if 'rtt30' in filename else 200
            three_ue_files[(mcs, rw, rtt)].append(np.loadtxt(file_path))

    # Handle the case of three UE
    for key, value in three_ue_files.items():
        mcs, rw, rtt = key
        stacked_data = np.vstack(value)
        perue_peak = np.max(stacked_data[:, 1])
        perue_avg = np.mean(stacked_data[:, 1])

        dfs = [pd.DataFrame(arr, columns=['Key', 'Value']) for arr in value]
        result = dfs[0].set_index('Key')
        for df in dfs[1:]:
            result = result.add(df.set_index('Key'), fill_value=0)
        result = result.reset_index()
        system_peak = np.max(result['Value'])
        system_avg = np.mean(result['Value'])

        new_row = {
            'RTT(ms)': rtt,
            'RWND(Bytes)': rw,
            'MCS': mcs,
            'Peak(Mbps)_PerUE': perue_peak,
            'Avg(Mbps)_PerUE': perue_avg,
            'Peak(Mbps)_System': system_peak,
            'Avg(Mbps)_System': system_avg
        }
        three_ue_data = three_ue_data.append(new_row, ignore_index=True)
    
    return one_ue_data, three_ue_data

def concatenate_data(one_ue_data, three_ue_data):
    index = {'RTT(ms)': [30, 200, 30, 200, 30, 200, 30, 200], 'RWND(Bytes)': [64000, 64000, 1024000, 1024000, 64000, 64000, 1024000, 1024000], 'MCS': ['HtMcs1', 'HtMcs1', 'HtMcs1', 'HtMcs1', 'HtMcs7', 'HtMcs7', 'HtMcs7', 'HtMcs7']}
    index_df = pd.DataFrame(index)
    
    # Perform left join of one_ue_data with index_df
    one_ue_merged = pd.merge(index_df, one_ue_data, on=['RTT(ms)', 'RWND(Bytes)', 'MCS'], how='left')
    
    # Perform left join of three_ue_data with index_df
    three_ue_merged = pd.merge(index_df, three_ue_data, on=['RTT(ms)', 'RWND(Bytes)', 'MCS'], how='left')
    
    # Combine the merged dataframes
    result = pd.concat([one_ue_merged, three_ue_merged], axis=1)
    
    # Remove duplicate columns
    result = result.loc[:,~result.columns.duplicated()]
    
    return pd.concat([one_ue_data, three_ue_data], ignore_index=True)

if __name__ == '__main__':
    # Check if the directory path is provided in the command line
    if len(sys.argv) > 1:
        output_directory = sys.argv[1]
    else:
        output_directory = './output'
    
    # Call the function to read and process files
    one_ue_data, three_ue_data = read_output_files(output_directory)
    final_data = concatenate_data(one_ue_data, three_ue_data)
    final_data.to_csv('problem_3_table.txt', sep=' ', index=False)
    