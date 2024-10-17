import os
import numpy as np
import sys
import pandas as pd
from collections import defaultdict

def read_output_files(directory_path):
    # List to store file data
    one_ue_data = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)', 'Peak(Mbps)_1UE', 'Avg(Mbps)_1UE'])
    three_ue_files = defaultdict(list)
    three_ue_data_per_device = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)', 'Peak(Mbps)_PerUE', 'Avg(Mbps)_PerUE'])
    three_ue_data_system = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)', 'Peak(Mbps)_System', 'Avg(Mbps)_System'])
    udp_data = pd.DataFrame(columns=['RTT(ms)', 'BW(Hz)', 'Peak(Mbps)', 'Avg(Mbps)'])

    # Iterate over all files in the directory
    def find_start_end_time(data, filename):
        start, end = 0, len(data) - 1
        for i in range(len(data)):
            if data[i, 1] > 0:
                start = i
                break
        for i in range(len(data) - 1, -1, -1):
            if data[i, 1] > 0:
                end = i
                break
        if end == len(data) - 1:
            print("Warning: insufficient time for", filename)
        return start, end

    for filename in os.listdir(directory_path):
        try: 
            file_path = os.path.join(directory_path, filename)
            bw = 20 if 'bw20' in filename else 10
            rw = 64000 if 'rw64k' in filename else 1024000
            rtt = 30 if 'rtt30' in filename else 200
            if filename.endswith('.dat') and 'tcp' in filename and 'all' not in filename and 'MacStat' not in filename and '1ue' in filename:
                data = np.loadtxt(file_path)
                start, end = find_start_end_time(data, filename)
                peak = round(np.max(data[start:end, 1]), 2)
                avg = round(np.mean(data[start:end, 1]), 2)
                new_row = {
                    'RTT(ms)': rtt,
                    'RWND(Bytes)': rw,
                    'BW(Hz)': bw,
                    'Peak(Mbps)_1UE': peak,
                    'Avg(Mbps)_1UE': avg
                }
                one_ue_data = pd.concat([one_ue_data, pd.DataFrame([new_row])], ignore_index=True)
            
            # handle three ue files per device
            if filename.endswith('.dat') and 'tcp' in filename and 'all' not in filename and '3ue' in filename and 'MacStat' not in filename:
                bw = 20 if 'bw20' in filename else 10
                rw = 64000 if 'rw64k' in filename else 1024000
                rtt = 30 if 'rtt30' in filename else 200
                three_ue_files[(bw, rw, rtt)].append(np.loadtxt(file_path))
            
            if filename.endswith('.dat') and 'tcp' in filename and 'all' in filename and '3ue' in filename and 'MacStat' not in filename:
                data = np.loadtxt(file_path)
                start, end = find_start_end_time(data, filename)
                system_peak = round(np.max(data[start:end, 1]), 2)
                system_avg = round(np.mean(data[start:end, 1]), 2)
                bw = 20 if 'bw20' in filename else 10
                rw = 64000 if 'rw64k' in filename else 1024000
                rtt = 30 if 'rtt30' in filename else 200
                new_row = {
                    'RTT(ms)': rtt,
                    'RWND(Bytes)': rw,
                    'BW(Hz)': bw,
                    'Peak(Mbps)_System': system_peak, 
                    'Avg(Mbps)_System': system_avg
                }
                three_ue_data_system = pd.concat([three_ue_data_system, pd.DataFrame([new_row])], ignore_index=True)

            if filename.endswith('.dat') and 'udp' in filename and 'all' in filename and 'MacStat' not in filename:
                data = np.loadtxt(file_path)
                start, end = find_start_end_time(data, filename)
                peak = round(np.max(data[start:end, 1]), 2)
                avg = round(np.mean(data[start:end, 1]), 2)
                new_row = {
                    'RTT(ms)': rtt,
                    'BW(Hz)': bw,
                    'Peak(Mbps)': peak, 'Avg(Mbps)': avg
                }   
                udp_data = pd.concat([udp_data, pd.DataFrame([new_row])], ignore_index=True)
        except Exception as e:
            print(f"Error processing file {filename}: {e}")

    # Handle the case of three UE
    three_ue_data = pd.DataFrame(columns=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)', 'Peak(Mbps)_PerUE', 'Avg(Mbps)_PerUE','Peak(Mbps)_System', 'Avg(Mbps)_System'])
    for key, value in three_ue_files.items():
        bw, rw, rtt = key
        
        perue_peak = 0
        UE_averages = []
        for arr in value:
            start, end = find_start_end_time(arr, f"bw: {bw}, rw: {rw} bytes, rtt: {rtt} ms")
            perue_peak = np.max([perue_peak, np.max(arr[start:end, 1])])
            UE_averages.append(np.mean(arr[start:end, 1]))

        perue_avg = round(np.mean(UE_averages), 2)
        perue_peak = round(perue_peak, 2)

        new_row = {
            'RTT(ms)': rtt,
            'RWND(Bytes)': rw,
            'BW(Hz)': bw,
            'Peak(Mbps)_PerUE': perue_peak, 
            'Avg(Mbps)_PerUE': perue_avg
        }
        three_ue_data_per_device = pd.concat([three_ue_data_per_device, pd.DataFrame([new_row])], ignore_index=True)
    
    return one_ue_data, three_ue_data_per_device, three_ue_data_system, udp_data

def concatenate_data(one_ue_data, three_ue_data_per_device, three_ue_data_system):
    index = {'RTT(ms)': [30, 200, 30, 200, 30, 200, 30, 200], 
             'RWND(Bytes)': [64000, 64000, 1024000, 1024000, 64000, 64000, 1024000, 1024000], 
             'BW(Hz)': [20,20,20,20,10,10,10,10]}
    
    index_df = pd.DataFrame(index)
    
    # Perform left join of one_ue_data with index_df
    one_ue_merged = pd.merge(index_df, one_ue_data, on=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)'], how='left')
    
    # Perform left join of three_ue_data with index_df
    three_ue_data_per_device_merged = pd.merge(index_df, three_ue_data_per_device, on=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)'], how='left')
    three_ue_data_system_merged = pd.merge(index_df, three_ue_data_system, on=['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)'], how='left')
    
    # Combine the dataframes in the specified order, skipping concatenation with index_df
    result = pd.concat([
        one_ue_merged[['RTT(ms)', 'RWND(Bytes)', 'BW(Hz)', 'Peak(Mbps)_1UE', 'Avg(Mbps)_1UE']],
        three_ue_data_per_device_merged[['Peak(Mbps)_PerUE', 'Avg(Mbps)_PerUE']],
        three_ue_data_system_merged[['Peak(Mbps)_System', 'Avg(Mbps)_System']],
    ], axis=1)
    
    return result

if __name__ == '__main__':
    # Check if the directory path is provided in the command line
    if len(sys.argv) > 1:
        output_directory = sys.argv[1]
    else:
        output_directory = './output'
    
    # Call the function to read and process files
    one_ue_data, three_ue_data_per_device, three_ue_data_system, udp_data = read_output_files(output_directory)
    final_data = concatenate_data(one_ue_data, three_ue_data_per_device, three_ue_data_system)
    final_data.to_csv('proj3_problem_3_table.txt', sep=' ', index=False)
    udp_data.to_csv('proj3_problem_4_table.txt', sep=' ', index=False)

    