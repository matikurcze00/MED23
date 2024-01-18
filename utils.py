import os
import pandas as pd

def save_file(association_rules):
    output_directory = 'output'
    # Create the directory if it doesn't exist
    if not os.path.exists(output_directory):
        os.makedirs(f'output')
    output_file_path = 'rules.txt'

    # Write the list to the file
    with open(f'{output_directory}\{output_file_path}', 'w') as file:
        for rule in association_rules:
            file.write(str(rule))
            file.write('\n')

def load_dataset(dataset_name):
    data_list = []
    
    # Open the file and read line by line
    with open(dataset_name, 'r') as file:
        for line in file:
            # Split each line by space (or your delimiter)
            split_line = line.strip().split(' ')
            # Append the split line (which is a list) to our data list
            data_list.append(split_line)

    # Convert the list of lists into a DataFrame
    return pd.DataFrame(data_list)