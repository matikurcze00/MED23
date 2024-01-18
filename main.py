import sys
import pandas as pd
from eclat import enclat 
import argparse
from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
import time

def run(args):
    # data = dataset_generation.__dict__[args.dataset]()
    # min_support = 2000
    # processes = 1
    data_list = []

    # Open the file and read line by line
    with open('T10I4D100K.dat', 'r') as file:
        for line in file:
            # Split each line by space (or your delimiter)
            split_line = line.strip().split(' ')
            # Append the split line (which is a list) to our data list
            data_list.append(split_line)

    # Convert the list of lists into a DataFrame
    data = pd.DataFrame(data_list)
    min_support_list = [50000]
    processes_list = [6, 4, 2, 1]
    for min_support in min_support_list:
        for processes in processes_list:
            print(f'Support : {min_support}')
            print(f'Processes : {processes}')
            enclat(data, processes, min_support)

    #Apriori
    # transactions = data.apply(lambda row: row.dropna().tolist(), axis=1)
    # start_time = time.time()
    # # Initialize TransactionEncoder
    # te = TransactionEncoder()

    # # Fit and transform the data
    # te_ary = te.fit(transactions).transform(transactions)
    # df = pd.DataFrame(te_ary, columns=te.columns_)

    # # Apply Apriori algorithm
    # frequent_itemsets = apriori(df, min_support=0.45, use_colnames=True)
    # end_time = time.time()
    # print(f'APRIORI TIME: {end_time - start_time}')
    # print(f'itemset len: {len(frequent_itemsets)}')
            
    # frequent_itemsets, global_count_dist = find_frequent_itemsets(data=data,num_processes=args.num_processes,min_support=args.min_support)

    # return association_rules

if __name__ == '__main__':
    args = sys.argv[1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default="dummy", type=str, help="Dataset name")
    parser.add_argument('--num_processes', default=1, type=int, help="Number of processors used for count distribution")
    parser.add_argument('--min_support', default=2, type=int, help="Minimal support")
    parser.add_argument('--min_confidence', default=0.7, type=float, help="Minimal confidence")

    run(parser.parse_args(args))
    