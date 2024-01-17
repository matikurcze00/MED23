import sys
import pandas as pd
from eclat import enclat 
import argparse

def run(args):
    # data = dataset_generation.__dict__[args.dataset]()
    min_support = 1000
    processes = 1
    data = pd.read_csv('chess.dat', delimiter=' ', header=None)
    enclat(data, processes, min_support)
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
    