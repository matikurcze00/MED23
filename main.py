import sys
from eclat import eclat, generate_rules_multithread
import argparse

from utils import load_dataset, save_file

def run(args):

    data = load_dataset(args.dataset)
    min_support = args.min_support
    vertical_database, transaction_count = eclat(data, args.processes_num, min_support)
    print(f'Number of frequent itemsets: {len(vertical_database.keys())}')
    rules = generate_rules_multithread(vertical_database, transaction_count, args.processes_num, args.min_confidence)
    print(f'Number of rules: {len(rules)}')

    #Save file
    save_file(rules)

if __name__ == '__main__':
    args = sys.argv[1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default="test_dataset.dat", type=str, help="Dataset name")
    parser.add_argument('--processes_num', default=6, type=int, help="Number of processors used in algorithm")
    parser.add_argument('--min_support', default=9, type=int, help="Minimal support")
    parser.add_argument('--min_confidence', default=0.7, type=float, help="Minimal confidence of rule")

    run(parser.parse_args(args))
    