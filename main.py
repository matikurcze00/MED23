import sys
from eclat import eclat, generate_rules_multithread
import argparse

from utils import load_dataset, save_file

def run(args):

    data = load_dataset(args.dataset)
    min_support = args.min_support
    vertical_database = eclat(data, args.processes_num, min_support)
    print("rules")
    min_confidence = args.min_confidence
    association_rules = generate_rules_multithread(vertical_database,args.processes_num,min_confidence)
    
    #Save file
    save_file(association_rules)

if __name__ == '__main__':
    args = sys.argv[1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default="test_dataset_2.dat", type=str, help="Dataset name")
    parser.add_argument('--processes_num', default=2, type=int, help="Number of processors used in algorithm")
    parser.add_argument('--min_support', default=500, type=int, help="Minimal support")
    parser.add_argument('--min_confidence', default=0.7, type=float, help="Minimal confidence")

    run(parser.parse_args(args))
    