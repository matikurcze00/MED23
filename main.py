import sys
from eclat import eclat
import argparse

from utils import load_dataset, save_file

def run(args):

    data = load_dataset(args.dataset)
    min_support = args.min_support
    vertical_database = eclat(data, args.processes_num, min_support)
   
    
    #Save file
    save_file(vertical_database)

if __name__ == '__main__':
    args = sys.argv[1:]
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', default="T10I4D100K.dat", type=str, help="Dataset name")
    parser.add_argument('--processes_num', default=6, type=int, help="Number of processors used in algorithm")
    parser.add_argument('--min_support', default=500, type=int, help="Minimal support")

    run(parser.parse_args(args))
    