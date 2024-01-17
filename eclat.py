from collections import defaultdict
import pandas as pd 
from itertools import combinations
from spicy import special
from multiprocessing import Process, Manager, Lock
import time

#Loading data

min_support = 3
max_length = 4
processes = 2
data = pd.read_csv('test_dataset.dat', delimiter=' ', header=None)

#Global distribution using a shared manager
# with Manager() as manager:
#     global_count_dist = manager.list()
#     if processes > len(data) : processes = len(data)
#     data_chunk_size = len(data) // processes
#     rest_data = len(data) % processes
#     data_chunks = [data[i * data_chunk_size:(i + 1) * data_chunk_size] for i in range(processes)]
#     data_chunks[-1].extend(data[processes * data_chunk_size:processes * data_chunk_size + rest_data])


#1st step
transactions = []
for index, row in data.iterrows():
    # Include all non-missing items in the transaction
    transaction = row.dropna().astype(int).tolist()
    transactions.append(transaction)

# Find frequent 2-itemsets
itemset_support = {}
for transaction in transactions:
    for itemset in combinations(transaction, 2):  
        if itemset in itemset_support:
            itemset_support[itemset] += 1
        else:
            itemset_support[itemset] = 1

# Calculate support for each itemset
frequent_itemsets = {itemset: support  for itemset, support in itemset_support.items() if support  >= min_support}
#Descending order
frequent_itemsets = dict(sorted(frequent_itemsets.items()))
print('frequent_itemsets')
print(frequent_itemsets)

#Agregate
# SUMMING
#2nd phase

#Part 1
def find_lowest_scheduling_value(value_list):
    min_id = 0
    min_value = value_list[0]
    for i in range(1,len(value_list)):
        if value_list[i] < min_value:
            min_id = i
            min_value = value_list[i]
    return min_id

list_of_list = []
value = []
for i in range(processes) :
    value.append(0)
    list_of_list.append([])

#Creating schedule
grouped_items = {}
for key, val in frequent_itemsets.items():
    first_element = key[0]
    if first_element not in grouped_items:
        grouped_items[first_element] = {'total': 0, 'keys': []}
    grouped_items[first_element]['total'] += val
    grouped_items[first_element]['keys'].append(list(key))

for group in grouped_items.values():
    min_id = find_lowest_scheduling_value(value)
    value[min_id] += special.binom( group['total'], 2)
    list_of_list[min_id].append(group['keys'])


# for i in range(processes):
#     print(list_of_list[i])
#     print(value[i])

#Part 2 
itemset_transactions_list = []
for process_list in list_of_list[0]:
    # Create itemsets as tuples
    itemsets = set(tuple(sublist) for sublist in process_list)
    # Sort the itemsets
    sorted_itemsets = sorted(itemsets, key=lambda x: x)
    # Create dictionary with sorted itemsets
    itemset_transactions = {itemset: set() for itemset in sorted_itemsets}
    itemset_transactions_list.append(itemset_transactions)

# Iterate through each transaction and update the dictionaries
for i, transaction in enumerate(transactions):
    for itemset_transactions in itemset_transactions_list:
        for itemset in itemset_transactions:
            if all(str(item) in map(str, transaction) for item in itemset):
                itemset_transactions[itemset].add(i)

# print(itemset_transactions_list)


Final_Lk = []

#Step 3
#Asynchronous Phase
keys = list(itemset_transactions_list[0].keys())

# for i in range (len(keys)):
#     print(itemset_transactions_list[0][keys[i]])

def are_first_k_elements_equal(tuple1, tuple2, k):
    return tuple1[:k] == tuple2[:k]

# def partition_Lk(Lk, k):
#     grouped_dicts = defaultdict(list)
#     for L in Lk:
#         for key in L.keys():
#             group_key = key[:k]  # Extract the first k elements of the tuple
#             grouped_dicts[group_key].append(L)

#     # Convert grouped_dicts to a list of lists
#     return list(grouped_dicts.values())


def Compute_Frequent(Ek, k, final_itemsets):
    keys = list(Ek.keys())
    Lk_list = []
    for i in range(len(keys)-1) : #Checking all Ek-1
        Ek_temp = {}
        for j in range(i+1, len(keys)):
            if are_first_k_elements_equal(keys[i], keys[j], k-1):
                common_elements = list(set(Ek[keys[i]]) & set(Ek[keys[j]]))
                if len(common_elements) > min_support:
                    new_key = keys[i] + (keys[j][-1],)
                    Ek_temp[new_key] = common_elements
            else :
                break
        if Ek_temp:
            Lk_list.append(Ek_temp)
    new_k = k + 1
    if not Lk_list :
        final_itemsets.append(Ek)        
    elif new_k == max_length :
        final_itemsets.append(Lk_list)
    # else :
    #     Lk_list = partition_Lk(Lk_list_temp, k-1)
    else :    
        for itemset in Lk_list:
            Compute_Frequent(itemset, k+1, final_itemsets)


final_itemsets = []
for itemset_list in itemset_transactions_list:
    k = 2
    Compute_Frequent(itemset_list, k, final_itemsets)

print(final_itemsets)
                
