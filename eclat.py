import pandas as pd 
from itertools import  chain, combinations
from spicy import special
from multiprocessing import Process, Manager, Lock
import time


#1st step
def initial_count_L2(data, min_support, lock, global_count_dict, transactions_list, proc_id):
    transactions = []
    for index, row in data.iterrows():
        # Include all non-missing items in the transaction
        transaction = row.dropna().astype(str).tolist()
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
    frequent_itemsets = {itemset: support  for itemset, support in itemset_support.items()}
    with lock:
        for itemset, support in frequent_itemsets.items():
            if itemset in global_count_dict:
                global_count_dict[itemset] += support
            else:
                global_count_dict[itemset] = support
        transactions_list[proc_id] = transactions
        

def find_lowest_scheduling_value(value_list):
    min_id = 0
    min_value = value_list[0]
    for i in range(1,len(value_list)):
        if value_list[i] < min_value:
            min_id = i
            min_value = value_list[i]
    return min_id

def create_schedule_L2(global_count_dict, processes):
    schedule_list = []
    proc_schedule_value = []
    for i in range(processes) :
        proc_schedule_value.append(0)
        schedule_list.append([])

    #Creating schedule
    grouped_items = {}
    for itemset, support in global_count_dict.items():
        first_element = itemset[0]
        if first_element not in grouped_items:
            grouped_items[first_element] = {'total': 0, 'keys': []}
        grouped_items[first_element]['total'] += support
        grouped_items[first_element]['keys'].append((itemset))

    for group in grouped_items.values():
        min_id = find_lowest_scheduling_value(proc_schedule_value)
        proc_schedule_value[min_id] += special.binom( group['total'], 2)
        schedule_list[min_id].append(group['keys'])
    return schedule_list

def vertical_database_transformation(tramsactions_chunk, data_chunk_size, proc_id,L2_elements, lock, vertical_database, transaction_count):
    additional_id_value = proc_id * data_chunk_size
    # Iterate through each transaction and update the dictionaries
    local_vertical_database = {}
    local_transaction_cout = {}
    for i, transaction in enumerate(tramsactions_chunk):
        for itemset in L2_elements:
            if all(str(item) in map(str, transaction) for item in itemset):
                if itemset in local_vertical_database:
                    local_vertical_database[itemset].add(i+additional_id_value)
                else:
                    local_vertical_database[itemset] = {i+additional_id_value}
        for element in transaction:
            if element in local_transaction_cout:
                local_transaction_cout[element].add(i+additional_id_value)
            else:
                local_transaction_cout[element] = {i+additional_id_value}


    
    with lock:
        for key, value in local_vertical_database.items():
            if key in vertical_database:
                temp_value = vertical_database[key]
                temp_value.update(value)
                vertical_database[key] = temp_value
            else:
                vertical_database[key] = value
        for key, value in local_transaction_cout.items():
            if key in transaction_count:
                temp_value = transaction_count[key]
                temp_value.update(value)
                transaction_count[key] = temp_value
            else:
                transaction_count[key] = value 
        
def are_first_k_elements_equal(tuple1, tuple2, k):
    return tuple1[:k] == tuple2[:k]

def calculate(Lk,vertical_database, k, min_support):

    local_vertical_database = dict(vertical_database)
    new_local_vertical_database = compute_frequent(Lk,local_vertical_database, k, min_support )

    vertical_database.update(new_local_vertical_database)


def compute_frequent(Lk,local_vertical_database, k, min_support):
    itemsets_dict = {}
    for Ek in Lk:
        if len(Ek) > 1:
            Lk_1_list = []
            temp_vertical_database = {}
            for i in range(len(Ek)-1) : #Checking all Ek-1
                Ek_temp = []
                for j in range(i+1, len(Ek)):
                    common_elements = local_vertical_database[Ek[i]] & local_vertical_database[Ek[j]]
                    if len(common_elements) > min_support:
                        new_key = Ek[i] + (Ek[j][-1],)
                        Ek_temp.append(new_key)
                        temp_vertical_database[new_key] = common_elements
                if Ek_temp:
                    Lk_1_list.append(Ek_temp)
            k_1 = k + 1
            if Lk_1_list :
                new_temp_vertical_database = compute_frequent(Lk_1_list, temp_vertical_database, k_1, min_support)
                itemsets_dict.update(new_temp_vertical_database)
                itemsets_dict.update(temp_vertical_database)
    return itemsets_dict
        

def eclat(data, processes, min_support):
    #Global distribution using a shared manager
    with Manager() as manager:
        start_time = time.time()
        global_dict = manager.dict()
        if processes > len(data) : processes = len(data)
        data_chunk_size = len(data) // processes
        rest_data = len(data) % processes
        data_chunks = [data[i * data_chunk_size:(i + 1) * data_chunk_size] for i in range(processes)]
        if rest_data > 0:
            data_chunks[-1] = pd.concat([data_chunks[-1], data[processes * data_chunk_size:]])
        
        lock = Lock()
        processes_list = []
        
        transactions_list = manager.dict()
        transaction_count = manager.dict()
        for i in range (processes):
            proc = Process(target = initial_count_L2, args=(data_chunks[i], min_support, lock, global_dict, transactions_list, i))
            processes_list.append(proc)
            proc.start()

        for proc in processes_list:
            proc.join()
        k = 2
        keys_to_remove = [key for key, value in global_dict.items() if value < min_support]
        for key in keys_to_remove:
            del global_dict[key]
        global_dict = dict(sorted(global_dict.items()))
        schedule_list = create_schedule_L2(global_dict, processes)

        
        L2_elements = list(global_dict.keys())        
        vertical_database = manager.dict()
        processes_list = []
        
        for i in range (processes):
            proc = Process(target = vertical_database_transformation, args=(transactions_list[i], data_chunk_size, i,L2_elements, lock, vertical_database, transaction_count))
            processes_list.append(proc)
            proc.start()

        for proc in processes_list:
            proc.join()


        processes_list = []
        for i in range (processes):
            proc = Process(target = calculate, args=(schedule_list[i], vertical_database,k , min_support))
            processes_list.append(proc)
            proc.start()
        
        for proc in processes_list:
            proc.join()

        return dict(vertical_database), dict(transaction_count)



def subsets(arr):
    return chain(*[combinations(arr, i) for i in range(1, len(arr))])

def intersection(subset,transaction_count_dict ):
    sets_for_subset_elements = []
        
    # Iterate through each element in the subset tuple
    for element in subset:
        if element in transaction_count_dict:
            # Add the set corresponding to the current element to the list
            sets_for_subset_elements.append(transaction_count_dict[element])
    
    # Intersect all the sets in the list
    if sets_for_subset_elements:
        intersection_of_sets = set.intersection(*sets_for_subset_elements)
    else:
        intersection_of_sets = set()
    
    # Set subset_support as the length of the intersection of sets
    return len(intersection_of_sets)

def calculate_confidence(itemset, subset, frequent_itemsets_dict, transaction_count_dict, min_confidence):
    itemset_support = len(frequent_itemsets_dict[itemset])
    if subset in frequent_itemsets_dict:
        subset_support = len(frequent_itemsets_dict[subset])
    else :
        subset_support = intersection(subset, transaction_count_dict)
        

    if subset_support and subset_support != 0:
        confidence = itemset_support / subset_support
        if confidence > min_confidence:
            subset_str = ', '.join(sorted(subset))
            itemset_str = ', '.join(sorted(itemset))
            confidence = round(confidence, 2)
            return (subset_str,  itemset_str, confidence)
        else :
            None
    return None

def rule_generation_worker(itemsets, frequent_itemsets_dict, transaction_count_dict, results, min_confidence):
    for itemset in itemsets:
        for subset in subsets(itemset):
            rule = calculate_confidence(itemset, frozenset(subset), frequent_itemsets_dict, transaction_count_dict, min_confidence)
            if rule:
                results.append(rule)

def generate_rules_multithread(vertical_database, transaction_count, num_processes=4, min_confidence = 0.7):
    with Manager() as manager:
        frequent_itemsets_dict = manager.dict(vertical_database)
        transaction_count_dict = manager.dict(transaction_count)
        results = manager.list()

        itemsets = list(vertical_database.keys())

        # Creating pool of processes

        # Splitting the itemsets into chunks for each process
        chunk_size = len(itemsets) // num_processes 
        rest_data = len(itemsets) % num_processes
        chunks = [itemsets[i:i + chunk_size] for i in range(0, len(itemsets), chunk_size)]
        if rest_data > 0:
            chunks[-1].extend(itemsets[num_processes * chunk_size:])

        # Start the worker processes
        processes_list = []
        for i in range (num_processes):
            proc = Process(target = rule_generation_worker, args = (chunks[i], frequent_itemsets_dict, transaction_count_dict, results, min_confidence))
            processes_list.append(proc)
            proc.start()
        
        for proc in processes_list:
            proc.join()

        return list(results)            
