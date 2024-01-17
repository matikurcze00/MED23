from collections import defaultdict
import pandas as pd 
from itertools import combinations
from spicy import special
from multiprocessing import Process, Manager, Lock
import time


#1st step
def initial_count_L2(data, min_support, lock, global_count_dict, transactions_list, proc_id):
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
    frequent_itemsets = {itemset: support  for itemset, support in itemset_support.items()}
    #Descending order
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

def vertical_database_transformation(tramsactions_chunk, data_chunk_size, proc_id,L2_elements, lock, vertical_database):
    additional_id_value = proc_id * data_chunk_size
    # Iterate through each transaction and update the dictionaries
    local_vertical_database = {}
    for i, transaction in enumerate(tramsactions_chunk):
        for itemset in L2_elements:
            if all(str(item) in map(str, transaction) for item in itemset):
                if itemset in local_vertical_database:
                    local_vertical_database[itemset].add(i+additional_id_value)
                else:
                    local_vertical_database[itemset] = {i+additional_id_value}

    
    with lock:
        for key, value in local_vertical_database.items():
            if key in vertical_database:
                temp_value = vertical_database[key]
                temp_value.update(value)
                vertical_database[key] = temp_value
            else:
                vertical_database[key] = value
        
def are_first_k_elements_equal(tuple1, tuple2, k):
    return tuple1[:k] == tuple2[:k]

def async_compute_frequent(Lk,vertical_database, k, min_support) :
    start_time = time.time()
    results = compute_frequent(Lk,vertical_database, k, min_support)
    start_time = time.time()
    for key, item in results.items():
        vertical_database[key] = item
    print(f'adding lasted: {time.time() - start_time}')

def compute_frequent(Lk,vertical_database, k, min_support):
    local_updates = {}
    for Ek in Lk:
        if len(Ek) > 1:
            Lk_1_list = []
            # temp_vertical_database = {} #dodanie nowych wartosci
            for i in range(len(Ek)-1) : #Checking all Ek-1
                Ek_temp = []
                for j in range(i+1, len(Ek)):
                    if are_first_k_elements_equal(Ek[i], Ek[j], k-1):
                        common_elements = vertical_database[Ek[i]] & vertical_database[Ek[j]]
                        if len(common_elements) > min_support:
                            new_key = Ek[i] + (Ek[j][-1],)
                            Ek_temp.append(new_key)
                            local_updates[new_key] = common_elements
                    else :
                        break
                if Ek_temp:
                    Lk_1_list.append(Ek_temp)
            new_k = k + 1
            if Lk_1_list :
                # for itemset, tid_list in temp_vertical_database.items():
                #     vertical_database[itemset] = tid_list
                local_updates.update(compute_frequent(Lk_1_list, local_updates, new_k, min_support))
    return local_updates

def enclat(data, processes, min_support):
    #Global distribution using a shared manager
    print("start")
    with Manager() as manager:
        global_dict = manager.dict()
        if processes > len(data) : processes = len(data)
        data_chunk_size = len(data) // processes
        rest_data = len(data) % processes
        data_chunks = [data[i * data_chunk_size:(i + 1) * data_chunk_size] for i in range(processes)]
        if rest_data > 0:
            data_chunks[-1] = pd.concat([data_chunks[-1], data[processes * data_chunk_size:]])
        
        lock = Lock()
        processes_list = []
        start_time = time.time()
        transactions_list = manager.dict()
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
        print("global dict")
        # print(global_dict)
        print(len(global_dict.keys()))
        #list_of_list
        schedule_list = create_schedule_L2(global_dict, processes)
        print ("schedule_list")
        # print (schedule_list)
        
        L2_elements = list(global_dict.keys())        
        vertical_database = manager.dict()
        processes_list = []
        for i in range (processes):
            proc = Process(target = vertical_database_transformation, args=(transactions_list[i], data_chunk_size, i,L2_elements, lock, vertical_database))
            processes_list.append(proc)
            proc.start()

        for proc in processes_list:
            proc.join()

        print("VERTICAL DB")
        print(len(vertical_database.keys()))
        end_time = time.time()

        print(f'TIME : {end_time - start_time}')
        start_time = time.time()
        processes_list = []
        for i in range (processes):
            proc = Process(target = async_compute_frequent, args=(schedule_list[i], vertical_database, k, min_support))
            processes_list.append(proc)
            proc.start()

        for proc in processes_list:
            proc.join()
        print("VERTICAL DB")
        print(len(vertical_database.keys()))

        end_time = time.time()
        print(f'TIME : {end_time - start_time}')


                
