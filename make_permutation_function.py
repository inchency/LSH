import random, pickle

PERMUTATION_FUNCTION_PATH = r'/home/seclab/Documents/meseum_data/pickle_data/LSH/permutation_64.pickle'

FUNCTION_CNT = 64
AVERAGE_SHINGLE_SIZE = 2770

def make_permutation_function(function_cnt, average_shingle_size):
    original_vector = [i+1 for i in range(average_shingle_size)]
    final_permutation_list = []
    for i in range(function_cnt):
        permutation_vector = original_vector[:]
        random.shuffle(permutation_vector)
        make_index_vector = [0 for _ in range(average_shingle_size)]
        for i, each in enumerate(permutation_vector):
            make_index_vector[each-1] = i
        tup = (permutation_vector, make_index_vector)
        final_permutation_list.append(tup)
    return final_permutation_list


if __name__ == '__main__':
    # permutation_list = make_permutation_function(FUNCTION_CNT, AVERAGE_SHINGLE_SIZE)
    # with open(PERMUTATION_FUNCTION_PATH, 'wb') as wpf:
    #     pickle.dump(permutation_list, wpf)
    with open(PERMUTATION_FUNCTION_PATH, 'rb') as rpf:
        data = pickle.load(rpf)
    print(data)