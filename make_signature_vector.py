import os, pickle
from make_permutation_function import PERMUTATION_FUNCTION_PATH
from make_bit_vector import INDEXING_BIT_VECTOR_PATH, SEARCH_BIT_VECTOR_PATH

SIGNATURE_VECTOR_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/LSH/ae_w_128_signature_vector'
TARGET_BIT_VECTOR_PATH = INDEXING_BIT_VECTOR_PATH

def read_pickle_data(file_path):
    with open(file_path, 'rb') as rpf:
        data = pickle.load(rpf)
        return data

def make_signature_vector(file_path, permutation_data):
    bit_vector = read_pickle_data(file_path)
    signature_vector = []
    for each_tup in permutation_data:
        permutation_function_list = each_tup[0]
        index_permutation_list = each_tup[1]
        for i, target_index in enumerate(index_permutation_list):
            if bit_vector[target_index] == 1:
                signature_vector.append(permutation_function_list[i])
                break
    return signature_vector

if __name__== '__main__':
    permutation_data = read_pickle_data(PERMUTATION_FUNCTION_PATH)
    cnt = 0
    for file_name in os.listdir(TARGET_BIT_VECTOR_PATH):
        file_md5 = os.path.splitext(file_name)[0]
        fp = os.path.join(TARGET_BIT_VECTOR_PATH, file_name)
        store_file_path = os.path.join(SIGNATURE_VECTOR_PATH, file_md5 + '.signature_vector')
        signature_vector = make_signature_vector(fp, permutation_data)
        with open(store_file_path, 'wb') as wpf:
            pickle.dump(signature_vector, wpf)
            cnt += 1
            print(cnt)
