import os, pickle
from multiprocessing import Pool, freeze_support

INDEXING_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/ae_w_128'
SEARCH_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/ae_w_128'

INDEXING_BIT_VECTOR_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/LSH/ae_w_128_bit_vector'
SEASRCH_BIT_VECTOR_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/LSH/ae_w_128_bit_vector'

BEFORE_SET_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/ae_w_128'
STORE_BIT_VECTOR_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/LSH/ae_w_128_bit_vector'

BIT_VECTOR_SIZE = 2770
PROCESS_NUMBER = 20

def read_set_data(file_path):
    with open(file_path, 'rb') as rpf:
        s = set(pickle.load(rpf))
        return s

def main_make_bit_vector(data_set):
    bitvector = [0 for _ in range(BIT_VECTOR_SIZE)]
    for each_set in data_set:
        ind = int(each_set, 16) % BIT_VECTOR_SIZE
        bitvector[ind] = 1
    return bitvector

def make_bit_vector(original_path, after_path): # only use process one
    for file_name in os.listdir(original_path):
        fp = os.path.join(original_path, file_name)
        data_set = read_set_data(fp)
        bit_vector = main_make_bit_vector(data_set)
        file_md5 = os.path.splitext(file_name)[0]
        store_path = os.path.join(after_path, file_md5 + '.bitvector')
        with open(store_path, 'wb') as wpf:
            pickle.dump(bit_vector, wpf)
    return

def make_bit_vector_mp(fp):
    data_set = read_set_data(fp)
    bit_vector = main_make_bit_vector(data_set)
    file_name = os.path.basename(fp)
    file_md5 = os.path.splitext(file_name)[0]
    store_path = os.path.join(STORE_BIT_VECTOR_PATH, file_md5 + '.bitvector')
    with open(store_path, 'wb') as wpf:
        pickle.dump(bit_vector, wpf)
    return

def get_section(search_set_path):
    path_files_list = []
    for file_name in os.listdir(search_set_path):
        fp = os.path.join(search_set_path, file_name)
        path_files_list.append(fp)
    return path_files_list

def run_mp():
    section_list = get_section(BEFORE_SET_PATH)
    freeze_support()
    with Pool(processes = PROCESS_NUMBER) as pool:
        pool.map(make_bit_vector_mp, section_list)
    return

if __name__ == '__main__':
    run_mp()
    # make_bit_vector(INDEXING_PATH, INDEXING_BIT_VECTOR_PATH)
