# You must prepare pickle set file first

import os, pickle, hashlib

INDEXING_SET_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/ae_w_128'
SEARCH_SET_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/ae_w_128'
STORE_BUCKET_DIC_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/LSH/BUCKET_H_144_DIC.pickle'
RB_SIZE = 144 # R_SIZE * B_SIZE
R_SIZE = 12
B_SIZE = 12
BUCKET_DIC = {}

def read_set_data(file_path):
    with open(file_path, 'rb') as rpf:
        s = set(pickle.load(rpf))
        return s

def make_min_hash(feature_set):
    deci_minhash_list = list()
    for i in range(1, RB_SIZE + 1):
        min_hashed_feature = None
        for j, element in enumerate(feature_set):
            salt_feature = str(i) + str(i) + element + str(i) + str(i)
            hashed_feature = int(hashlib.md5(salt_feature.encode()).hexdigest(), 16)
            if not min_hashed_feature:
                min_hashed_feature = hashed_feature
            elif min_hashed_feature > hashed_feature:
                min_hashed_feature = hashed_feature
        min_hashed_feature = hex(min_hashed_feature)[2:].rjust(32, '0')
        deci_minhash_list.append(min_hashed_feature)
    return deci_minhash_list

def into_bucket(min_hash_list, r_size, file_name_md5):
    r_concat = ''
    for i, element in enumerate(min_hash_list):
        r_concat += element
        if i % r_size == r_size-1:
            if r_concat not in BUCKET_DIC.keys():
                BUCKET_DIC[r_concat] = {file_name_md5}
            else:
                BUCKET_DIC[r_concat].add(file_name_md5)
            r_concat = ''
    return

def indexing_bucket(indexing_set_path):
    for file_name in os.listdir(indexing_set_path):
        file_name_md5 = file_name.split('.')[0]
        fp = os.path.join(indexing_set_path, file_name)
        feature_set = read_set_data(fp)
        min_hash_list = make_min_hash(feature_set)
        into_bucket(min_hash_list, R_SIZE, file_name_md5)
        print(len(BUCKET_DIC))
    with open(STORE_BUCKET_DIC_PATH, 'wb') as wpf:
        pickle.dump(BUCKET_DIC, wpf)

def read_bucket(file_path):
    with open(file_path, 'rb') as rpf:
        data = pickle.load(rpf)
        return data

def search_bucket(search_set_path, r_size):
    for file_name in os.listdir(search_set_path):
        indexing_collision_max_cnt_dic = {}
        file_name_md5 = file_name.split('.')[0]
        fp = os.path.join(search_set_path, file_name)
        feature_set = read_set_data(fp)
        min_hash_list = make_min_hash(feature_set)
        r_concat = ''
        for i, element in enumerate(min_hash_list):
            r_concat += element
            if i % r_size == r_size - 1:
                if r_concat in BUCKET_DIC.keys():
                    collision_set = BUCKET_DIC[r_concat]
                    for collision_file_md5 in collision_set:
                        if collision_file_md5 not in indexing_collision_max_cnt_dic.keys():
                            indexing_collision_max_cnt_dic[collision_file_md5] = 1
                        else:
                            indexing_collision_max_cnt_dic[collision_file_md5] += 1
                r_concat = ''
        sort_data = sorted(indexing_collision_max_cnt_dic.items(), key=lambda x:x[1])
        try:
            max_collision_file_md5 = sort_data[0][0]
            print(max_collision_file_md5, sort_data[0][1], file_name_md5)
        except:
            print(sort_data)

if __name__ == '__main__':
    # indexing_bucket(INDEXING_SET_PATH)
    BUCKET_DIC = read_bucket(STORE_BUCKET_DIC_PATH)
    search_bucket(SEARCH_SET_PATH, R_SIZE)
