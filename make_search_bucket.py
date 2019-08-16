import os, pickle, hashlib, json
from multiprocessing import Pool, freeze_support

INDEXING_SET_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/ae_w_128'
SEARCH_SET_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/ae_w_128'
INDEXING_SIGNATURE_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/LSH/ae_w_128_signature_vector'
SEARCH_SIGNATURE_PATH = r'/home/seclab/Documents/meseum_data/features/20181002_3000/LSH/ae_w_128_signature_vector'
STORE_BUCKET_DIC_PATH = r'/home/seclab/Documents/meseum_data/features/20181001/LSH/ORIGINAL_BUCKET_H_64_R_1_B_64_list.pickle'
MOST_JACCARD_INDEX_ROUND_TRUTH_PATH = r'/home/seclab/Documents/meseum_data/ground_truth/combine_ae_w_128_jaccard_index.json' # don't touch
SEARCH_REPORT_PICKLE_PATH = r'/home/seclab/Documents/meseum_data/pickle_data/combine_json_result_to_list_dic_20181002_3000_ae_w_128_ULMAN_LSH_RB_SIZE_64_R_SIZE_1_B_SIZE_64_check_all.pickle'
RB_SIZE = 64 # R_SIZE * B_SIZE
R_SIZE = 1
B_SIZE = 64 # Bucket count
BUCKET_SIZE = 2770
BUCKET_DIC = {}
PROCESS_NUMBER = 24

ONLY_COMPARE_MOST_SIMILAR_FILE = False
FEATURE_NAME = 'ae'

def jaccard_similarity(list1, list2):
    if list1 == None or list2 == None:
        return 0
    intersection_set = set(list1) & set(list2)
    union_set = set(list1) | set(list2)
    try:
        similarity = len(intersection_set) / len(union_set)
    except ZeroDivisionError:
        similarity = 0
    return similarity

def jaccard_containment(list1, list2):  # |A & B| / MIN(|A|, |B|)
    if list1 == None or list2 == None:
        return 0
    intersection_set = set(list1) & set(list2)
    # union_set = set(list1)
    union_set = len(set(list1)) > len(set(list2)) and set(list2) or set(list1)
    try:
        similarity = len(intersection_set) / len(union_set)
    except ZeroDivisionError:
        similarity = 0
    return similarity

def make_bucket(indexing_signature_path):
    # initial BUCKETS
    BUCKETS = []
    for _ in range(B_SIZE):
        bucket_element_list = [set() for i in range(BUCKET_SIZE)]
        BUCKETS.append(bucket_element_list)
    file_cnt = 1
    for file_name in os.listdir(indexing_signature_path):
        print(file_cnt)
        file_cnt += 1
        fp = os.path.join(indexing_signature_path, file_name)
        file_md5 = os.path.splitext(file_name)[0]
        with open(fp, 'rb') as rpf:
            signature_vector = pickle.load(rpf)
            r_concat = ''
            for i, element in enumerate(signature_vector):
                r_concat += str(element)
                if i % R_SIZE == R_SIZE - 1:
                    md5_hash = int(hashlib.md5(r_concat.encode()).hexdigest(), 16)
                    ind = md5_hash % BUCKET_SIZE
                    BUCKETS[i][ind].add(file_md5)
                    r_concat = ''

    # store pickle file
    with open(STORE_BUCKET_DIC_PATH, 'wb') as wpf:
        pickle.dump(BUCKETS, wpf)

def load_pickle_file(file_path):
    with open(file_path, 'rb') as rpf:
        data = pickle.load(rpf)
        return data
    
def search_bucket(buckets, search_signature_path):
    for file_name in os.listdir(search_signature_path):
        indexing_collision_max_cnt_dic = {}
        fp = os.path.join(search_signature_path, file_name)
        file_name_md5 = os.path.splitext(file_name)[0]
        with open(fp, 'rb') as rpf:
            signature_vector = pickle.load(rpf)
            r_concat=''
            for i, element in enumerate(signature_vector):
                r_concat += str(element)
                if i % R_SIZE == R_SIZE - 1:
                    md5_hash = int(hashlib.md5(r_concat.encode()).hexdigest(), 16)
                    ind = md5_hash % BUCKET_SIZE
                    collision_md5s_set = buckets[i][ind]
                    for collision_file_md5 in collision_md5s_set:
                        if collision_file_md5 not in indexing_collision_max_cnt_dic.keys():
                            indexing_collision_max_cnt_dic[collision_file_md5] = 1
                        else:
                            indexing_collision_max_cnt_dic[collision_file_md5] += 1
                    r_concat = ''
            if ONLY_COMPARE_MOST_SIMILAR_FILE:
                sort_data = sorted(indexing_collision_max_cnt_dic.items(), key = lambda x: x[1], reverse=True)
                if sort_data:
                    max_collision_file_md5 = sort_data[0][0]
                    similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, max_collision_file_md5) + '.' + FEATURE_NAME)
                    query_set = load_pickle_file(os.path.join(SEARCH_SET_PATH, file_name_md5) + '.' + FEATURE_NAME)
                    LSH_jaccard_index = jaccard_similarity(similar_set, query_set)
                    LSH_jaccard_containment = jaccard_containment(similar_set, query_set)
                    predict_lsh_jaccard_index = sort_data[0][1] / B_SIZE
                    min_len = len(query_set) > len(similar_set) and len(similar_set) or len(query_set)
                    predict_lsh_jaccard_containment = (predict_lsh_jaccard_index * (len(query_set) + len(similar_set))) / (min_len * (predict_lsh_jaccard_index + 1))
                else:
                    max_collision_file_md5 = None
                    LSH_jaccard_index = 0
                    LSH_jaccard_containment = 0
                    predict_lsh_jaccard_index = 0
                    predict_lsh_jaccard_containment = 0
            else:
                if indexing_collision_max_cnt_dic:
                    max_LSH_jaccard_index = 0
                    collision_cnt = 0
                    query_set = load_pickle_file(os.path.join(SEARCH_SET_PATH, file_name_md5) + '.' + FEATURE_NAME)
                    for k, v in indexing_collision_max_cnt_dic.items():
                        candidate_similar_file_md5 = k
                        similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, candidate_similar_file_md5) + '.' + FEATURE_NAME)
                        LSH_jaccard_index = jaccard_similarity(similar_set, query_set)
                        if max_LSH_jaccard_index < LSH_jaccard_index:
                            max_LSH_jaccard_index = LSH_jaccard_index
                            max_collision_file_md5 = k
                            collision_cnt = v
                    LSH_jaccard_index = max_LSH_jaccard_index
                    similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, max_collision_file_md5) + '.' + FEATURE_NAME)
                    LSH_jaccard_containment = jaccard_containment(similar_set, query_set)
                    predict_lsh_jaccard_index = collision_cnt / B_SIZE
                    min_len = len(query_set) > len(similar_set) and len(similar_set) or len(query_set)
                    predict_lsh_jaccard_containment = (predict_lsh_jaccard_index * (len(query_set) + len(similar_set))) / (min_len * (predict_lsh_jaccard_index + 1))
                else:
                    max_collision_file_md5 = None
                    LSH_jaccard_index = 0
                    LSH_jaccard_containment = 0
                    predict_lsh_jaccard_index = 0
                    predict_lsh_jaccard_containment = 0

        try:
            most_jaccard_index_ground_truth = GROUND_TRUTH_DIC[file_name_md5]['jaccard_similarity']
        except Exception as e:
            print(e)
            most_jaccard_index_ground_truth = None

        sim_info_dic = {
            "query_md5": file_name_md5,
            "similar_md5": max_collision_file_md5,
            "museum_jaccard_index": predict_lsh_jaccard_index,
            "museum_jaccard_containment": predict_lsh_jaccard_containment,
            "original_jaccard_index": LSH_jaccard_index,
            "original_jaccard_containment": LSH_jaccard_containment,
            "most_jaccard_index_ground_truth": most_jaccard_index_ground_truth
        }
        print(sim_info_dic)
    return

def search_bucket_mp(fp):
    file_name = os.path.basename(fp)
    file_name_md5 = os.path.splitext(file_name)[0]
    with open(fp, 'rb') as rpf:
        signature_vector = pickle.load(rpf)
        indexing_collision_max_cnt_dic = {}
        r_concat=''
        for i, element in enumerate(signature_vector):
            r_concat += str(element)
            if i % R_SIZE == R_SIZE - 1:
                md5_hash = int(hashlib.md5(r_concat.encode()).hexdigest(), 16)
                ind = md5_hash % BUCKET_SIZE
                collision_md5s_set = BUCKETS[i][ind]
                for collision_file_md5 in collision_md5s_set:
                    if collision_file_md5 not in indexing_collision_max_cnt_dic.keys():
                        indexing_collision_max_cnt_dic[collision_file_md5] = 1
                    else:
                        indexing_collision_max_cnt_dic[collision_file_md5] += 1
                r_concat = ''
        if ONLY_COMPARE_MOST_SIMILAR_FILE:
            sort_data = sorted(indexing_collision_max_cnt_dic.items(), key = lambda x: x[1], reverse=True)
            if sort_data:
                max_collision_file_md5 = sort_data[0][0]
                similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, max_collision_file_md5) + '.' + FEATURE_NAME)
                query_set = load_pickle_file(os.path.join(SEARCH_SET_PATH, file_name_md5) + '.' + FEATURE_NAME)
                LSH_jaccard_index = jaccard_similarity(similar_set, query_set)
                LSH_jaccard_containment = jaccard_containment(similar_set, query_set)
                predict_lsh_jaccard_index = sort_data[0][1] / B_SIZE
                min_len = len(query_set) > len(similar_set) and len(similar_set) or len(query_set)
                predict_lsh_jaccard_containment = (predict_lsh_jaccard_index * (len(query_set) + len(similar_set))) / (min_len * (predict_lsh_jaccard_index + 1))
            else:
                max_collision_file_md5 = None
                LSH_jaccard_index = 0
                LSH_jaccard_containment = 0
                predict_lsh_jaccard_index = 0
                predict_lsh_jaccard_containment = 0
        else:
            if indexing_collision_max_cnt_dic:
                max_LSH_jaccard_index = 0
                collision_cnt = 0
                query_set = load_pickle_file(os.path.join(SEARCH_SET_PATH, file_name_md5) + '.' + FEATURE_NAME)
                for k, v in indexing_collision_max_cnt_dic.items():
                    candidate_similar_file_md5 = k
                    similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, candidate_similar_file_md5) + '.' + FEATURE_NAME)
                    LSH_jaccard_index = jaccard_similarity(similar_set, query_set)
                    if max_LSH_jaccard_index < LSH_jaccard_index:
                        max_LSH_jaccard_index = LSH_jaccard_index
                        max_collision_file_md5 = k
                        collision_cnt = v
                LSH_jaccard_index = max_LSH_jaccard_index
                similar_set = load_pickle_file(os.path.join(INDEXING_SET_PATH, max_collision_file_md5) + '.' + FEATURE_NAME)
                LSH_jaccard_containment = jaccard_containment(similar_set, query_set)
                predict_lsh_jaccard_index = collision_cnt / B_SIZE
                min_len = len(query_set) > len(similar_set) and len(similar_set) or len(query_set)
                predict_lsh_jaccard_containment = (predict_lsh_jaccard_index * (len(query_set) + len(similar_set))) / (min_len * (predict_lsh_jaccard_index + 1))
            else:
                max_collision_file_md5 = None
                LSH_jaccard_index = 0
                LSH_jaccard_containment = 0
                predict_lsh_jaccard_index = 0
                predict_lsh_jaccard_containment = 0

    try:
        most_jaccard_index_ground_truth = GROUND_TRUTH_DIC[file_name_md5]['jaccard_similarity']
    except Exception as e:
        print(e)
        most_jaccard_index_ground_truth = None

    sim_info_dic = {
        "query_md5": file_name_md5,
        "similar_md5": max_collision_file_md5,
        "museum_jaccard_index": predict_lsh_jaccard_index,
        "museum_jaccard_containment": predict_lsh_jaccard_containment,
        "original_jaccard_index": LSH_jaccard_index,
        "original_jaccard_containment": LSH_jaccard_containment,
        "most_jaccard_index_ground_truth": most_jaccard_index_ground_truth
    }
    print(sim_info_dic)
    return sim_info_dic

def ground_truth_to_make_dic(file_path):
    sample_list = []
    original_jaccard_dic = {}
    with open(file_path, 'rt') as rjf:
        total_jaccard_result_list = json.load(rjf)
        for list_content in total_jaccard_result_list:
            compare_file_name = list(list_content.keys())[0]
            sample_list.append(compare_file_name)
            values = list_content.values()
            for content in values:
                most_similar_file_md5 = content['most_similar_file_md5']
                jaccard_similarity = content['jaccard_similarity']
                temp_dic = {
                    "most_similar_file_md5": most_similar_file_md5,
                    "jaccard_similarity": jaccard_similarity
                }
                original_jaccard_dic[compare_file_name] = temp_dic
    return original_jaccard_dic

def get_section(search_set_path):
    path_files_list = []
    for file_name in os.listdir(search_set_path):

        fp = os.path.join(search_set_path, file_name)
        path_files_list.append(fp)
    return path_files_list

def run_mp():
    section_list = get_section(SEARCH_SIGNATURE_PATH)
    print(section_list)
    freeze_support()
    with Pool(processes=PROCESS_NUMBER) as pool:
        action_list = pool.map(search_bucket_mp, section_list)
    print(len(action_list))
    with open(SEARCH_REPORT_PICKLE_PATH, 'wb') as wpf:
        pickle.dump(action_list, wpf)

if __name__ == '__main__':
    # make_bucket(INDEXING_SIGNATURE_PATH)

    GROUND_TRUTH_DIC = ground_truth_to_make_dic(MOST_JACCARD_INDEX_ROUND_TRUTH_PATH)

    BUCKETS = load_pickle_file(STORE_BUCKET_DIC_PATH)
    run_mp()
    # search_bucket(BUCKETS, SEARCH_SIGNATURE_PATH)