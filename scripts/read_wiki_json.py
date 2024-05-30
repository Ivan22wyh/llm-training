import os
import gzip
import json
import fasttext
import numpy as np
from tqdm import tqdm
import vaex

def get_all_files(directory, filetype):
    npy_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith(filetype):
                npy_files.append(os.path.join(root, file))
    return npy_files

def load_file(file_path, save_title=False):
    id_list, text_list, title_list, url_list = [], [], [], []
    count = 1

    if file_path.endswith(".json.gz") or file_path.endswith(".jsonl.gz"):
        with gzip.open(file_path, "rt") as f:
            for i, line in enumerate(f):
                data = json.loads(line)
                try:
                    title = data['text'].splitlines()[1]
                except AttributeError:
                    continue
                if title == '': continue
                id_list.append(data['meta']['corpusid'])
                title_list.append(check_text_format(title))

                if i >= 1000*count: 
                    print(f"{i} files ... have been extracted ")
                    count += 1
            return id_list, title_list

    elif file_path.endswith(".json") or file_path.endswith(".jsonl"):
        with open(file_path, "r") as f:
            for i, line in enumerate(f):
                data = json.loads(line)
                id_list.append(data["id"])
                text_list.append(data["text"])
                url_list.append(data['metadata']['url'])
                if save_title: title_list.append(data['metadata']['title'])
                break

    elif file_path.endswith(".parquet"):
        df = vaex.open(file_path)
        title_list, text_list = df['title'].tolist(), df['title'].tolist()
    else:
        raise ValueError("Unknown file format.")

    return id_list, title_list, url_list, text_list

def append_title_to_txt(file_path, data_list, overwrite=False):
    if overwrite: write_type = 'w+'
    else: write_type = 'a+'
    with open(file_path, write_type, encoding='utf-8') as file:  # 使用'a'模式追加内容
        for item in data_list:
            file.write(f"{item}\n")  # 写入元素，并在每个元素后添加换行符
    return

def check_text_format(text):
    fine_text = text.lower().strip().replace('\n', ' ')
    return fine_text

# 使用示例
# 假设我们有一个名为'data.json'的JSON文件
# get_all_keys_from_json('data.json')
if __name__ == '__main__':
    threshold = 0.9
    model_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/output/keywords_model_v2.bin'
    test_file_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/input/test_data/dolma_wiki_title.txt'
    wiki_gz_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/dolma_v1_7/wiki/documents', filetype='.gz')
    wiki_parquet_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/wikipedia/20231101.en/', filetype='.pqrquet')
    c4_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data/RedPajamaC4', filetype='.json')
    s2orc_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/ossutil_output/public/datasets/processed/s2orc/v0/documents', filetype='.gz')
    
    file_list = s2orc_file_list
    model = fasttext.load_model(model_path)
    print(f"{file_list[0].split('.')[1]} file count: {len(file_list)}")

    df = vaex.from_dict({
        'id': [],
        'text': [],
        'prob': []
    })

    for i, f in enumerate(file_list):
        print(f'Data mining in {i} of {len(file_list)} files')
        id_list, title_list = load_file(f)
        print('Extracting text finished...')
        res = model.predict(title_list, k=1)
        tag_list, prob_list = res[0], res[1]
        for i, (tag, prob) in enumerate(zip(tag_list, prob_list)):
            if prob[0] > 0.9 and (tag[0] == '__label__astro'):  
                #print(prob[0], title_list[i])
                new_data = vaex.from_dict({'id': [id_list[i]], 'text': [title_list[i]], 'prob': [prob[0]]})
                df = df.concat(new_data)
        print('Predicting finished...')
    

    #df.export_hdf5('/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/output/s2orc_v2.hdf5')
    #df_hdf5 = vaex.open('/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/output/s2orc_v2.hdf5')
        #astro_label_index = np.where(np.array(astro_prob, dtype=float)>threshold)[0]
        #print([(prob, text) for prob, text in zip(np.array(astro_prob)[astro_label_index], np.array(text_list)[astro_label_index])])


"""    for wiki_file in wiki_file_list:
        print(f"Begin to extract {wiki_file.split('.en/')[1]}")
        df = vaex.open(wiki_file)
        title, text = df['title'].tolist(), df['title'].tolist()
        astro_prob = [prob[1] for prob in model.predict(title, k=2)[1]]
        astro_label_index = np.where(np.array(astro_prob, dtype=float)>threshold)[0]
        print(np.array(title)[astro_label_index])


     for wiki_file in wiki_file_list:
        break
        print(f"Begin to extract {wiki_file.split('documents/')[1]}")
        title, text = extract_json_from_gz(wiki_file)
        print(f".json file count: {len(title)}")
        astro_prob = [prob[1] for prob in model.predict(title, k=2)[1]]
        astro_label_index = np.where(np.array(astro_prob, dtype=float)>threshold)[0]
        print(np.array(title)[astro_label_index])
        #append_title_to_txt(file_path, title, overwrite=True)
        print(f".gz file size: {get_file_size(wiki_file)}")
 """
