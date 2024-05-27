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

def get_file_size(file_path):
    # 检查文件是否存在
    if os.path.isfile(file_path):
        # 使用os.path.getsize()获取文件大小，以字节为单位
        file_size = os.path.getsize(file_path)
        # 转换为更友好的格式
        if file_size < 1024:
            return f"{file_size} 字节"
        elif file_size < 1024 * 1024:
            return f"{file_size / 1024:.2f} KB"
        else:
            return f"{file_size / (1024 * 1024):.2f} MB"
    else:
        return "文件不存在"

def load_file(file_path, save_title=False):
    id_list, text_list, title_list, url_list = [], [], [], []

    if file_path.endswith(".json.gz"):
        with gzip.open(file_path, "rt") as f:
            for line in f:
                data = json.loads(line)
                id_list.append(data["id"])
                text_list.append(data["text"])

    elif file_path.endswith(".json") or file_path.endswith(".jsonl"):
        with open(file_path, "r") as f:
            for line in tqdm(f):
                data = json.loads(line)
                id_list.append(data["id"])
                text_list.append(data["text"])
                url_list.append(data['metadata']['url'])
                if save_title: title_list.append(data['metadata']['title'])

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
    fine_text = [s.replace('\n', ' ') for s in text]
    return fine_text

# 使用示例
# 假设我们有一个名为'data.json'的JSON文件
# get_all_keys_from_json('data.json')
if __name__ == '__main__':
    threshold = 0.5
    model_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/output/keywords_model.bin'
    test_file_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/input/test_data/dolma_wiki_title.txt'
    wiki_gz_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/dolma_v1_7/wiki/documents', filetype='.gz')
    wiki_parquet_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/wikipedia/20231101.en/', filetype='.pqrquet')
    c4_file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data/RedPajamaC4', filetype='.json')
    
    file_list = c4_file_list
    model = fasttext.load_model(model_path)
    print(f"{file_list[0].split('.')[1]} file count: {len(file_list)}")

    for i, f in enumerate(file_list):
        print(f'Data mining in {i} of {len(file_list)} files')
        id_list, title_list, url_list, text_list = load_file(f)
        text_list = check_text_format(text_list)
        astro_prob = [prob[1] for prob in model.predict(text_list, k=2)[1]]
        astro_label_index = np.where(np.array(astro_prob, dtype=float)>threshold)[0]
        print(np.array(text_list)[astro_label_index])
        #print(id_list, title_list, url_list, text_list)
        break


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