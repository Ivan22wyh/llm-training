import os
import glob
import gzip
import json
import vaex
import fasttext
import argparse
import numpy as np
from tqdm import tqdm


def get_all_files(directory, filetype):
    return sorted(glob.glob(os.path.join(directory, f'**/*{filetype}'), recursive=True))

def process_parquet(file_path, dataset):
    df = vaex.open(file_path)[:]
    if dataset == 'wiki_parquet':
        return df['id'].tolist(), \
                df['title'].tolist(), \
                df['text'].tolist(), \
                list(map(lambda x: '', range(len(df))))
    if dataset == 'fineweb':
        return np.array(df['id'].tolist()), \
                np.array(list(map(lambda x: '', range(len(df))))), \
                [check_text_format(x) for x in df['text'].tolist()], \
                np.array(df['url'].tolist())

def process_json(file_path, dataset):
    with open(file_path, "r") as f:
        data = json.load(f)
        id_list = [item["id"] for item in data]
        text_list = [item["text"] for item in data]
        url_list = [item['metadata']['url'] for item in data]
        title_list = [item['metadata']['title'] for item in data]
    return id_list, title_list, text_list, url_list

def process_json_gz(file_path, dataset):
    id_list, title_list, text_list, url_list = [], [], [], [], 
    with gzip.open(file_path, "rt") as f:
        json_lines= [json.loads(line) for line in f]

    if dataset == 's2orc':
        for data in json_lines:
            try:
                title_list.append(check_text_format(data['text'].splitlines()[1]))
                id_list.append(data['meta']['corpusid'])
                text_list.append(data['text'])
                url_list.append('')
            except Exception:
                continue
        return id_list, title_list, text_list, url_list

    if dataset == 'wiki':
        id_list = [data['id'] for data in json_lines]
        url_list = [data['metadata']['url'] for data in json_lines]
        title_list = [data['metadata']['wikipedia']['title'] for data in json_lines]
        text_list = [data['text'] for data in json_lines]  
        return id_list, title_list, text_list, url_list 

def load_file(file_path, *args, **kwargs):
    file_type_handlers = {
        ".json.gz": process_json_gz,
        ".jsonl.gz": process_json_gz,
        ".json":  process_json,
        ".jsonl": process_json,
        ".parquet": process_parquet
    }

    for extension, handler in file_type_handlers.items():
        if file_path.endswith(extension):
            return handler(file_path, *args, **kwargs)

    raise ValueError("Unknown file format.")

def check_text_format(text):
    return text.lower().strip().replace('\n', ' ')

def cal_mask(batch_size, threshold, ):
    mask = set()
    astro_keywords = [
        "astrophysics", "astronomy", "gravitation", "neutron", "telescope", "black hole", "main sequence", "nebula", "asteroid", "constellation", "cosmology", "redshift", "supernova", "quasar", "pulsar", "exoplanet", "spectroscopy", "cosmic"
    ]
    for i in tqdm(range(0, len(prob_list), batch_size)):
        batch_prob_list = prob_list[i: i+batch_size]
        batch_tag_list = tag_list[i: i+batch_size]
        if dataset == 'fineweb':
            batch_text_list = text_list[i: i+batch_size]
        else:
            batch_text_list = title_list[i: i+batch_size]
    
        mask_prob = set(np.where(batch_prob_list > threshold)[0])
        mask_tag = set(np.where(batch_tag_list == '__label__astro')[0])
        mask_exact = set(np.where(np.array([any(keyword.lower() in text.lower() for keyword in astro_keywords) for text in batch_text_list]))[0])
        mask_batch = mask_prob.intersection(mask_tag)
        mask_batch = mask_batch.union(mask_exact)
    
        # Adjust the indices to match with the original array
        mask_batch_adj = {index + i for index in mask_batch}
        mask = mask.union(mask_batch_adj)

    mask = np.array(list(mask))
    print(f'Total {len(mask)} astro texts ...')

    return mask

def main(): 
    global tag_list, prob_list, id_list, title_list, text_list, url_list, dataset
    model_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/fasttext/output/keywords_model_v4.bin'
    base_save_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/astro_mining_data'
    model = fasttext.load_model(model_path)

    parser = argparse.ArgumentParser(description='search astro data in datasets')
    parser.add_argument('dataset', type=str, help='Input the dataset... ')
    dataset = parser.parse_args().dataset
    if dataset == 'fineweb':
        file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/ossutil_output/public/datasets/FineWeb/CC-MAIN-2024-10', filetype='.parquet')
    elif dataset == 'wiki_parquet':
        file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/wikipedia/20231101.en/', filetype='.parquet')
    elif dataset == 's2orc':
        file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/ossutil_output/public/datasets/processed/s2orc/v0/documents', filetype='.gz')
    elif dataset == 'wiki':
        file_list = get_all_files('/mnt/geogpt-gpfs/llm-course/public/datasets/dolma_v1_7/wiki/documents', filetype='.gz')

    start=170
    for file_index, f in enumerate(file_list[:200], start=start):
        print(f'Data mining in {file_index} of {len(file_list)} files')
        try:
            id_list, title_list, text_list, url_list = load_file(f, dataset=dataset)
        except Exception as e:  # More specific exception handling
            print(f'Error loading data from {f}: {str(e)}')
            continue
        print('Extracting text finished...')

        # Use fasttext model to search astronomy information
        if dataset == 'fineweb': res = model.predict(text_list, k=1)
        else: res = model.predict(title_list, k=1)
        tag_list, prob_list = np.array([_[0] for _ in res[0]]), np.array([_[0] for _ in res[1]])
        print(f'Predicting finished {len(tag_list)} texts ...')

        # Filter astro related information
        mask = cal_mask(batch_size=100000, threshold=0.9, )

        # Write astro data to json
        json_data = [
            {'id': str(id_), 'title': title, 'text': text, 'url': url, 'prob': str(prob)}
            for i, (id_, title, text, url, prob) in enumerate(zip(
                id_list,
                title_list,
                text_list,
                url_list,
                prob_list,
            ))
            if i in mask
        ]
        try:
            with open(f'{base_save_path}/{dataset}/{dataset}_astro_{file_index:03d}.json', 'w+') as f:
                json.dump(json_data, f, indent=4)
        except FileNotFoundError:
            os.makedirs(f'{base_save_path}/{dataset}/')

    return

if __name__ == '__main__':
    main()

