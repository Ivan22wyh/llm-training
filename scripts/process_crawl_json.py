import os
import re
import json
import glob
import gzip
from tqdm import tqdm

def get_all_files(directory, filetype):
    return sorted(glob.glob(os.path.join(directory, f'**/*{filetype}'), recursive=True))

def process_json(file):
    with gzip.open(file, 'r') as f:
        json_data = f.readlines()
    cleaned_data = []
    for i, line in enumerate(json_data):
        bad_data = eval(line)
        text = bad_data['text'].replace('\\n', '\n') 
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r'\\+', r'\\', text)
        text = re.sub(r'\s+', ' ', text)
        cleaned_text = re.sub(r'\\ub[0-9]{3}', '', text)
        cleaned_data.append({
            "id": f"{file.split('-')[4]}-{i}",
            "source": "web_crawl_astro_data_v1",
            "url": bad_data['url'],
            "text": cleaned_text
        })
    with open(file.replace('web_crawl', 'web_crawl_clean_v2').replace('jsonl.gz', 'jsonl'), 'w') as f:
        for i in cleaned_data:
            json.dump(i, f, ensure_ascii=False)
            f.write('\n')

    return 

def add_key(file, *kwargs):
    with open(file, 'r') as f:
        json_data = json.load(f)

def check_json(file):
    with gzip.open(file.replace('web_crawl', 'web_crawl_clean_v2'), 'r') as f:
        json_data = json.load(f)
    for x in json_data:
        print(type(x))

def main():
    crawl_json_path = "/mnt/geogpt-gpfs/llm-course/ossutil_output/public/tianwen/datasets/web_crawl"
    json_file_list = get_all_files(crawl_json_path, filetype='.gz')
    for json_file in tqdm(json_file_list):
        process_json(json_file)
        #check_json(json_file)
        #break
    return

if __name__ == '__main__':
    main()
