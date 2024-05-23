import os
from transformers import AutoTokenizer
from olmo.data import MemMapDataset

tokenizer = AutoTokenizer.from_pretrained('EleutherAI/gpt-neox-20b')

wiki_datasets_dir = '/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data/RedPajamaWikipedia'

def get_all_npy_files(directory):
    """
    获取指定目录及其子目录中的所有 .npy 文件。

    参数:
        directory (str): 要搜索的目录路径。

    返回:
        List[str]: 包含所有 .npy 文件路径的列表。
    """
    npy_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.npy'):
                npy_files.append(os.path.join(root, file))
    return npy_files

wiki_datasets = get_all_npy_files(wiki_datasets_dir)
mapped_wiki_dataset = MemMapDataset(f'{wiki_datasets[99]}')


test_line = mapped_wiki_dataset[22]['input_ids']
decode_text = tokenizer.decode(test_line)
print(decode_text)