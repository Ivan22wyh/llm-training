import os
import gzip
import json

def get_all_gz_files(directory):
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
            if file.endswith('.gz'):
                npy_files.append(os.path.join(root, file))
    return npy_files

def read_gz(file_path):
    data = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                json_data = json.loads(line)
                data.append(json_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
                continue
    return data

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

        

if __name__ == '__main__':
    wiki_gz = get_all_gz_files('/mnt/geogpt-gpfs/llm-course/public/datasets/dolma_v1_7/wiki/documents')
    #print(wiki_gz)
    content = read_gz(wiki_gz[152])
    #print(json.dumps(content[833], indent=4))
    #print(len(wiki_gz), len(content))

    #print(get_file_size(wiki_gz[152]))