import numpy as np
import os
import random

def hexdump(filename, bytes_per_line=16):
    with open(filename, 'rb') as f:
        offset = 0
        while True:
            # 读取一行数据
            chunk = f.read(bytes_per_line)
            if not chunk:
                break
            
            # 打印偏移量
            print(f'{offset:08X}:', end=' ')
            
            # 打印十六进制表示
            hex_str = ' '.join(f'{byte:02X}' for byte in chunk)
            #print(hex_str.ljust(bytes_per_line * 3), end=' ')
            
            # 打印十进制表示
            dec_str = ' '.join(f'{byte:08}' for byte in chunk)
            print(dec_str.ljust(bytes_per_line * 4), end=' ')
            
            offset += bytes_per_line

            return

def count_files_and_size(directory):
    print(directory.split('npy_data/')[1])
    # 获取目录下的文件列表
    files = os.listdir(directory)
    
    # 打印文件数量
    print(f'Number of files in directory: {len(files)}')
    
    # 如果有文件，则打印第一个文件的大小
    if files:
        first_file = os.path.join(directory, files[0])
        first_file_size = os.path.getsize(first_file)/2/1000000000
        print(f'Size of first file: {first_file_size} bytes')

    return first_file_size

def list_directories(path):

    directories = []
    for entry in os.listdir(path):
        full_path = os.path.join(path, entry)
        if os.path.isdir(full_path):
            directories.append(full_path)

    directories[2] = '/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data/RedPajamaCommonCrawl/npy'
    directories[4] = '/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data/RedPajamaC4/npy'
    return directories

def random_files(path, total_train_file_path, num_files=15):
    npy_files = [file for file in os.listdir(path) if file.endswith('.npy')]
    if len(npy_files) < num_files:
        print(f'Warning: There are fewer than {num_files} .npy files in the directory')
    selected_files = random.sample(npy_files, min(num_files, len(npy_files)))
    file_paths = [os.path.join(path, file) for file in selected_files]
    for f in file_paths: total_train_file_path.append(f)
    return total_train_file_path 

def total_train_file_size(file_paths):
    total_size = 0
    for file_path in file_paths:
        total_size += os.path.getsize(file_path)
    return f'total_size: {total_size/2/1000000000}'

def write_train_file_to_dat(lst, filename):
    # 打开一个名为 filename 的文件，以写入模式打开
    with open(filename, 'w') as f:
        # 遍历列表中的每个元素
        for item in lst:
            # 将当前元素转换为字符串，并添加换行符
            line = '  - ' + item + '\n'
            # 将当前行写入文件
            f.write(line)


if __name__ == '__main__':
    total_file_size = 0
    total_train_file_path = []

    data_path = list_directories('/mnt/geogpt-gpfs/llm-course/public/datasets/npy_data')

    for data in data_path:
        total_file_size += count_files_and_size(data)
        total_train_file_path = random_files(data, total_train_file_path, num_files=20)
        print('\n')

    print(total_file_size)
    write_train_file_to_dat(total_train_file_path, '/mnt/geogpt-gpfs/llm-course/home/wenyh/train/train_file.dat')

    print(total_train_file_size(total_train_file_path))
