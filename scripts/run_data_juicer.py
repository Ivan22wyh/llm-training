import os
import subprocess
import logging

# 设置日志记录
logging.basicConfig(filename='process_data_log.txt', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# 需要修改的文件路径
config_file = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data-juicer/configs/demo/mine_book_test.yaml'

# 要遍历的目录路径
directory = '/mnt/geogpt-gpfs/llm-course/ossutil_output/public/tianwen/datasets/books/json_czz'

for filename in os.listdir(directory)[:10]:
    file_path = os.path.join(directory, filename)
    if os.path.isfile(file_path):
        try:
            # 读取 YAML 文件的内容
            with open(config_file, 'r') as f:
                lines = f.readlines()

            # 修改第三行和第四行的内容
            lines[2] = f"dataset_path: ''/mnt/geogpt-gpfs/llm-course/ossutil_output/public/tianwen/datasets/books/json_czz/{filename}'\n"
            lines[3] = f"export_path: '/mnt/geogpt-gpfs/llm-course/public/tianwen/datasets/web_crawl_clean_dt/{filename}'\n"

            # 写回修改后的内容
            with open(config_file, 'w') as f:
                f.writelines(lines)

            # 运行 process_data.py 脚本
            command = 'sudo python3 /mnt/geogpt-gpfs/llm-course/home/wenyh/data-juicer/tools/process_data.py --config /mnt/geogpt-gpfs/llm-course/home/wenyh/data-juicer/configs/demo/mine_book_test.yaml'
            os.system(command)
        except Exception as e:
            # 如果出现错误,记录到日志文件
            logging.error(f"Error processing file: {filename}, {e}\n\n\n")
