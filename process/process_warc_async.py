import os
import re
import json
import yaml
import hashlib
import pathlib
import argparse
import asyncio
import aiofiles
from aiofiles import os as aio_os
from warcio.archiveiterator import ArchiveIterator
from multiprocessing import Pool, Manager
from loguru import logger

class WarcProcessor:
    def __init__(self, warc_file, output_dir, lock):
        self.warc_file = warc_file
        self.output_dir = output_dir
        self.lock = lock

    @staticmethod
    def unique_file_name(base_name, url):
        """
        Generate a unique filename based on URL.
        """
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()
        return f"{base_name}_{url_hash}"

    @staticmethod
    def content_mapper(content_type):
        if 'pdf' in content_type: return 'pdf'
        elif 'image' in content_type: return 'png'
        elif 'video' in content_type: return 'mp4'

    def correct_name(self, file_name, content_type):
        if "?" in file_name:
            correct_file_name = file_name.split('?')[0]
        if "." not in file_name:
            # convert content type of response in WARC file to specific file type 
            file_type = self.content_mapper(content_type)
            correct_file_name = correct_file_name + f'.{file_type}'
        return correct_file_name

    async def _process_text(self, record, file_path):
        """
        Process and save HTML text content as JSONL.
        """
        content_type = record.http_headers.get_header('Content-Type').lower().replace(' ', '')
        encoding = content_type.split('charset=')
        code = encoding[1] if len(encoding) == 2 else 'utf-8'
        url = record.rec_headers.get_header('WARC-Target-URI')

        try:
            content = await record.content_stream.read().decode(code)
            async with aiofiles.open(file_path, mode='a') as writer:
                await writer.write(json.dumps({
                    "url": url,
                    "text": content
                }) + '\n')
            logger.info(f"Write text content from {url} to {file_path}")
        except UnicodeDecodeError:
            logger.warning(f"Decode error for {record.rec_headers.get_header('WARC-Target-URI')}")

    async def _save_binary_content(self, record, file_path, content_type_desc):
        """
        Save binary content (image, video, pdf) to file.
        """
        url = record.rec_headers.get_header('WARC-Target-URI')
        try:
            async with aiofiles.open(file_path, 'wb') as f:
                await f.write(await record.content_stream.read())
            logger.info(f"Saved {content_type_desc} content from {url} to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save {content_type_desc} content from {url}: {e}")

    async def process(self):
        """
        Extract various files from WARC file and save them appropriately.
        """
        file_base_name = os.path.splitext(os.path.basename(self.warc_file))[0].split('.warc')[0]

        with open(self.warc_file, 'rb') as stream:
            for record in ArchiveIterator(stream):
                print("1\n\n\n")
                if not record.rec_type == 'response' or record.http_headers is None:
                    continue

                content_type = record.http_headers.get_header('Content-Type', '').lower()
                url = record.rec_headers.get_header('WARC-Target-URI')

                if 'text/html' in content_type:
                    file_path = os.path.join(self.output_dir, 'text', f"{file_base_name}.jsonl")
                    await self._process_text(record, file_path)

                elif re.search(r'image/|video/|application/pdf', content_type):
                    file_name = file_base_name + '_' + url.split('/')[-1]
                    file_name = self.correct_name(file_name, content_type)
                    save_dir = self.content_mapper(content_type)
                    file_path = os.path.join(self.output_dir, save_dir, file_name)
                    await self._save_binary_content(record, file_path, content_type)


def warc_file_iter(src_dir):
    """
    Find all WARC files in the source directory.
    """
    return pathlib.Path(src_dir).glob('*.warc.gz')

def process_warc_file(warc_fp, output_dir, lock):
    asyncio.run(WarcProcessor(warc_fp, output_dir, lock).process())

def check_and_create_output_dirs(output_dir):
    directories = [output_dir, f'{output_dir}/text', f'{output_dir}/png', f'{output_dir}/mp4', f'{output_dir}/pdf']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

def read_configs():
    parser = argparse.ArgumentParser(description="Read the YAML file from the command line.")
    parser.add_argument('yaml_path', help='Path to the YAML file')
    args = parser.parse_args()
    with open(args.yaml_path, 'r') as f:
        configs = yaml.safe_load(f)
    return configs

def main():
    configs = read_configs()
    source_dir = configs['source_dir']
    output_dir = configs['output_dir']
    num_procs = configs.get('num_procs', 10)

    check_and_create_output_dirs(output_dir)

    manager = Manager()
    lock = manager.Lock()

    pool = Pool(num_procs)
    for warc_fp in warc_file_iter(source_dir):
        logger.info(f'Starting process for {warc_fp}')
        pool.apply_async(process_warc_file, args=(warc_fp, output_dir, lock))
        
    pool.close()
    pool.join()


if __name__ == "__main__":
    main()
