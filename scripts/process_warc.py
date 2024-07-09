import pathlib
import os
from multiprocessing import Pool

#import fire
#import jsonlines
from warcio.archiveiterator import ArchiveIterator
from loguru import logger


def single_warc_process(warc_file, jsonl_file):
    """
    extract html files from warc file and save them as jsonl file
    :param warc_file: path to warc file
    :param jsonl_file: path to jsonl file
    """

    #logger.info(f"Processing WARC file: {warc_file}, Output JSONL: {jsonl_file}")

    with open(warc_file, 'rb') as stream, open(jsonl_file, mode='wb') as writer:
        for record in ArchiveIterator(stream):
            if not record.rec_type =='response' or record.http_headers is None:
                continue

            content_type = record.http_headers.get_header('Content-Type')
            #save_type = 'text/html'
            save_type = 'video/quicktime'
            MAX_SAVE_FILES = 5
            if content_type is None or save_type not in content_type:
                continue
            
            try:
                # print('start process: ', record.rec_headers.get_header('WARC-Target-URI'))
                content_type = content_type.lower().replace(' ', '')
                strs = content_type.split('charset=')

                """ code = 'utf-8'
                if len(strs) == 2:
                    code = strs[1]

                content = record.content_stream().read().decode(code) 
                writer.write(
                    {
                    'url': record.rec_headers.get_header('WARC-Target-URI'), 
                    'text': content
                    }
                )
                writer.write('\n')"""
                writer.write(record.content_stream().read())
                raise ValueError('done')
            except UnicodeDecodeError:
                logger.warning(f"Unicode decode error for {record.rec_headers.get_header('WARC-Target-URI')}")
                continue


def warc_file_iter(src_dir):
    """
    Find all warc files in the source directory.
    """
    for warc_fp in pathlib.Path(src_dir).glob('*.warc.gz'):
        yield warc_fp
        


def main(source_dir, output_dir, num_proc=2):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    pool = Pool(num_proc)
    for warc_fp in warc_file_iter(source_dir):
        #logger.info(f'Start to process {warc_fp}')
        jsonl_fp = os.path.join(output_dir,
                                warc_fp.name.replace('.warc.gz', '.jsonl'))
        video_fp = os.path.join(output_dir,
                                warc_fp.name.replace('.warc.gz', '.mp4'))
        pool.apply_async(single_warc_process,
                            args=(
                            warc_fp,
                            video_fp,
                            ))
        
        # single_warc_process(warc_fp, jsonl_fp)
    pool.close()
    pool.join()

if __name__ == "__main__":
    #fire.Fire(main)
    main(
        source_dir="/mnt/geogpt-gpfs/llm-course/home/wenyh/crawler/heritrix_astro_crawler/jobs/astro_crawler/20240611072634/warcs",
        output_dir="/mnt/geogpt-gpfs/llm-course/home/wenyh/data"
    )