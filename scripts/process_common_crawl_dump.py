import sys
import nltk

from datatrove.executor.local import LocalPipelineExecutor
from datatrove.pipeline.extractors import Trafilatura
from datatrove.pipeline.filters import (
    GopherQualityFilter,
    GopherRepetitionFilter,
    LanguageFilter,
    URLFilter,
)
from datatrove.pipeline.readers import WarcReader
from datatrove.pipeline.writers.jsonl import JsonlWriter

nltk.download('punkt')

# DUMP should be given as an argument. Example: CC-MAIN-2023-23
if len(sys.argv) != 2:
    print("Argument required: dump name")
    sys.exit(-1)
DUMP = sys.argv[1]

WARC_PATH = "/mnt/geogpt-gpfs/llm-course/home/wenyh/crawler/warcs"
MAIN_OUTPUT_PATH = "/mnt/geogpt-gpfs/llm-course/home/wenyh/crawler/output"

executor = LocalPipelineExecutor(
    pipeline=[
        WarcReader(
            WARC_PATH,
            glob_pattern="*.warc.gz",  # we want the warc files
            default_metadata={"dump": DUMP},
        ),
        #URLFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/url/{DUMP}")),
        Trafilatura(favour_precision=True),
        LanguageFilter(
            exclusion_writer=JsonlWriter(
                f"{MAIN_OUTPUT_PATH}/non_english/",
                output_filename="${language}/" + DUMP + "/${rank}.jsonl.gz",  # folder structure: language/dump/file
            )
        ),
        #GopherRepetitionFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/repetitive/{DUMP}")),
        GopherQualityFilter(exclusion_writer=JsonlWriter(f"{MAIN_OUTPUT_PATH}/removed/quality/{DUMP}")),
        JsonlWriter(f"{MAIN_OUTPUT_PATH}/{DUMP}"),
    ],
    #concurrency=8,
    logging_dir=f"{MAIN_OUTPUT_PATH}/logs/base_processing/{DUMP}",
)
executor.run()
