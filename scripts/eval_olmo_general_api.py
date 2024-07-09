from mmengine.config import read_base
from opencompass.models import OLMoAPI
from opencompass.partitioners import NaivePartitioner
from opencompass.runners import LocalRunner
from opencompass.tasks import OpenICLInferTask
'''
with read_base():
    from .datasets.agieval.agieval_gen import agieval_datasets # 考试（6.生物）

    from .datasets.MedQA.medqa_gen import medqa_datasets
    from .datasets.mmlu.mmlu_gen import mmlu_datasets # 考试
    from .datasets.cmmlu.cmmlu_gen import cmmlu_datasets # 考试
    # from .datasets.ceval.ceval_gen import ceval_datasets # 考试
    # from .datasets.GaokaoBench.GaokaoBench_gen import GaokaoBench_datasets # 考试

    # from .datasets.tydiqa.tydiqa_gen import tydiqa_datasets # 语言
    # from .datasets.flores.flores_gen import flores_datasets # 语言

    from .datasets.triviaqa.triviaqa_gen import triviaqa_datasets # 知识
    from .datasets.xiezhi.xiezhi_gen import xiezhi_datasets # 知识
    # from .datasets.nq.nq_gen import nq_datasets # 知识


    from .datasets.race.race_gen import race_datasets # 理解
    from .datasets.lambada.lambada_gen import lambada_datasets # 理解

    from .datasets.IFEval.IFEval_gen import ifeval_datasets # 理解（8.指令）

    from .datasets.winogrande.winogrande_gen import winogrande_datasets # 推理
    from .datasets.hellaswag.hellaswag_ppl import hellaswag_datasets # 推理
    from .datasets.bbh.bbh_gen import bbh_datasets # 推理

    from .datasets.math.math_gen import math_datasets # 推理（9.数学）
    from .datasets.TheoremQA.TheoremQA_gen import TheoremQA_datasets# 推理（9.数学）
    from .datasets.gsm8k.gsm8k_gen import gsm8k_datasets # 推理（9.数学）
    
    from .datasets.humaneval.humaneval_gen import humaneval_datasets # 推理（10.代码）
    from .datasets.mbpp.mbpp_gen import mbpp_datasets # 推理（10.代码）

datasets = sum((v for k, v in locals().items() if k.endswith("_datasets")), [])

'''
root_dir = '/mnt/geogpt-gpfs/llm-course/public/tianwen/eval/quiz'
datasets = []
datasets.append({"path":f"{root_dir}/planet_qa.jsonl","data_type":"qa","infer_method":"gen"})

datasets.append({"path":f"{root_dir}/astrochemistry_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/astrometry_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/cosmology_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/galaxy_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/gaoneng_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/hengxing_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/radio_qa.csv","data_type":"qa","infer_method":"gen"})
datasets.append({"path":f"{root_dir}/solar_qa.csv","data_type":"qa","infer_method":"gen"})

datasets.append({"path":f"{root_dir}/astrometry_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/cosmology_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/galaxy_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/gaoneng_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/hengxing_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/radio_mcq.csv","data_type":"mcq","infer_method":"ppl"})
datasets.append({"path":f"{root_dir}/solar_mcq.csv","data_type":"mcq","infer_method":"ppl"})

api_meta_template = dict(
    round=[
            dict(role='HUMAN', api_role='HUMAN'),
            dict(role='BOT', api_role='BOT', generate=True),
    ],
)

models = [
    dict(abbr='high-energy-2b',
        type=OLMoAPI, path='high-energy-2b',
        key='ENV',  # The key will be obtained from $OPENAI_API_KEY, but you can write down your key here as well
        meta_template=api_meta_template,
        query_per_second=16,
        url='http://localhost:8668/v1/chat/completions',
        tokenizer_path='/mnt/geogpt-gpfs/llm-course/home/masg/OLMo/sft_output_gaoneng',
        max_out_len=2048, max_seq_len=4096, batch_size=4,
    )
]

infer = dict(
    partitioner=dict(type=NaivePartitioner),
    runner=dict(
        type=LocalRunner,
        max_num_workers=8,
        task=dict(type=OpenICLInferTask)),
)
