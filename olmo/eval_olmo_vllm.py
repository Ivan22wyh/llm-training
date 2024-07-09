from opencompass.models import VLLM
from opencompass.partitioners import SizePartitioner
from opencompass.runners import LocalRunner
from opencompass.tasks import OpenICLInferTask
import argpare
from mmengine.config import read_base
with read_base():
    from .datasets.agieval.agieval_gen import agieval_datasets # 考试（6.生物）

    # from .datasets.MedBench.medbench_gen_0b4fff import medbench_datasets # 考试（7.医学）
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
    from .datasets.hellaswag.hellaswag_gen import hellaswag_datasets # 推理
    from .datasets.bbh.bbh_gen import bbh_datasets # 推理

    from .datasets.math.math_gen import math_datasets # 推理（9.数学）
    from .datasets.TheoremQA.TheoremQA_gen import TheoremQA_datasets# 推理（9.数学）
    from .datasets.gsm8k.gsm8k_gen import gsm8k_datasets # 推理（9.数学）
    
    from .datasets.humaneval.humaneval_gen import humaneval_datasets # 推理（10.代码）
    from .datasets.mbpp.mbpp_gen import mbpp_datasets # 推理（10.代码）

datasets = sum((v for k, v in locals().items() if k.endswith("_datasets")), [])
# datasets = medqa_datasets
olmo_meta_template = dict(
    round=[
        dict(role='HUMAN', begin='', end=''),
        dict(role='BOT', begin='', end='', generate=True),
    ],
    eos_token_id=0
)

models = [
    dict(
        type=VLLM,
        abbr='olmo',
        path="/mnt/geogpt-gpfs/llm-course/home/masg/OLMo/sft_output_gaoneng",
        model_kwargs=dict(gpu_memory_utilization=0.98),
        generation_kwargs=dict(temperature=0),
        max_out_len=2048,
        max_seq_len=2048,
        batch_size=32,
        end_str='<|endoftext|>',
        run_cfg=dict(num_gpus=1, num_procs=1),
        meta_template=olmo_meta_template
    )
]


infer = dict(
    partitioner=dict(type=SizePartitioner, max_task_size=20000),
    runner=dict(
        type=LocalRunner, max_num_workers=8,
        max_workers_per_gpu=1,
        task=dict(type=OpenICLInferTask)),
)
