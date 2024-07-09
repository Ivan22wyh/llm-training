#!/bin/bash
python3 -m torch.distributed.run \
--nproc_per_node=8 \
--nnodes=1 \
--standalone \
/mnt/geogpt-gpfs/llm-course/home/wenyh/LLaMA-Factory/src/train.py /mnt/geogpt-gpfs/llm-course/home/masg/llama_factory.yaml
