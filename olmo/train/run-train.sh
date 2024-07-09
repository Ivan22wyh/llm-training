export OMP_NUM_THREADS=16
python -u -m torch.distributed.run \
  --nproc_per_node=4 \
  --log-dir=${OUTPUT_DIR}/logs \
  --tee=3 \
  /mnt/geogpt-gpfs/llm-course/home/lfu/OLMo/scripts/train.py \
  ${INPUT_DIR}/OLMo-0.3B-mixings-v3-fast-nowandb.yaml \
  --save-folder=${OUTPUT_DIR}
