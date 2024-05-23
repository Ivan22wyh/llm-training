from transformers import AutoModelForCausalLM, AutoTokenizer

tokenizer = AutoTokenizer.from_pretrained("EleutherAI/gpt-neox-20b")

model = AutoModelForCausalLM.from_pretrained("/mnt/geogpt-gpfs/llm-course/home/wenyh/output/hfnew")

message = ["who are you"]

inputs = tokenizer(message, return_tensors='pt', return_token_type_ids=False)

response = model.generate(**inputs, max_new_tokens=100, do_sample=True, top_k=50, top_p=0.95)

print(tokenizer.batch_decode(response, skip_special_tokens=True)[0])