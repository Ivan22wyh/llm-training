import fasttext
from tqdm import tqdm

stats = {}

for f in ['journal_title', 'title', 'keywords', 'abstract', 'mesh_headings']:

    print("training "+f, flush=True)
    
    for is_stratified in [True, False]:
        
        model = fasttext.train_supervised(f'../data/fasttext/input/train_data/pubmed_train_{f}_strat{is_stratified}.txt',
                                            wordNgrams = 2,
                                            minCount = 20,
                                            loss='ova',
                                            epoch = 50)
        model_filename = f"../data/fasttext/output/pubmed_model_{f}_strat{is_stratified}.model"
        model.save_model(model_filename)
        #upload_object("models", model_filename)  
        print(f"tesing {model_filename.split('output/')[1]}")

        stats[f"{f}_train_{is_stratified}_test_{is_stratified}"] = model.test_label(f'../data/fasttext/input/train_data/pubmed_test_{f}_strat{is_stratified}.txt', k=3, threshold=0.5)

        test = model.test(f'../data/fasttext/input/train_data/pubmed_test_{f}_strat{is_stratified}.txt', k=3, threshold=0.5)
        precision = test[1]
        recall = test[2]
        f1 = 2*(recall * precision) / (recall + precision)
        global_stats = {'precision': precision, 
                       'recall': recall,
                       'f1score': f1}
        stats[f"{f}_train_{is_stratified}_test_{is_stratified}"]['global'] = global_stats

