import random
from sklearn.model_selection import train_test_split
import pickle
import pandas as pd
#import fasttext
#from utils_str import normalize

collection = "pubmed"
issn_map = pickle.load(open(f"../data/fasttext/input/raw_data/issn_dict.pkl", "rb"))

for is_stratified in [True, False]:

    if is_stratified:
        # download from https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/sampling/sample_data_pubmed_stratTrue_50000.json.gz
        sample_data = "../data/fasttext/input/raw_data/sample_data_pubmed_stratTrue_50000.json"
    else:
        #download from https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/sampling/sample_data_pubmed_stratFalse_1000000.json.gz
        sample_data = "../data/fasttext/input/raw_data/sample_data_pubmed_stratFalse_1000000.json"
    data = pd.read_json(sample_data, orient="records", lines=True).to_dict(orient='records')
    if not is_stratified:
        data = random.sample(data, 850000)
    #download_object("tmp", sample_data.split('/')[-1], sample_data)
    print("len data = "+str(len(data)), flush=True)
    
    print("len issn_map = "+str(len(issn_map)), flush=True)
    for elt in data:

        all_fields, all_fields_health = [], []
        for issn in [e.get('issn_electronic'), e.get('issn_print')]:
            if issn in issn_dict:
                all_fields += issn_dict[issn]
            if issn in issn_dict_health:
                all_fields_health += issn_dict_health[issn]
            
        e['fields'] = all_fields
        e['fields_health'] = all_fields_health
    
        for_codes4 = []
        for_codes2 = []
        for f in all_fields:
            if len(for_dict2[f]) == 4:
                for_codes4.append(for_dict2[f])
            for_codes2.append(for_dict2[f][0:2])
        e['for_codes4'] = list(set(for_codes4))
        e['for_codes2'] = list(set(for_codes2))

        if '_id' in elt:
            del elt['_id']
        #current_label_text = []
        #for issn_type in ['issn_electronic', 'issn_print']:
        #    issn = elt[issn_type]
        #    if issn in issn_map:
        #        current_label_text += issn_map[issn]
        #current_label_text = list(set(current_label_text))
        #elt["labels_text"] = current_label_text

    data_with_label = [ e for e in data if len(e['for_codes2'])]
    data_train, data_test = train_test_split(data_with_label, test_size = 0.1, random_state = 0)

    for data_type in ["train", "test"]:
        print(data_type, flush=True)
        outfile = {}
        for f in ['title', 'abstract', 'keywords', 'mesh_headings', 'journal_title']:
            outfile[f] = open(f"../data/fasttext/input/train_data/{collection}_{data_type}_{f}_strat{is_stratified}.txt", "w", encoding='utf-8')
            outfile[f].close()

        for f in ['title', 'abstract', 'keywords', 'mesh_headings', 'journal_title']:
            outfile[f] = open(f"../data/fasttext/input/train_data{collection}_{data_type}_{f}_strat{is_stratified}.txt", "w+", encoding='utf-8')
            print(f, flush=True)

            if data_type == "train":
                current_data = data_train
            else:
                current_data = data_test

            for ix, elt in enumerate(current_data):
                if ix % 100000 == 0:
                    print(ix, end=',', flush=True)

                current_words = elt.get(f)
                if current_words is None:
                    continue

                if isinstance(current_words, list):
                    current_words = " ".join(current_words)

                if f == "abstract" and len(current_words.split(" ")) < 20:
                    continue
                elif f == "title" and len(current_words.split(" ")) < 10:
                    continue
                elif len(current_words.split(" ")) < 2:
                    continue
                elif len(current_words) < 5:
                    continue

                #current_words = normalize(current_words)

                #labels = ["__label__" + label.replace(' ','_') for label in elt.get('labels_text', [])]
                for label_code, sublabel_code in zip(elt.get('for_codes2'), elt.get('for_codes4')):
                    if sublabel_code == '0201': label = 'astro'
                    elif label_code in ['1', '2', '3', '4']: label = 'astro_related'
                    else: label = 'non_astro'


                #tags = " ".join(labels)

                newline = current_words + " " + label + "\n"

                outfile[f].write(newline)
        outfile[f].close()
        print(flush=True)
