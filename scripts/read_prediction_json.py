import os
import json
import glob
from tqdm import tqdm

def get_all_files(directory, filetype):
    return sorted(glob.glob(os.path.join(directory, f'**/*{filetype}'), recursive=True))

def read_json_file(file):
    with open(file, 'r') as fp:
        return json.load(fp)

def filter_astro_text(json_data, astro_data, astro_data_direct):
    astro_keywords = {
        "astrophysics", "astronomy", "gravitation", "neutron", "telescope", "black hole", "main sequence", "nebula", "asteroid", "cosmology", "redshift", "supernova", "quasar", "pulsar", "exoplanet", "spectroscopy", 
    }

    for id_, text, prob, tag in tqdm(zip(json_data['id'], json_data['text'], json_data['prob'], json_data['tag'])):
        text_lower = text.lower()
        if prob > 0.9 and tag == '__label__astro':
            astro_data.append({
                'id':  id_,
                'text': f"{text}\n",
                'prob': prob
            })
            continue

        if any(keyword in text_lower for keyword in astro_keywords):
            astro_data_direct.append({
                'id':  id_,
                'text': f"{text}\n",
                'prob': prob
            })

def process_astro_data(file):
    json_data = read_json_file(file)
    astro_data = []
    astro_data_direct = []
    filter_astro_text(json_data, astro_data, astro_data_direct)
    return astro_data, astro_data_direct

def main():
    data_path = '/mnt/geogpt-gpfs/llm-course/home/wenyh/data/astro_mining_data/v2'
    json_file_list = get_all_files(data_path, filetype='.json')

    for i, json_file in enumerate(json_file_list[:3]):
        print(f'reading {i+1} of {len(json_file)} file ...')
        astro_data, astro_data_direct = process_astro_data(json_file)
        base_filename = os.path.basename(json_file).split('.json')[0]

        print(f'reading finish, start writing {i+1} of {len(json_file)} file ...')
        with open(f'{data_path}/fineweb_astro/{base_filename}_astro.json', 'w+') as f:
            json.dump(astro_data, f, indent=4)
        #with open(f'{data_path}/fineweb_astro/{base_filename}_exact_astro.json', 'w+') as f:
        #    json.dump(astro_data_direct, f, indent=4)
        #with open('a.txt', 'w+') as f:
        #    f.writelines([f"__label__astro {x['text']}" for x in astro_data_direct[:1000]])
        with open('a.txt', 'a+') as f:
            f.writelines([f"__label__astro {x['text']}" for x in astro_data[:]])

    return

if __name__ == '__main__':
    main()
    """ def find_keywords(text, keywords):
        found_keywords = []
    # 将文本分割成单词
        words = text.lower()
        if any(keyword in words for keyword in keywords):
            print(1)
    # 遍历关键词列表
        for keyword in keywords:
            # 检查关键词是否在文本中
            if keyword.lower() in words:
                found_keywords.append(keyword)
        return found_keywords

# 测试函数
    text = "resources for distance learning the windmill school staff would like to share these resources with you to enhance your distance learning time with your preschooler. additional activities and resources will be added each week, so please check back for new ideas. it is our hope that these activities will enhance the time you and your preschooler spend together. have you ever wanted to go on a safari!! well now you can! take a walk through the cincinnati zoo to check out all the animals! be sure to stop and hang out with fiona the hippo! http://cincinnatizoo.org/home-safari-resources/ what's more fun than getting messy? nothing! roll your sleeves up and show your parents how to get messy! check out these ooey gooey lady sensory activities! did you know you can make play dough at home? here's our windmill recipe: 2 cups of flour 1 cup of salt 2 tsp of cream of tartar 2 tbsp of oil 2 cups of water drops of food coloring add all of the dry ingredients together first and mix. add oil, water and food coloring and mix. cook over the stove top on medium heat. stir constantly until a ball forms. once the ball has formed, remove from heat and knead the dough. store the playdough in a ziploc bag or airtight container! next time you are on a walk, look for some california poppies! have you ever noticed what happens to the poppies when it is cloudy or night time? check out this link to cosmic kids yoga www.youtube.com/user/cosmickidsyoga make a musical wall tie your pots and pans to the fence! add a wooden spoon or a serving spoon! make some music!! balloon blow up materials: plastic water bottle, 2 teaspoons of sugar, 1 packet of yeast, 1/2 a cup of warm water procedure: put the sugar and yeast into the bottle. carefully pour in the warm water. put the neck of the balloon over the water bottle. over the course of the morning the balloon will inflate. try these fun ideas for art and science projects at dreme: family math family math is parents, caregivers and young children engaging each other in fun ear"
    keywords = ["astrophysics", "astronomy", "physics", "gravitation", "neutron", "telescope", "gaia", "black hole", "main sequence", "nebula", "comet", "asteroid", "constellation", "cosmology", "redshift", "supernova", "quasar", "pulsar", "exoplanet", "spectroscopy", "cosmic"]

    found_keywords = find_keywords(text, keywords)
    print(found_keywords)


 """