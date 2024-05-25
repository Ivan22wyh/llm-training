from wordcloud import WordCloud, STOPWORDS
import matplotlib.pyplot as plt
import spacy

# 加载模型
nlp = spacy.load("en_core_web_sm") 
# 使用模型，传入句子即可
doc = nlp("Apple is looking at buying U.K. startup for $1 billion")
# 获取分词结果
print([token.text for token in doc])


def generate_word_cloud(file_path):
    # 读取文件内容
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    stopwords = set(STOPWORDS)
    stopwords.update(["__label__negative", "Mr __label__negative", "__label__negative", "__label__negative Fairfax", "__label__negative Reed"])  # 添加自定义停用词

    # 生成词云
    wordcloud = WordCloud(width=800, height=400, background_color='white', stopwords=stopwords).generate(content)
    print(wordcloud.words_)

    # 显示词云
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')  # 不显示坐标轴
    plt.savefig('janeye.png')
    plt.show()

# 使用示例
file_path = 'data/fasttext/input/raw_data/non_astro_text.txt'
generate_word_cloud(file_path)
