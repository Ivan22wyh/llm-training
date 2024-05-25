import os
import random
import argparse

def extract_sentences_from_txt(txt_dir):
    sentences = []
    txt_files = [f for f in os.listdir(txt_dir) if f.endswith('.txt')]
    for txt_file in txt_files:
        txt_path = os.path.join(txt_dir, txt_file)
        with open(txt_path, 'r', encoding='utf-8') as file:
            for line in file:
                sentence = line.strip()
                if sentence:
                    sentences.append(sentence)
    return sentences

def shuffle_sentences(sentences):
    random.shuffle(sentences)

def write_sentences_to_txt(sentences, output_path):
    with open(output_path, 'w', encoding='utf-8') as file:
        for sentence in sentences:
            file.write(sentence + '\n')

def main(args):
    txt_dir = args.input_dir
    output_path = args.output_dir
    
    sentences = extract_sentences_from_txt(txt_dir)
    shuffle_sentences(sentences)
    write_sentences_to_txt(sentences, output_path)
    
    print("Sentences shuffled and written to", output_path)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Shuffle sentences from txt files and write to a new txt file")
    parser.add_argument("--input_dir", type=str, help="Input directory containing txt files (default: 'input')")
    parser.add_argument("--output_dir", type=str, help="Output file path for shuffled sentences (default: 'output.txt')")
    args = parser.parse_args()
    
    main(args)
