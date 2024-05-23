import os
import argparse
import PyPDF2
import re
from tqdm import tqdm

def extract_sentences_from_pdf(pdf_path):
    pdf_reader = PyPDF2.PdfReader(pdf_path)
    sentences = []
    for page_num in range(len(pdf_reader.pages)):
        page = pdf_reader.pages[page_num]
        text = page.extract_text()
        if text:
            # Replace newline characters with space
            text = text.replace('\n', ' ')
            # Split text into sentences using the improved regex
            page_sentences = re.split(r'(?<=[.?!])(?=\s|$|["\'\)\]])', text)
            # Filter out short sentences
            page_sentences = [sentence.strip() for sentence in page_sentences if len(sentence.strip()) >= 50]
            sentences.extend(page_sentences)
    return sentences

def write_sentences_to_txt(sentences, txt_path, tag):
    try:
        with open(txt_path, 'a', encoding='utf-8') as file:
            for sentence in sentences:
                file.write(f'__label__{tag} ' + sentence + '\n')
    except Exception as e:
        print(f"Error writing to file: {e}")

def process_pdfs_in_directory(pdf_dir, output_txt_path, tag):
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    for pdf_file in pdf_files:
        pdf_path = os.path.join(pdf_dir, pdf_file)
        sentences = extract_sentences_from_pdf(pdf_path)
        write_sentences_to_txt(sentences=sentences, 
                                txt_path=output_txt_path, 
                                tag=tag)

def main():
    parser = argparse.ArgumentParser(description="Extract sentences from PDF files in a directory and write them to a text file.")
    parser.add_argument("--input_dir", type=str, default="../data/astro_pdf/", help="Path to the directory containing PDF files")
    parser.add_argument("--output_dir", type=str, default="../output/astro_text.txt", help="Path to the output text file")
    parser.add_argument("--tag", type=str, default="negative", help="Add tag to train text")
    args = parser.parse_args()

    process_pdfs_in_directory(pdf_dir=args.input_dir, 
                              output_txt_path=args.output_dir,
                              tag=args.tag
                              )
    
    print(f"All PDF files in {args.input_dir} processed. Output written to {args.output_dir}")

if __name__ == "__main__":
    main()

