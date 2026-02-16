import spacy
from pathlib import Path
import pdfplumber

nlp = spacy.load("en_core_web_lg")  # large model has vectors

def read_txt(path):
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def read_pdf(path):
    text = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text.append(t)
    return "\n".join(text)

def load_corpus(folder):
    texts = []
    for p in Path(folder).glob("*"):
        if p.suffix == ".txt":
            texts.append(read_txt(p))
        elif p.suffix == ".pdf":
            texts.append(read_pdf(p))
    return texts

texts = load_corpus("corpus_folder")
docs = list(nlp.pipe(texts))
