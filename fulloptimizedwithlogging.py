import spacy
from pathlib import Path
import pdfplumber
from collections import Counter, defaultdict
import math
import logging

logging.getLogger("pdfminer").setLevel(logging.ERROR)

CORPUS_FOLDER = "your_folder_here"

seed_words = {"law","act","statute","amendment","ordinance","legislation","bill"}
window = 5

nlp = spacy.load("en_core_web_lg", disable=["ner"])  
# disable NER for speed

# -------------------------
# File readers
# -------------------------

def read_txt(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except:
        return ""

def read_pdf(path):
    try:
        text = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text.append(t)
        return "\n".join(text)
    except:
        print("Skipping bad PDF:", path.name)
        return ""

# -------------------------
# Generator: stream files
# -------------------------

def text_stream(folder):
    for p in Path(folder).glob("*"):
        if p.suffix.lower() == ".txt":
            t = read_txt(p)
            if t.strip():
                yield t

        elif p.suffix.lower() == ".pdf":
            t = read_pdf(p)
            if t.strip():
                yield t

# -------------------------
# Build context vectors
# -------------------------

word_contexts = defaultdict(Counter)

doc_count = 0

for doc in nlp.pipe(text_stream(CORPUS_FOLDER), batch_size=20):
    doc_count += 1
    if doc_count % 500 == 0:
        print("Processed docs:", doc_count)

    tokens = [t for t in doc if t.is_alpha and not t.is_stop]

    for i, tok in enumerate(tokens):
        lemma = tok.lemma_.lower()

        start = max(0, i-window)
        end = min(len(tokens), i+window+1)

        for j in range(start, end):
            if j != i:
                word_contexts[lemma][tokens[j].lemma_.lower()] += 1

print("Total processed:", doc_count)

# -------------------------
# Seed context vector
# -------------------------

seed_context = Counter()
for w in seed_words:
    seed_context += word_contexts[w]

# -------------------------
# Cosine similarity
# -------------------------

def cosine(a, b):
    common = set(a) & set(b)
    num = sum(a[x]*b[x] for x in common)

    sum1 = sum(v*v for v in a.values())
    sum2 = sum(v*v for v in b.values())

    denom = math.sqrt(sum1)*math.sqrt(sum2)
    return num/denom if denom else 0

scores = {}

for word, ctx in word_contexts.items():
    if word in seed_words:
        continue

    sim = cosine(ctx, seed_context)
    if sim > 0.2:
        scores[word] = sim

print("\nWords used similarly to your legal terms:\n")

for w, s in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:60]:
    print(f"{w:20} {s:.3f}")
