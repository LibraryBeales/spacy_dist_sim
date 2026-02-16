import spacy
from pathlib import Path
import pdfplumber
from collections import Counter, defaultdict
import math

CORPUS_FOLDER = "C:/Users/rdb104/Documents/repos/climatescrape/scraped_policy_docs"

seed_words = {"law","act","statute","amendment","ordinance","legislation","bill"}
window = 5

nlp = spacy.load("en_core_web_lg")

# -------------------------
# File readers
# -------------------------

def read_txt(path):
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def read_pdf(path):
    try:
        text = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text.append(t)

        return "\n".join(text)

    except Exception as e:
        print(f"Skipping bad PDF: {path.name}")
        return ""

# -------------------------
# Load all files
# -------------------------

texts = []

for p in Path(CORPUS_FOLDER).glob("*"):
    if p.suffix.lower() == ".txt":
        texts.append(read_txt(p))
    elif p.suffix.lower() == ".pdf":
        texts.append(read_pdf(p))

print("Loaded files:", len(texts))

# -------------------------
# Record Bad pdfs
# -------------------------

bad_files = []

def read_pdf(path):
    try:
        text = []
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text.append(t)
        return "\n".join(text)

    except Exception:
        bad_files.append(path.name)
        return ""

# -------------------------
# Run spaCy
# -------------------------

docs = list(nlp.pipe(texts, batch_size=20))
print("Docs created:", len(docs))

# -------------------------
# Build context vectors
# -------------------------

word_contexts = defaultdict(Counter)

for doc in docs:
    tokens = [t for t in doc if t.is_alpha and not t.is_stop]

    for i, tok in enumerate(tokens):
        lemma = tok.lemma_.lower()

        start = max(0, i-window)
        end = min(len(tokens), i+window+1)

        for j in range(start, end):
            if j != i:
                word_contexts[lemma][tokens[j].lemma_.lower()] += 1

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

# -------------------------
# Results
# -------------------------

print("\nWords used similarly to your legal terms:\n")

for w, s in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:50]:
    print(f"{w:20} {s:.3f}")
