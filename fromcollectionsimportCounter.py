import spacy
from pathlib import Path
from collections import Counter

# load model with vectors
nlp = spacy.load("en_core_web_lg")
nlp.max_length = 100000000


text_dir = Path("clean_text")

token_counts = Counter()
docs = []

print("Reading files...\n")

for path in text_dir.glob("*.txt"):
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        doc = nlp(text)
        docs.append(doc)

        for token in doc:
            if (
                token.has_vector
                and token.is_alpha
                and not token.is_stop
                and len(token.text) > 2
            ):
                token_counts[token.lemma_.lower()] += 1

        print(f"Processed: {path.name}")

    except Exception as e:
        print(f"ERROR reading {path.name}: {e}")

print("\nTop 50 most common tokens with vectors:\n")

for word, count in token_counts.most_common(50):
    print(f"{word}: {count}")

