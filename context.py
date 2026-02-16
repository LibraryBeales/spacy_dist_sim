from collections import Counter, defaultdict

seed_words = {"law","act","statute","amendment","ordinance","legislation","bill"}

context_counts = Counter()

window = 5

for doc in docs:
    tokens = [t for t in doc if t.is_alpha and not t.is_stop]
    for i, tok in enumerate(tokens):
        if tok.lemma_.lower() in seed_words:
            start = max(0, i-window)
            end = min(len(tokens), i+window+1)

            for j in range(start, end):
                if j != i:
                    context_counts[tokens[j].lemma_.lower()] += 1
