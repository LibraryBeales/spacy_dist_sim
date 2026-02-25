import spacy
from pathlib import Path
from collections import Counter, defaultdict
import torch

# ---- GPU Setup ----
print("Checking GPU availability...")
if torch.cuda.is_available():
    print(f"✓ GPU available: {torch.cuda.get_device_name(0)}")
    print(f"✓ CUDA version: {torch.version.cuda}")
    spacy.prefer_gpu()
else:
    print("WARNING: No GPU detected, running on CPU")

# ---- Load model ----
print("\nLoading spaCy model...")
nlp = spacy.load("en_core_web_lg")
nlp.max_length = 100000000
print(f"✓ Model loaded on: {'GPU' if spacy.prefer_gpu() else 'CPU'}\n")

# ---- Check files ----
text_dir = Path("clean_text")
print(f"Looking in directory: {text_dir.absolute()}")
print(f"Directory exists: {text_dir.exists()}")

files = list(text_dir.glob("*.txt"))
print(f"Found {len(files)}.txt files\n")

if len(files) == 0:
    print("ERROR: No.txt files found!")
    print("   Check that:")
    print("   1. The 'clean_text' folder exists")
    print("   2. It contains.txt files")
    print("   3. You're running the script from the correct directory")
    exit()

# ---- Seed words ----
seeds = ["law", "act", "statute", "amendment", "ordinance", "legislation", "bill"]
seed_tokens = [nlp.vocab[w] for w in seeds if nlp.vocab[w].has_vector]

print(f"Seed words with vectors: {[t.text for t in seed_tokens]}")
print(f"Seeds with vectors: {len(seed_tokens)}/{len(seeds)}\n")

if len(seed_tokens) == 0:
    print("ERROR: No seed words have vectors!")
    exit()

# ---- Storage ----
sim_scores = defaultdict(list)
token_freq = Counter()

# ---- Counters for debugging ----
total_tokens = 0
filtered_tokens = 0

# ---- Prepare texts for batch processing ----
print("Loading all texts into memory...")
texts = []
file_paths = []

for path in files:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        if len(text.strip()) > 0:
            texts.append(text)
            file_paths.append(path)
        else:
            print(f"WARNING: {path.name} is empty")
    except Exception as e:
        print(f"ERROR reading {path.name}: {e}")

print(f"✓ Loaded {len(texts)} non-empty files\n")

if len(texts) == 0:
    print("ERROR: No valid text files to process!")
    exit()

# ---- Process with GPU batching in chunks ----
print("Processing documents with GPU acceleration...\n")

BATCH_SIZE = 8
CHUNK_SIZE = 500  # Process 500 files at a time, then clear memory

processed_count = 0
total_files = len(texts)

for chunk_start in range(0, total_files, CHUNK_SIZE):
    chunk_end = min(chunk_start + CHUNK_SIZE, total_files)
    chunk_texts = texts[chunk_start:chunk_end]
    
    print(f"\nProcessing chunk: files {chunk_start+1} to {chunk_end}")
    
    for doc in nlp.pipe(chunk_texts, batch_size=BATCH_SIZE, n_process=1):
        processed_count += 1
        
        for token in doc:
            total_tokens += 1
            
            if (
                token.has_vector
                and token.is_alpha
                and not token.is_stop
                and len(token.text) > 2
            ):
                filtered_tokens += 1
                token_freq[token.lemma_.lower()] += 1

                sims = [token.similarity(seed) for seed in seed_tokens]
                if sims:
                    sim_scores[token.lemma_.lower()].append(max(sims))
        
        if processed_count % 10 == 0 or processed_count == total_files:
            percent = (processed_count / total_files) * 100
            print(f"Progress: {processed_count}/{total_files} files ({percent:.1f}%) - Tokens: {filtered_tokens:,}")
    
    # Clear GPU memory after each chunk
    torch.cuda.empty_cache()
    print(f"✓ Chunk complete. GPU memory cleared.")

# ---- Debug output ----
print(f"\n{'='*60}")
print(f"Total tokens processed: {total_tokens:,}")
print(f"Tokens passing filters: {filtered_tokens:,}")
print(f"Unique words with similarity scores: {len(sim_scores):,}")
print(f"Unique words in frequency counter: {len(token_freq):,}")
print(f"{'='*60}\n")

if len(sim_scores) == 0:
    print("ERROR: No tokens passed the filters!")
    print("\nPossible reasons:")
    print("   1. Text files are empty or contain no valid text")
    print("   2. Filters are too restrictive")
    print("   3. Tokens don't have vectors in this model")
    exit()

print("Computing averages...\n")

# ---- Scoring ----
results = []

for word, scores in sim_scores.items():
    max_sim = max(scores)
    mean_sim = sum(scores) / len(scores)
    freq = token_freq[word]

    results.append((word, freq, max_sim, mean_sim))

# ---- Sort by max similarity ----
results.sort(key=lambda x: x[2], reverse=True)

print(f"\n{'='*60}")
print(f"Top {min(50, len(results))} candidates by MAX similarity to any seed:")
print(f"{'='*60}\n")

for word, freq, max_sim, mean_sim in results[:50]:
    print(f"{word:20s} freq={freq:5d}  max={max_sim:.3f}  mean={mean_sim:.3f}")