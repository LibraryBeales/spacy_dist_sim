from langdetect import detect, DetectorFactory
from pathlib import Path
import csv

DetectorFactory.seed = 0  # reproducible results

text_dir = Path("clean_text")
files = list(text_dir.glob("*.txt"))

print(f"Found {len(files)} files\n")

with open("language_report.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["filename", "language", "character_count"])

    for i, path in enumerate(files, 1):
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
            char_count = len(text)

            if char_count < 50:
                lang = "TOO_SHORT"
            else:
                lang = detect(text[:5000])

            writer.writerow([path.name, lang, char_count])
            print(f"[{i}/{len(files)}] {path.name} → {lang}")

        except Exception as e:
            writer.writerow([path.name, "ERROR", 0])
            print(f"[{i}/{len(files)}] ERROR: {path.name} — {e}")

print("\nDone")
