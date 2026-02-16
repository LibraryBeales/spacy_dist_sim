from pathlib import Path
import pdfplumber
import logging

logging.getLogger("pdfminer").setLevel(logging.ERROR)

INPUT_FOLDER = r"your_input_folder"
OUTPUT_FOLDER = r"clean_text"

Path(OUTPUT_FOLDER).mkdir(exist_ok=True)

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
        print("BAD PDF:", path.name)
        return ""

def read_txt(path):
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except:
        return ""

count = 0

for p in Path(INPUT_FOLDER).glob("*"):
    if p.suffix.lower() == ".pdf":
        text = read_pdf(p)

    elif p.suffix.lower() == ".txt":
        text = read_txt(p)

    else:
        continue

    if not text.strip():
        continue

    out_name = p.stem + ".txt"
    out_path = Path(OUTPUT_FOLDER) / out_name
    out_path.write_text(text, encoding="utf-8")

    count += 1
    if count % 500 == 0:
        print("Converted:", count)

print("Done. Total text files:", count)
