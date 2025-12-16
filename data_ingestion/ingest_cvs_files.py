import os
import shutil

RAW_PATH = "../data/raw/"
BRONZE_PATH = "../data_lake/bronze/"

def ingest_raw_files():
    os.makedirs(BRONZE_PATH, exist_ok=True)

    for file in os.listdir(RAW_PATH):
        if file.endswith(".csv"):
            src = os.path.join(RAW_PATH, file)
            dst = os.path.join(BRONZE_PATH, file)
            shutil.copy(src, dst)
            print(f"[<] Arquivo movido para bronze: {dst}")

if __name__ == "__main__":
    ingest_raw_files()
    print("Ingestão de arquivos CSV concluída.")