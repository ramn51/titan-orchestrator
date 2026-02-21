import time
print("[Worker] Extracting raw data from source...")
time.sleep(2)
with open("data.csv", "w") as f:
    f.write("id,value\n1,100")
print("[Worker] Extraction complete.")