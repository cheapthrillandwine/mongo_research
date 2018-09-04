import csv
import os

if __name__ == "__main__":
    fp = open(os.path.join(r"./", "test3.csv"), 'w')
    writer= csv.DictWriter(fp, fieldnames = ["DateTime"])
    writer.writeheader()
    writer.writerow({'DateTime': 1})

