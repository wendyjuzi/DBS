import random, csv
with open("students.csv","w",newline="",encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["id","name","age","score"])
    for i in range(1, 10001):
        w.writerow([i, f"User{i}", random.randint(16,30), round(random.uniform(50,100),1)])
print("OK -> students.csv")