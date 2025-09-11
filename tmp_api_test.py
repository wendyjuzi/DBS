from src.api.db_api import DatabaseAPI

db = DatabaseAPI()
print(db.execute("CREATE TABLE t(id INT PRIMARY KEY, name STRING, age INT, score DOUBLE)"))
print(db.execute("INSERT INTO t VALUES (1,'Alice',20,95.5)"))
print(db.execute("SELECT * FROM t"))
print(db.execute("SELECT id,name FROM t WHERE id = 1"))

