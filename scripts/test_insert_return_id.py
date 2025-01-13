from catalogue import Catalogue, SQLDatabase

sql = SQLDatabase(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo")

cat = Catalogue(sql, 'inserttest', force=True)

x = {'test':123}
y = {'test':666}
id = cat.insert(x)
cat.insert(y)
print("Inserted ID:")
print(id)

print("contents")
print(cat.listContents())

