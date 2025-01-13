from catalogue import Catalogue, SQLDatabase

sql = SQLDatabase(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo")

cat = Catalogue(sql, 'updatetest', force=True)

x = {'test':123}
id = cat.insert(x)
print(cat.listContents())

cat.update({"id":id}, {"test":666})

print("updated contents")
print(cat.listContents())

