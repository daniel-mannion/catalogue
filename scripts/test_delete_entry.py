from catalogue import Catalogue, SQLDatabase

sql = SQLDatabase(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo")

cat = Catalogue(sql, 'deletetest', force=True)

x = {'test':123}
y = {'test':666}
cat.insert(x)
cat.insert(y)
print("Before delete:")
print(cat.listContents())

cat.delete({'test':123})
print("After delete")
print(cat.listContents())
