from catalogue import SQLDatabase, Catalogue

sql = SQLDatabase(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo")

float_cat = Catalogue(sql, 'float_test', force=True)
entry = {'val':0.1}
# float_cat.insert(entry)

print(float_cat.filter(entry))