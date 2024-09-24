from catalogue import Catalogue, SQLDatabase
import numpy as np

sql = SQLDatabase(database="demo",
                        host="localhost",
                        port="5432",
                        user="user",
                        password="demo")

# Create catalogue that doesn't exist by definiing structure
# columns_definition = [('texts',str),('numbers',float)]
# new_cat = Catalogue(sql, 'newcat3', force=True, columns=columns_definition)

# Create catalogue that doesn't exist by passing data into uninitialised catalogue
new_cat = Catalogue(sql, 'newcat6', force=True)
entry = {'path':'new_location',
         'accuracy':float(np.random.rand())}
new_cat.insert(entry)

print(new_cat.listContents())
breakpoint()

print(new_cat.filter({'path':'new_location'}))
breakpoint()

print(new_cat.filter({'path':'new_location','accuracy':0.249024}))
quit()
# List all catalogues which are part of the database.
catalogues = Catalogue.listCatalogues(sql)
for c in catalogues:
    print(c)

# Connect to specific catalogue by passing name
cat = Catalogue(sql, 'catalogue')

# View the headers/columns within the catalogue. Useulf if you forget.
headers = cat.listHeaders()
print(headers)

# VIew entire contents of catalogue. Returns as a dataframe
data = cat.listContents()

print(data)
print(data.info(verbose=True))
quit()
# Filter contents. Returns dataframe.
filtered_data = cat.filter({"col2":'b'})
print(filtered_data)

entry = cat.getBlankEntry()
entry['col1'] = 20
entry['col2'] = 'c'
entry['col4'] = 'test string'
print(entry)
cat.insert(entry)

quit()