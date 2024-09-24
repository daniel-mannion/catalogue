import psycopg2
import logging
import pandas as pd
import psycopg2.extensions as psy
import datetime



def pythonTypeToSQLType(p_type):
    if(p_type is bool):
        return 'bool'
    elif(p_type is float):
        return 'real'
    elif(p_type is int):
        return 'integer'
    elif(p_type is str):
        return 'varchar'
    elif(p_type is datetime.date):
        return 'date'
    elif(p_type is datetime.time):
        return 'time'
    elif(p_type is datetime.datetime):
        return 'timestamp'
    else:
        raise Exception('This data type is not currently supported: %s'%str(p_type))

class SQLDatabase:
    def __init__(self, database, host, port, user, password):
        self.connection_info = {"database": database,
                                "host":host,
                                "user":user,
                                "password":password,
                                "port":port}
    def listTables(self):
        conditions = {"table_schema":'public',
                      "table_type":'BASE TABLE'}
        tables = self.select('table_name', 'information_schema.tables',conditions)
        return tables
    
    def select(self, element, source, conditions=None, tolerance_real=1e-6):
        if(conditions is None):
            sql_command = "SELECT %s FROM %s"%(element, source)
            print(sql_command)
            return self.query(sql_command)
        else:
            conditions_str = []
            for k in conditions.keys():
                sql_type = pythonTypeToSQLType(type(conditions[k]))
                if(sql_type == 'varchar'):
                    conditions_str.append(k+"='%s'"%(conditions[k]))
                elif(sql_type == 'real'):
                    conditions_str.append("abs((%s-%f)/(%f))<=%f"%(k, conditions[k],conditions[k], tolerance_real))
                else:
                    conditions_str.append("%s=%s"%(k, conditions[k]))
            
            conditions_str = " AND ".join(conditions_str)
            sql_command = "SELECT %s FROM %s WHERE %s"%(element, source, conditions_str)
            print(sql_command)
            conditions_vars = [conditions[k] for k in conditions.keys()]
            return self.query(sql_command, conditions_vars)
        
    def query(self, query_str, vars=None, insert=False):
        conn = psycopg2.connect(**self.connection_info)
        logging.info("SQL Query: %s"%query_str)
        cursor = conn.cursor()
        cursor.execute(query_str, vars)
        if(insert):
            conn.commit()
            result = None
        else:
            result = cursor.fetchall()
        conn.close()
        return result
    
    def queryTable(self, table_name, conditions=None, return_elements="*"):
        return self.select(return_elements, table_name, conditions)
    def queryTableColumnNames(self, table_name):
        conditions = {'table_name':table_name}
        return self.select('column_name','information_schema.columns',conditions)
    def insertInTable(self, entry, table_name):
        destination_str = [k for k in entry.keys()]
        destination_str = ', '.join(destination_str)
        destination_str = "%s(%s)"%(table_name, destination_str)

        value_str = ["%s" for k in entry.keys()]
        value_str = ', '.join(value_str)
        value_str = '(%s)'%value_str
        value_vars = [entry[k] for k in entry.keys()]

        sql_insert_query = 'INSERT INTO %s VALUES %s'%(destination_str, value_str)
        print(sql_insert_query)
        self.query(sql_insert_query, value_vars, insert=True)
    def filterTable(self, table_name, conditions):
        data = self.select('*', table_name, conditions)
        return data
    def getPrimaryKeyColumn(self, table_name):
        select_element = 'C.COLUMN_NAME'
        source = 'INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME'
        conditions = {'C.TABLE_NAME':table_name,
                      'T.CONSTRAINT_TYPE':'PRIMARY KEY'}
        return self.select(select_element, source, conditions)
    
    def createTable(self, table_name, columns):
        column_datatypes = [pythonTypeToSQLType(c[1]) for c in columns]
        column_names = [c[0] for c in columns]

        query_str = "CREATE TABLE %s"%table_name
        columns_str = ["%s %s"%(column_names[n],column_datatypes[n]) for n in range(len(column_names))]
        columns_str = ", ".join(columns_str)
        query_str = "%s(id int GENERATED ALWAYS AS IDENTITY, %s, PRIMARY KEY (id))"%(query_str, columns_str)

        print(query_str)
        self.query(query_str, insert=True)
        # Now add auto increment and not null to primary key
        # query_str = "ALTER TABLE %s ADD CONSTRAINT NOT NULL UNIQUE(%s)"%(table_name, primary_key)
        # self.query(query_str, insert = True)
        # query_str = "ALTER TABLE %s ADD CONSTRAINT IDENTITY UNIQUE(%s)"%(table_name, primary_key)
        # self.query(query_str, insert = True)

class Catalogue:
    def __init__(self, database, catalogue_name, force=False, columns=None):
        self.database = database
        self.name = catalogue_name
        if(not self.exists()):
            if(force):
                if(columns is None):
                    self.initialised = False
                else:
                    self.database.createTable(catalogue_name, columns)
                    self.initialised = True
            else:
                raise Exception("Table with name %s does not exist in the database"%catalogue_name)
        else:
            self.initialised = True
        if(self.initialised):
            self.primary_key_column = self.getPrimaryKey()
        else:
            self.primary_key_column = None
    def exists(self):
        tables = self.database.listTables()
        tables = [l[0] for l in tables]
        return self.name in tables
    def __str__(self):
        return "Catalogue with Name: %s"%self.name
    def getPrimaryKey(self):
        primary_key = self.database.getPrimaryKeyColumn(self.name)
        return primary_key[0][0]
    def listContents(self):
        headers = self.listHeaders()
        return_elements = ", ".join(headers)
        contents = self.database.queryTable(self.name, return_elements = return_elements)
        contents_df = pd.DataFrame(contents, columns=headers)
        contents_df.set_index(self.primary_key_column)
        return contents_df
    def listHeaders(self):
        headers = self.database.queryTableColumnNames(self.name)
        headers = [h[0] for h in headers]
        return headers
    def getBlankEntry(self):
        headers = self.listHeaders()
        blank_entry = {h:None for h in headers}
        return blank_entry
    def insert(self, entry):
        if(self.initialised):
            self.database.insertInTable(entry, self.name)
        else:
            columns = [(k, type(entry[k])) for k in entry.keys()]
            self.database.createTable(self.name, columns)
            self.primary_key_column = self.getPrimaryKey()
            self.initialised = True
            self.insert(entry)
    def filter(self, conditions):
        data = self.database.filterTable(self.name, conditions)
        headers = self.listHeaders()
        data_df = pd.DataFrame(data, columns=headers)

        return data_df
    @staticmethod
    def listCatalogues(database):
        tables = database.listTables()
        catalogues = [Catalogue(database, cat_name[0]) for cat_name in tables]
        return catalogues

        
        
