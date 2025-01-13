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
        tables, _ = self.select('table_name', 'information_schema.tables',conditions)
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
        
    def query(self, query_str, vars=None, insert=False, delete=False):
        conn = psycopg2.connect(**self.connection_info)
        logging.info("SQL Query: %s"%query_str)
        cursor = conn.cursor()
        cursor.execute(query_str, vars)
        if(insert or delete):
            conn.commit()
            result = cursor.fetchall()
        else:
            result = cursor.fetchall()
        conn.close()
        if(cursor.description):
            colnames = [desc[0] for desc in cursor.description]
            return result, colnames
        else:
            return result, None
    
    def queryTable(self, table_name, conditions=None, return_elements="*"):
        data, colnames = self.select(return_elements, table_name, conditions)
        return data, colnames
    def queryTableColumnNames(self, table_name):
        conditions = {'table_name':table_name}
        resp, _ = self.select('column_name','information_schema.columns',conditions)
        return resp
    def insertInTable(self, entry, table_name):
        destination_str = [k for k in entry.keys()]
        destination_str = ', '.join(destination_str)
        destination_str = "%s(%s)"%(table_name, destination_str)

        value_str = ["%s" for k in entry.keys()]
        value_str = ', '.join(value_str)
        value_str = '(%s)'%value_str
        value_vars = [entry[k] for k in entry.keys()]

        sql_insert_query = 'INSERT INTO %s VALUES %s RETURNING id'%(destination_str, value_str)
        print(sql_insert_query)
        return self.query(sql_insert_query, value_vars, insert=True)[0][0][0]
    def delete_from_table(self, conditions, table_name, tolerance_real=1e-6):
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
        sql_command = "DELETE FROM %s WHERE %s RETURNING id"%(table_name, conditions_str)
        print(sql_command)
        conditions_vars = [conditions[k] for k in conditions.keys()]
        return self.query(sql_command, conditions_vars, delete=True)[0][0][0]
    def filterTable(self, table_name, conditions):
        data, colnames = self.select('*', table_name, conditions)
        return data, colnames
    def getPrimaryKeyColumn(self, table_name):
        select_element = 'C.COLUMN_NAME'
        source = 'INFORMATION_SCHEMA.TABLE_CONSTRAINTS T JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C ON C.CONSTRAINT_NAME=T.CONSTRAINT_NAME'
        conditions = {'C.TABLE_NAME':table_name,
                      'T.CONSTRAINT_TYPE':'PRIMARY KEY'}
        resp, _ = self.select(select_element, source, conditions)
        return resp[0][0]
    
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
        return primary_key
    def listContents(self):
        contents, colnames = self.database.queryTable(self.name)
        contents_df = pd.DataFrame(contents, columns=colnames)
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
            id = self.database.insertInTable(entry, self.name)
        else:
            columns = [(k, type(entry[k])) for k in entry.keys()]
            self.database.createTable(self.name, columns)
            self.primary_key_column = self.getPrimaryKey()
            self.initialised = True
            id = self.insert(entry)
        return id
    def delete(self, conditions):
        return self.database.delete_from_table(conditions, self.name)
    def filter(self, conditions):
        data, colnames = self.database.filterTable(self.name, conditions)
        data_df = pd.DataFrame(data, columns=colnames)
        return data_df
    @staticmethod
    def listCatalogues(database):
        tables = database.listTables()
        catalogues = [Catalogue(database, cat_name[0]) for cat_name in tables]
        return catalogues

        
        
