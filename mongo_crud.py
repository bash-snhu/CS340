from pymongo import MongoClient
from bson.objectid import ObjectId

class DatabaseHandler(object):
    """ CRUD tools for managing a specified database and collection """
    
    def __init__(self, usr, passwrd, datab, coll):
        # Estalish parameters for connection to database
        USER = usr
        PASS = passwrd
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 30131
        DB = datab
        COL = coll
        
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER, PASS, HOST, PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]
        
    def connection_test(self):
        """ Uses single find_one query to establish valid connection """
        
        try:
            # Query database to prove connection
            x = self.collection.find_one()
            return(x)
        except Exception as e:
            print("Connection Failed : ", e)
            return False
        
    def create(self, data):
        """ Insert document into database given dictionary data """
        
        # Ensure data is not None or non-Dict
        if data is not None and type(data) is dict:
            try: 
                # Insert one document
                self.collection.insert_one(data)
                return True
            except Exception as e:
                print("Failed to insert document : ", e)
                return False
        else:
            print("Empty or Invalid Data provided.")
            return False
    
    def read(self, data):
        """ Query database using find() method and given dictionary data """
        
        # Ensure data is not None or non-dict type
        if data is not None and type(data) is dict:
            # Create results cursor to read data from
            results_cursor = self.collection.find(data)
            # Create list from results cursor
            results_list = list(results_cursor)
           
            # Return populuated or empty list of results
            return results_list
        else:
            print("Empty or Invalid Data Provided")
            
    def update(self, query, update_data):
        """ Update document given dictionary data """
        # Return number of updated items
        if query is not None and update_data is not None:
            updated = self.collection.update_many(query, update_data)
            return updated.matched_count
        elif query is None and update_data is None:
            return "Query and Provided Information are Empty"
        elif query is None:
            return "Query is Empty"
        elif update_data is None:
            return "Update Information is Empty"
    
    def delete(self, query):
        """ Deletes documents given dictionary query """
        if query is not None:
            deleted = self.collection.delete_many(query)
            return deleted.deleted_count
        else:
            return "Query is Empty"
            
    
    
    
    