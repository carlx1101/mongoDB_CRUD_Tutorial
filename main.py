from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

# Load .env password into the url 
load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://carlx1101:{password}@mongodbtutorial.u6n8v.mongodb.net/?retryWrites=true&w=majority"


client = MongoClient(connection_string)

# showing all databases
dbs = client.list_database_names()

# access specific database & print out its collections 
tutorial_db = client.Tutorial
collections = tutorial_db.list_collection_names()


def insert_tutorial_doc():
    collection = tutorial_db.tutorial
    tutorial_document = {
        "name": "Tutorial 1",
        "intake": "UCDF2005(1)ICT(SE)",
        "major": "Software Engineering"
    }
    # access document id 
    inserted_id = collection.insert_one(tutorial_document).inserted_id
    print(inserted_id)

# Mongodb auto create dn and collections if it doesnt exist
student = client.student
student_collection = student.student_collection 
    

def create_student_documents():
    first_names = ["Yip", "Wu", "Ng", "Wong", "Mohammed"]
    last_names = ["Kar Fai", "Ka Lok", "Li Sheng", "Wei Zhangw", "Muhsin Sultan"]
    ages = [21,20,20,20,18]

    x = zip(first_names, last_names, ages)
   
    documents = []
    for first_name, last_name, age in x:
        student_document = {"first_name": first_name, "last_name":last_name, "age":age}
        # student_collection.insert_one(student_document)
       
        documents.append(student_document)
    student_collection.insert_many(documents)

# QUERY 
# print data using pretter printer 
printer = pprint.PrettyPrinter()

def find_all_student():
    students = student_collection.find()
    for student in students:
        printer.pprint(student)


def find_student_muhsin():
    find_muhsin = student_collection.find_one({"first_name": "Mohammed"})
    printer.pprint(find_muhsin)

def number_of_student():
    # Method 1 
    student_number = student_collection.count_documents(filter={})

    # Method 2 - (not working yet fix it)
    # student_number = student_collection.find().count()
    print("Number of student",student_number)

def get_student_by_id(student_id):
    from bson.objectid import ObjectId

    _id = ObjectId(student_id)
    student = student_collection.find_one({"_id":_id})
    printer.pprint(student)

def get_age_range(min_age, max_age):
    query = {"$and": 
                [
                    {"age" : {"$gte": min_age}},
                    {"age" : {"$lte": max_age}},
                ]
            }
  

    students = student_collection.find(query).sort("age")
    for student in students:
        printer.pprint(student)

def project_columns():
    columns = {"_id":0, "first_name":1, "last_name":1}
    students = student_collection.find({}, columns)
    for student in students:
        printer.pprint(student)


# UPDATE 
def update_student_by_id(student_id):
    from bson.objectid import ObjectId 

    _id = ObjectId(student_id)

    # UPDATE METHOD   
    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "first","last_name": "last"}
    # }
    # student_collection.update_one({"_id":_id}, all_updates)

    # REMOVE
    # student_collection.update_one({"_id": _id}, {"$unset": {"new_field" : ""}})

    # REPLACE 
    student_collection.update_one({"_id": _id}, {"$unset": {"new_field" : ""}})

def replace_one(student_id):
    from bson.objectid import ObjectId 
    _id = ObjectId(student_id)

    new_doc = {
        "first_names" : "new first ",
        "last_name" : "new ",

    } 
    student_collection.replace_one({"_id" : _id}, new_doc)


def delete_student_by_id(student_id):
    from bson.objectid import ObjectId 
    _id = ObjectId(student_id)
    
    student_collection.delete_one({"_id":_id})

    #  to delete multiple 
    # student_collection.delete_many(query)

delete_student_by_id("6271ee1551d743847b79d605") 
