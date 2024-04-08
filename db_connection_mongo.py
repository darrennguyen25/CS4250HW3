#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "corpus"
    DB_HOST = "localhost"
    DB_PORT = 27017

    client = MongoClient(DB_HOST, DB_PORT)
    db = client[DB_NAME]
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    index = {}
    docTextList = docText.lower().replace(".", "").replace("?", "").replace("!", "").replace(",", "").split()
    for text in docTextList:
        if text in index:
            index[text] = index.get(text) + 1
        else:
            index[text] = 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term = []
    for key, value in index.items():
        term.append({"term": key, "count": value, "num_chars": len(key)})

    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here

    document = {
        "_id": int(docId),
        "title": docTitle,
        "text": docText,
        "num_chars": len(docText),
        "date": docDate,
        "categories":{
            "category": docCat
        },
        "index": index,
        "terms": term
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": int(docId)})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    pipeline = [
        {"$project": {'title': 1, 'index': 1}}
    ]

    index = col.aggregate(pipeline)
    indexDict = {}
    for terms in index:
        key = list(terms['index'].keys())
        value = list(terms['index'].values())
        for k, v in zip(key, value):
            indexDict[k] = indexDict.get(k, "") + terms['title'] + ":" + str(v) + ","
    return indexDict
