from pymongo import MongoClient

# MongoDB connection setup
mongo_uri = "mongodb://localhost:27017"  # Change if your MongoDB is hosted elsewhere
db_name = "candy_store_22"  # Ensure this matches your database name
collection_name = "transactions_20240201"  # Collection to fetch data from

# Establish connection
client = MongoClient(mongo_uri)
db = client[db_name]  # Select database
collection = db[collection_name]  # Select collection

# Fetch all records
documents = collection.find()

# Display results
for doc in documents:
    print(doc)
