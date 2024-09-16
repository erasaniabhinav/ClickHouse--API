# application programming interface — a software intermediary that allows two applications to talk to each other. 
# APIs are an accessible way to extract and share data
#models talk to datbase and controllers control the logic also views build the webapp 
#everything stays in one growing large codebase
#duplication can be eliminated in api and routes will interact by making calls
#del route, update, log in , logout call and make calls to API
#if api is in place we can connect anything to it, by interacting back n forth 
#python api is called FAST-API

#importing fastapi
import uuid
from fastapi import FastAPI, HTTPException
#BaseModel is a class provided by pydantic that you can inherit from to define your data 
from pydantic import BaseModel
import clickhouse_connect


app = FastAPI()


host = "https://hott5igq4u.us-east1.gcp.clickhouse.cloud:8443"
username =  'default'
password = 'fEQPaQt8P_.w0'

# client = clickhouse_connect.get_client(host=host, username=username, password=password)
client = clickhouse_connect.get_client(host='hott5igq4u.us-east1.gcp.clickhouse.cloud', secure=True, port=443, user='default', password='fEQPaQt8P_.w0')



# creating a custom class that inherits from BaseModel
class Item(BaseModel):
    name: str
    price: float 


@app.get("/")
def read_root():
    return {"Hello": "ClickHouse"}

@app.get("/items/")
def get_items():
    #executed on the ClickHouse database to select all items from the items table
    result = client.query('SELECT * FROM items')
    #result_rows attribute contains the fetched items as rows
    items = result.result_rows
    # print(items)
    result_items = []
    for item in items: 
        # dictionary containing three keys: "item_id", "name", and "price" for each item
        result_items.append({
            "item_id": item[0],
            "name":item[1],
            "price": item[2]

        })

    return {"items": result_items}


@app.get("/items/{item_id}")
#executed when a GET request is made to the /items/{item_id} 
def read_item(item_id: str):
    
    try:
        item_uuid = uuid.UUID(item_id)
    except ValueError:
        # If item_id is not a valid UUID, raise HTTPException with status code 400 (Bad Request)
        raise HTTPException(status_code=400, detail="Invalid item_id format. Must be a valid UUID.")
    
    #function takes item_id as a parameter
    #select all columns from the items table where item_id matches provided item_id
    result = client.query("SELECT * FROM items WHERE item_id = '"+str(item_uuid)+"'")

    # Check if result is empty (no matching item found)
    if not result.result_rows:
        # Raise HTTPException with status 404 (Not Found) and error message
        # return {"message": "item not found"}
        raise HTTPException(status_code=404, detail="Item not found")

    # first row of the query result 
    item = result.result_rows[0]
    return {"item_id": item_id, "name": item[1], "price": item[2]}

@app.post("/items/create")
#function is executed when a POST request is made /items/create endpoint
#takes a parameter item
def create_item(item: Item):
    #SQL query to insert a new item into the items 
    query = "INSERT INTO items (item_id, item_name, item_price) VALUES (generateUUIDv4(), '"+ item.name+"', "+ str(item.price)+")"
    result = client.query(query)
    # item = result.result_rows[0]
    print(result, item)
    #dictionary containing the name and price
    return {"item_name": item.name, "price":item.price}


@app.put("/items/{item_id}")
#updates the provided item_id with new name and price
def update_item(item_id: str, item: Item):

    try:
        item_uuid = uuid.UUID(item_id)
    except ValueError:
        # If item_id is not a valid UUID, raise HTTPException with status code 400 (Bad Request)
        raise HTTPException(status_code=400, detail="Invalid item_id format. Must be a valid UUID.")

    query = "alter table items update item_name = '"+item.name+"', item_price = '"+str(item.price)+"' where item_id ='"+str(item_uuid)+"'"
    client.query(query)

    return {"item_name": item.name, "price":item.price}
   

@app.delete("/items/{item_id}")
#delete the record that matches provided item_id 
def delete_item(item_id: str):

    try:
        item_uuid = uuid.UUID(item_id)
    except ValueError:
        # If item_id is not a valid UUID, raise HTTPException with status code 400 (Bad Request)
        raise HTTPException(status_code=400, detail="Invalid item_id format. Must be a valid UUID.")
    
    #SQL query to delete a record 
    query = "alter table items delete where item_id ='"+item_uuid+"'"
    client.query(query)
    #return a message after deletion
    return {"message": 'Deleted record'}

