from datetime import datetime
from flask import make_response, abort
from conn import config as cg

def get_timestamp():
    return datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))

# Data to serve with our API
engine = cg.dbconn2.getcon(name='AdventureWorksDW')

def get_customer():
    transaction = cg.dbconn2.runtrans_apioutput(engine,"SELECT TOP 10 * FROM [AdventureWorksDW].[dbo].[DimCustomer]","CustomerKey")
    return transaction

def get_customer_by_lastname(lname):
    transaction = cg.dbconn2.runtrans_apioutput(engine,"SELECT TOP 10 * FROM [AdventureWorksDW].[dbo].[DimCustomer] WHERE [LastName] = '" + lname + "'","LastName")
    if len(transaction) > 0:
        return transaction
    # otherwise, nope, not found
    else:
        abort(
            404, "Person with last name {lname} not found".format(lname=lname)
        )