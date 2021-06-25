# Vincent VanMongo

## Installation

## Getting started

The following guide will get you up and running as fast as possible. The guide assumes you have already completed [installation](#installation), so check that out if you havn't already

### 1. Set up the lifecycle

___ needs to know when your app is starting up or shutting down. You can learn more about why in [our lifecycle documentation](#todo) but for now it involves calling two different methods.

Note that this will depend on how your project is setup and the framework you are using. The below example is for [FastAPI](https://fastapi.tiangolo.com/advanced/events/?h=start) but [check out our examples for other frameworks](./examples/README.md)

```py
# main.py

from fastapi import FastAPI
from vanmongo import Client

app = FastAPI()

@app.on_event("startup")
async def startup():
    await Client.initialize(
      mongo_url="mongodb://localhost:27017",
      mongo_database="mydb",
    )

@app.on_event("shutdown")
async def shutdown():
    await Client.shutdown()

# ...
```

### 2. Declare your documents

```py
# product.py

from typing import Optional, List
from vanmongo import BaseDocument

class Product(BaseDocument):
    title: str
    description: Optional[str]
    price: int
    tags: List[str] = []
```

### 3. Access your documents

```py
# main.py (cont)
from vanmongo import Client
from .product import Product

# ...

@app.get('/products/{product_id}')
async def get_product_route(product_id: str):
    client = Client()

    products = client.use(Product)

    return await products.find_one_by_id(product_id)

@app.post('/products')
async def post_product_route(product: ProductCreate):
    client = Client()

    products = client.use(Product)

    return await products.create_one(product)
```

## FastAPI

```py
from fastapi import Depends
from vanmongo import Client

# ...

def create_client():
    # Add context here
    context = create_context()
    return Client(context=context)

# ...

@app.get("/products")
async def get_products(client: Client = Depends(create_client)):
    products = client.use(Product)
    return await products.find(limit=50)
```

## Ariadne

```py

from ariadne import QueryType, gql, make_executable_schema
from ariadne.asgi import GraphQL
from vanmongo import Client


async def create_context(request):
    client = Client(context=db_context)

    return MyAriadneContext(
        client=client
    )


query = QueryType()


@query.field("products")
async def resolve_hello(_, info):
    products = info.context.client.use(Product)
    return await products.find_connection(first=50)


schema = make_executable_schema(type_defs, query)
app = GraphQL(schema, debug=True, context_value=create_context)

```
