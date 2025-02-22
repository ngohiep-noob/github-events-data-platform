from typing import Union
from fastapi import FastAPI, Query
from pydantic import BaseModel, Field
from typing import Annotated, Literal
app = FastAPI()

class DownloadQuery(BaseModel):
    hour: int
    day: int
    month: int
    year: int

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get('/download')
def download(query: Annotated[DownloadQuery, Query()]):
    return query.dict()