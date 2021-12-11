from fastapi import FastAPI
app = FastAPI()

@app.get("/")
def hello():
    return {"message":"Hello TutLinks.com"}
