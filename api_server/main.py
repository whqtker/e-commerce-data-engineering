from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    """
    서버의 상태를 확인하기 위한 기본적인 헬스 체크 엔드포인트
    """
    return {"status": "ok", "message": "Welcome to the API server!"}

