from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api import routers


app = FastAPI(
    title="Bitcoin Prediction API",
    description="비트코인 가격 예측을 위한 FastAPI 서버",
    version="1.0.0"
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
for router, prefix, tags in routers:
    app.include_router(router)

# uvicorn으로 실행할 때 필요한 부분
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)