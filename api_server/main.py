from fastapi import FastAPI, HTTPException
from typing import List
import logging
import os

# 추천 로직 임포트 (파일 구조에 따라 경로 조정 필요)
from core.recommendation_logic import recommendation_engine

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "ok", "message": "Welcome to the API server!"}

@app.get("/recommendations/{user_id}", response_model=List[str])
def get_user_recommendations(user_id: str, num_items: int = 10):
    """
    특정 사용자를 위한 상품을 추천합니다.
    """
    try:
        logger.info(f"'{user_id}'에 대한 추천 요청 (아이템 수: {num_items})")
        recommendations = recommendation_engine.get_recommendations(user_id, num_items)
        if not recommendations:
            # 추천 결과가 없을 경우 404 에러 대신 빈 리스트를 반환하거나,
            # 인기 상품 등 기본 추천을 제공할 수 있습니다.
            logger.warning(f"'{user_id}'에 대한 추천 결과를 생성할 수 없습니다.")
        return recommendations
    except Exception as e:
        logger.error(f"추천 API 처리 중 심각한 오류 발생: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="추천을 생성하는 동안 서버 오류가 발생했습니다.")