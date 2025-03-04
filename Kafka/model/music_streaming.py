from typing import Optional
from pydantic import BaseModel, Field

class EventLog(BaseModel):
    ts: int = Field(..., description="이벤트 타임스탬프")
    userId: Optional[str] = Field(None, description="사용자 ID")
    sessionId: Optional[int] = Field(None, description="세션 ID")
    page: str = Field(..., description="페이지 유형")
    auth: str = Field(..., description="인증 상태")
    method: str = Field(..., description="HTTP 메서드")
    status: int = Field(..., description="HTTP 상태 코드")
    level: Optional[str] = Field(None, description="사용자 레벨 (무료/유료)")
    location: Optional[str] = Field(None, description="사용자 위치")
    artist: Optional[str] = Field(None, description="아티스트")
    song: Optional[str] = Field(None, description="재생된 노래 제목")
    length: Optional[float] = Field(None, description="노래 길이 (초 단위)")

    class Config:
        extra = "ignore"  # 🔹 불필요한 필드는 무시