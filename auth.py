from google.oauth2 import id_token
from google.auth.transport import requests
from fastapi import HTTPException, Depends
from fastapi.security import HTTPBearer
from jose import jwt
import os

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
JWT_SECRET = os.getenv("JWT_SECRET", "default_secret")
security = HTTPBearer()


def verify_google_token(token: str):
    try:
        info = id_token.verify_oauth2_token(
            token,
            requests.Request(),
            GOOGLE_CLIENT_ID
        )
        email = info.get("email", "")
        if not email.endswith("@ucr.edu"):
            raise HTTPException(status_code=403, detail="Only ucr.edu allowed")
        return email
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


def create_session_jwt(email: str):
    return jwt.encode({"sub": email}, JWT_SECRET, algorithm="HS256")


def get_current_user(creds=Depends(security)):
    try:
        payload = jwt.decode(
            creds.credentials,
            JWT_SECRET,
            algorithms=["HS256"]
        )
        return payload["sub"]
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid session")
