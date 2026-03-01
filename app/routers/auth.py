import os
import httpx 
from jose import JWTError, jwt
from fastapi import APIRouter, Depends, HTTPException, Security
import requests
from functools import lru_cache
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
AUTH0_AUDIENCE = os.getenv("AUTH0_AUDIENCE")

if not AUTH0_DOMAIN:
    raise RuntimeError("AUTH0_DOMAIN environment variable is not set")
if not AUTH0_AUDIENCE:
    raise RuntimeError("AUTH0_AUDIENCE environment variable is not set")

token_auth_scheme = HTTPBearer()

AUTH0_ISSUER = f"https://{AUTH0_DOMAIN}/"

bearer_scheme = HTTPBearer()

router = APIRouter()

def get_jwks():
    url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
    response = requests.get(url)
    return response.json()

def verify_token(token: HTTPAuthorizationCredentials = Security(token_auth_scheme)):
    try:
        jwks = get_jwks()
        payload = jwt.decode(
            token.credentials,
            jwks,
            algorithms=["RS256"],
            audience=AUTH0_AUDIENCE,
            issuer=f"https://{AUTH0_DOMAIN}/"
        )
        return payload
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")



@router.get("/auth/")
async def get_user(token: dict = Depends(verify_token)):
    return {
        "user_id": token.get("sub"),
        "email": token.get("email"),
    }