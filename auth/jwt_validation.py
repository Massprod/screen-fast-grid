import os
import dotenv
from loguru import logger
from typing import Optional
from jose import jwt, JWTError
from starlette.status import HTTP_401_UNAUTHORIZED
from fastapi import Depends, HTTPException, status
from constants import PUBLIC_KEY, ALGORITHM, ISSUER
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


if not dotenv.load_dotenv('.env'):
    logger.warning('Failed to load .env file, using system environment variables.')
valid_token_required: bool = os.getenv('JWT_VALIDATION_TOKEN_REQ', 'false').lower() == 'true'
logger.warning(
    f'Setting for JWT validation required: {valid_token_required}'
)
oauth_security = HTTPBearer(
    auto_error=valid_token_required
)
issuerName: str = ISSUER


async def validate_credentials(
        credentials: HTTPAuthorizationCredentials = Depends(oauth_security)
) -> dict | None:
    if credentials is None and not valid_token_required:
        return None
    token: str = credentials.credentials
    logger.info(
        f'Validating access token: {token}'
    )
    try:
        payload = jwt.decode(token, PUBLIC_KEY, algorithms=[ALGORITHM])
        username = payload.get('sub')
        if username is None:
            logger.warning(
                f'Attempt to use token without correct data in it | Missing `sub`'
            )
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid token: Missing 'sub'"
            )
        user_role = payload.get('userRole')
        if user_role is None:
            logger.warning(
                f'Attempt to use token without correct data in it | Missing `userRole`'
            )
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid token: Missing 'userRole'"
            )
        issuer = payload.get('iss')
        if issuer != issuerName:
            logger.warning(
                f'Attempt to use token without trusted `iss` | Issuer: {issuer}'
            )
            raise HTTPException(
                status_code=HTTP_401_UNAUTHORIZED,
                detail="Invalid token: Incorrect issuer"
            )
        return payload
    except JWTError as error:
        logger.error(
            f'Error while verifying provided token | Error: {error}'
        )
        raise HTTPException(
            status_code=HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )


async def verify_multi_roles_token(
        allowed_roles: set[str],
        credentials: HTTPAuthorizationCredentials = Depends(oauth_security),
) -> dict | None:
    if credentials is None and not valid_token_required:
        return None
    token: str = credentials.credentials
    allowed_roles_log: str = ' | '.join(allowed_roles)
    logger.info(
        f'Verifying access token for roles: {allowed_roles}'
    )
    try:
        payload = await validate_credentials(credentials)
        role: str = payload.get('userRole')
        if role not in allowed_roles:
            logger.warning(
                f'Unauthorized access attempt | Token: {token} | Role: {role} | Required Roles: {allowed_roles_log}'
            )
            raise HTTPException(
                detail='Invalid Token: Unauthorized role',
                status_code=status.HTTP_401_UNAUTHORIZED,
            )
        logger.info(
            f'Successfully verified role: {role} for token'
        )
        return payload
    except JWTError as error:
        logger.error(
            f'Error while verifying provided token | Error: {error}'
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail='Invalid Token'
        )


def get_role_verification_dependency(allowed_roles: set[str]) -> callable:
    async def dependency(credentials: Optional[HTTPAuthorizationCredentials] = Depends(oauth_security)):
        return await verify_multi_roles_token(allowed_roles=allowed_roles, credentials=credentials)
    return Depends(dependency)
