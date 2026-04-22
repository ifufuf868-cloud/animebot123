"""
Cosmos TV Backend API
FastAPI-based REST API with PostgreSQL database
"""
#Shodiyorbek
import os
import logging
import hashlib
import time
import hmac
import jwt
import uuid
import asyncio
import tempfile
import psycopg2
import resend
import urllib.request
import urllib.parse
import json
import shutil
import aiohttp
import random
import string
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional, Dict
from contextlib import contextmanager, asynccontextmanager
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

# Load environment variables from the same directory
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

from fastapi import FastAPI, HTTPException, Depends, status, Query, BackgroundTasks, UploadFile, File, Request, Response, Form
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, EmailStr, Field
from fastapi.responses import StreamingResponse, JSONResponse
from telegram_utils import (
    start_telegram, 
    send_error_to_admin
)
from notification_service import init_firebase, send_push_notification

# Environment variables are loaded above

# TSPay configuration will be handled manually
TSPAY_AVAILABLE = True

# ==================== CONFIGURATION ====================

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("CosmosTV")

# App Configuration
class Config:
    SECRET_KEY = os.getenv("SECRET_KEY", "FlEDAs55B6TNmrLZkzqTzsuv5ClihQP8")
    ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days
    
    # Database (Supabase)
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres.tviifpwygusqtxomdsss")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "shodiyor2009")
    DB_HOST = os.getenv("DB_HOST", "aws-1-ap-south-1.pooler.supabase.com")
    DB_PORT = os.getenv("DB_PORT", "6543")
    
    # TSPay
    TSPAY_EMAIL = os.getenv("TSPAY_EMAIL", "ifufuf868@gmail.com")
    TSPAY_PASSWORD = os.getenv("TSPAY_PASSWORD", "shodiyor2009")
    TSPAY_REDIRECT_URL = os.getenv("TSPAY_REDIRECT_URL", "https://shodiyor.uz/payment/callback")
    TSPAY_SHOP_TOKEN = os.getenv("TSPAY_SHOP_TOKEN", "JQwLltdwe9m2YnYoyOV4FxB2hImjXzVXUZtMX1p6ws")
    TSPAY_SECRET_KEY = os.getenv("TSPAY_SECRET_KEY", "")

    # Resend API
    RESEND_API_KEY = "re_TNzeQXRg_DLQ2pbjgZiZNpVJTT3MiDTM7"

    # Admin
    SUPER_ADMIN_ID = "00000000-0000-0000-0000-076080532000"
    
    # DoodStream Video Hosting
    DOODSTREAM_API_KEY = os.getenv("DOODSTREAM_API_KEY", "564291lt0fwxhjndwnqysv")
    DOODSTREAM_API_BASE = "https://doodapi.com/api"

    # Supabase Storage — S3 uyg'un kalitlar (Rasm/Avatar hosting)
    SUPABASE_URL           = os.getenv("SUPABASE_URL", "https://tviifpwygusqtxomdsss.supabase.co")
    SUPABASE_S3_ENDPOINT   = os.getenv("SUPABASE_S3_ENDPOINT", "https://tviifpwygusqtxomdsss.storage.supabase.co/storage/v1/s3")
    SUPABASE_S3_REGION     = os.getenv("SUPABASE_S3_REGION", "ap-south-1")
    SUPABASE_S3_ACCESS_KEY = os.getenv("SUPABASE_S3_ACCESS_KEY", "")
    SUPABASE_S3_SECRET_KEY = os.getenv("SUPABASE_S3_SECRET_KEY", "")
    SUPABASE_STORAGE_BUCKET = os.getenv("SUPABASE_STORAGE_BUCKET", "media")

    @classmethod
    def validate_storage(cls):
        """DoodStream va Supabase Storage konfiguratsiyasini tekshirish"""
        # DoodStream
        if not cls.DOODSTREAM_API_KEY:
            logger.error("🛑 DoodStream API Key MISSING! Check .env")
        else:
            masked = cls.DOODSTREAM_API_KEY[:6] + "..."
            logger.info(f"🎬 DoodStream: Key={masked}")
        # Supabase S3
        if not cls.SUPABASE_S3_ACCESS_KEY:
            logger.warning("⚠️  Supabase S3 kaliti topilmadi! Rasmlar local saqlanadi.")
        else:
            logger.info(f"🖼️  Supabase S3: {cls.SUPABASE_S3_ENDPOINT} | Bucket={cls.SUPABASE_STORAGE_BUCKET}")

# In-memory cache for streaming links
# Format: {token: {"message_id": int, "expires": datetime}}
STREAM_CACHE = {}

# In-memory cache for API responses (Performance Optimization)
API_CACHE = {}
API_CACHE_TTL = {}

# Cleaning task for caches
async def clean_caches():
    while True:
        now = datetime.now()
        current_time = time.time()
        
        # Clean stream cache
        expired_streams = [token for token, data in STREAM_CACHE.items() if data["expires"] < now]
        for token in expired_streams:
            del STREAM_CACHE[token]
        
        # Clean API cache
        expired_api = [key for key, ttl in API_CACHE_TTL.items() if ttl < current_time]
        for key in expired_api:
            if key in API_CACHE:
                del API_CACHE[key]
            del API_CACHE_TTL[key]
        
        logger.info(f"🧹 Cache cleaned: {len(expired_streams)} streams, {len(expired_api)} API responses")
        await asyncio.sleep(60)

async def supabase_keep_alive_task():
    """
    Supabase loyihasini pauza bo'lishidan himoya qilish.
    Supabase Free Tier 1 hafta faoliyatsiz qolganda pauza qiladi.
    Har 5 kunda bir marta DB ga oddiy SELECT so'rovi yuboramiz.
    Bu Supabase ni 'faol loyiha' deb belgilaydi.
    """
    # Birinchi ishga tushishda 5 daqiqa kutish
    await asyncio.sleep(300)

    while True:
        try:
            def _ping_db():
                with get_db() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()

            await asyncio.to_thread(_ping_db)
            logger.info("📊 Supabase keep-alive ping yuborildi ✅")
        except Exception as e:
            logger.warning(f"⚠️ Supabase keep-alive xatosi: {e}")

        # Har 5 kunda bir ping (5 * 24 * 3600 = 432000 sekund)
        await asyncio.sleep(5 * 24 * 3600)


# FastAPI App
app = FastAPI(
    title="Cosmos TV API",
    description="Professional Anime Streaming Platform Backend",
    version="2.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

@app.on_event("startup")
async def startup_event():
    init_pool()
    init_db()
    await start_telegram()
    init_firebase() # Initialize Firebase Admin
    asyncio.create_task(clean_caches())
    asyncio.create_task(doodstream_keep_alive_task())  # 🔑 DoodStream auto-delete oldini olish
    asyncio.create_task(supabase_keep_alive_task())    # 📊 Supabase pauza oldini olish
    Config.validate_storage()
    logger.info("✅ Backend started: DoodStream + Supabase keep-alive active")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("👋 Backend shutting down")

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False, # Wildcard origins '*' cannot have credentials=True
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

# GZIP Compression Middleware (Performance Optimization)
from fastapi.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Request Logging Middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests and responses for debugging"""
    import time
    start_time = time.time()
    
    # Log request
    logger.info(f"📥 {request.method} {request.url.path} from {request.client.host if request.client else 'unknown'}")
    
    # Process request
    try:
        response = await call_next(request)
        process_time = time.time() - start_time
        
        # Log response
        if response.status_code >= 400:
            if response.status_code == 404:
                logger.warning(f"⚠️  404 NOT FOUND: {request.method} {request.url.path} ({process_time:.2f}s)")
            else:
                logger.error(f"❌ {response.status_code}: {request.method} {request.url.path} ({process_time:.2f}s)")
        else:
            logger.info(f"✅ {response.status_code}: {request.method} {request.url.path} ({process_time:.2f}s)")
        
        return response
    except Exception as e:
        process_time = time.time() - start_time
        logger.error(f"💥 EXCEPTION in {request.method} {request.url.path}: {e} ({process_time:.2f}s)")
        raise

# Exception Handler to report errors to Admin via Telegram
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    # Support for ExceptionGroup in Python 3.11+
    if hasattr(__builtins__, 'ExceptionGroup') and isinstance(exc, ExceptionGroup):
        error_msg = f"🔥 **ExceptionGroup** ({len(exc.exceptions)} errors)\n"
        for sub_exc in exc.exceptions:
            logger.error(f"💥 Nested Exception: {type(sub_exc).__name__}: {sub_exc}")
            error_msg += f"• `{type(sub_exc).__name__}: {str(sub_exc)[:100]}`\n"
        error_msg += f"Path: `{request.url.path}`\nMethod: `{request.method}`"
    else:
        error_msg = f"🔥 **Unhandled Exception**\nType: `{type(exc).__name__}`\nError: `{str(exc)}`\nPath: `{request.url.path}`\nMethod: `{request.method}`"
    
    # Try to get user info if possible (safe way)
    try:
        auth_header = request.headers.get("Authorization")
        if auth_header and auth_header.startswith("Bearer "):
            token = auth_header.split(" ")[1]
            payload = verify_token(token)
            error_msg += f"\nUser ID: `{payload.get('sub')}`"
    except: pass

    logger.error(error_msg)
    # Only send non-streaming related errors to admin to reduce noise
    if "/stream" not in request.url.path:
        await send_error_to_admin(error_msg)
        
    return JSONResponse(status_code=500, content={"ok": False, "detail": "Internal Server Error"})

@app.post("/admin/report-frontend-error")
async def report_frontend_error(data: dict):
    """Endpoint for frontend to report errors to admin"""
    try:
        error = data.get("error", "Unknown error")
        stack = data.get("stack", "")
        device = data.get("device", "Unknown device")
        
        msg = f"📱 **FRONTEND ERROR**\n\n**Error:** `{error}`\n\n**Device:** `{device}`"
        if stack:
            msg += f"\n\n**Stack:**\n`{stack[:500]}...`" # Limit stack size
            
        await send_error_to_admin(msg)
        return {"ok": True}
    except Exception as e:
        logger.error(f"Failed to report frontend error: {e}")
        return {"ok": False, "detail": str(e)}

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if exc.status_code == 500:
        error_msg = f"💥 **Backend Error (500)**\nDetail: `{exc.detail}`\nPath: `{request.url.path}`"
        logger.error(error_msg)
        await send_error_to_admin(error_msg)
    elif exc.status_code == 404:
        # Enhanced 404 response with debugging info
        logger.warning(f"🔍 404 Details: {request.method} {request.url.path} - {exc.detail}")
        return JSONResponse(
            status_code=404, 
            content={
                "ok": False, 
                "detail": exc.detail,
                "path": str(request.url.path),
                "method": request.method,
                "message_uz": "Endpoint topilmadi. Iltimos, URL ni tekshiring."
            }
        )
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})

# Static Files (Uploads)
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

security = HTTPBearer()

# TSPay Client Initialization
# We will use direct API calls instead of the library
tspay_client = None

# Resend Configuration
resend.api_key = Config.RESEND_API_KEY

# Database Connection Pool
db_pool = None

def init_pool():
    global db_pool
    try:
        db_pool = SimpleConnectionPool(
            1, 20, # Min and Max connections
            dbname=Config.DB_NAME,
            user=Config.DB_USER,
            password=Config.DB_PASSWORD,
            host=Config.DB_HOST,
            port=Config.DB_PORT,
            cursor_factory=RealDictCursor
        )
        logger.info("✅ Database connection pool initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize database pool: {e}")

# In-memory OTP storage
otp_storage = {}

def send_sms(phone: str, message: str) -> bool:
    """
    Send SMS using a gateway (Placeholder for Eskiz.uz or similar)
    For now, we log it.
    """
    logger.info(f"📱 SMS sent to {phone}: {message}")
    # Implementation for Eskiz.uz would go here:
    # payload = {'mobile_phone': phone, 'message': message, 'from': '4546'}
    # response = requests.post("https://notify.eskiz.uz/api/message/sms/send", ...)
    return True

def send_email(to_email: str, subject: str, html_content: str) -> bool:
    """Send email using Resend API"""
    try:
        r = resend.Emails.send({
            "from": "onboarding@resend.dev",
            "to": to_email,
            "subject": subject,
            "html": html_content
        })
        logger.info(f"✅ Email sent successfully to {to_email}. ID: {r.get('id')}")
        return True
    except Exception as e:
        logger.error(f"❌ Email send error: {e}")
        return False

# ==================== DATABASE ====================

@contextmanager
def get_db():
    """PostgreSQL connection with atomic transaction management"""
    conn = None
    try:
        if not db_pool:
            init_pool()
        conn = db_pool.getconn()
        # Ensure clean state
        conn.autocommit = False
        yield conn
        # If we reach here, no exception occurred
        conn.commit()
    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
        logger.error(f"📡 Database connection error: {e}")
        if conn:
            try: conn.rollback()
            except: pass
        raise HTTPException(status_code=503, detail="Ma'lumotlar bazasi bilan aloqa uzildi")
    except psycopg2.Error as e:
        logger.error(f"🗄️ Database logic error: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=400, detail=f"Ma'lumotlar saqlashda xatolik: {e.diag.message_primary if hasattr(e, 'diag') else str(e)}")
    except HTTPException:
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"🔥 Critical Database Error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Serverda ichki xatolik yuz berdi")
    finally:
        if conn and db_pool:
            db_pool.putconn(conn)

def init_db():
    """Initialize database schema with all required tables and default settings"""
    with get_db() as conn:
        with conn.cursor() as cursor:
            # 0. GLOBAL TYPE UNIFICATION (CRITICAL)
            # This block ensures all user-related IDs are TEXT to avoid mismatch with UUID
            try:
                # Dynamic discovery and removal of any FKs referencing users(id)
                cursor.execute("""
                DO $$ 
                DECLARE 
                    r RECORD;
                BEGIN
                    FOR r IN (
                        SELECT constraint_name, table_name 
                        FROM information_schema.key_column_usage 
                        WHERE referenced_table_name = 'users' AND referenced_column_name = 'id'
                    ) 
                    LOOP
                        EXECUTE 'ALTER TABLE ' || quote_ident(r.table_name) || ' DROP CONSTRAINT ' || quote_ident(r.constraint_name);
                    END LOOP;
                END $$;
                """)
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.debug(f"FK discovery/drop skipped or failed: {e}")

            unification_queries = [
                "ALTER TABLE users ALTER COLUMN id TYPE TEXT",
                "ALTER TABLE notifications ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE payments ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE purchases ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE shorts_likes ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE shorts_comments ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE ads ALTER COLUMN user_id TYPE TEXT"
            ]
            
            for query in unification_queries:
                try:
                    cursor.execute(query)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.debug(f"Unification step skipped: {query} -> {e}")

            # 1. Table definitions
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    short_id TEXT UNIQUE,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password TEXT NOT NULL,
                    balance DOUBLE PRECISION DEFAULT 0,
                    is_premium_user BOOLEAN DEFAULT FALSE,
                    premium_expiry TIMESTAMP,
                    avatar_url TEXT,
                    plain_password TEXT,
                    created_at TIMESTAMP NOT NULL
                );
            """)

            # Schema updates for existing tables
            updates = [
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS short_id TEXT UNIQUE",
                "ALTER TABLE users ADD COLUMN IF NOT EXISTS plain_password TEXT",
                "ALTER TABLE news ADD COLUMN IF NOT EXISTS news_type TEXT DEFAULT 'ordinary'",
                "ALTER TABLE news ADD COLUMN IF NOT EXISTS target_shorts_id TEXT",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS content_type TEXT",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
                "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS content_type TEXT"
            ]
            
            for update in updates:
                try:
                    cursor.execute(update)
                    conn.commit() # Commit each update to avoid total failure
                except Exception as e:
                    logger.warning(f"⚠️ Migration note: {update} -> {e}")
                    conn.rollback()

            # Migrate existing users to have short_id
            cursor.execute("SELECT id FROM users WHERE short_id IS NULL")
            users_without_short_id = cursor.fetchall()
            for u in users_without_short_id:
                new_short = generate_short_id()
                try:
                    cursor.execute("UPDATE users SET short_id = %s WHERE id = %s", (new_short, u['id']))
                except:
                    # Retry once if collision (rare)
                    cursor.execute("UPDATE users SET short_id = %s WHERE id = %s", (generate_short_id(), u['id']))
            if users_without_short_id:
                conn.commit()
                logger.info(f"Generated short_id for {len(users_without_short_id)} existing users.")

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS admins (
                    id TEXT PRIMARY KEY,
                    dubbing_name TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    added_at TIMESTAMP NOT NULL,
                    role TEXT DEFAULT 'admin'
                );
                CREATE TABLE IF NOT EXISTS animes (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    genre TEXT NOT NULL,
                    description TEXT NOT NULL,
                    price DOUBLE PRECISION NOT NULL,
                    poster_url TEXT NOT NULL,
                    banner_url TEXT,
                    added_by TEXT NOT NULL,
                    dubbing_name TEXT NOT NULL,
                    status TEXT DEFAULT 'ongoing',
                    is_premium BOOLEAN DEFAULT FALSE,
                    views INTEGER DEFAULT 0,
                    purchases INTEGER DEFAULT 0,
                    revenue DOUBLE PRECISION DEFAULT 0,
                    created_at TIMESTAMP NOT NULL
                );
                CREATE TABLE IF NOT EXISTS episodes (
                    id TEXT PRIMARY KEY,
                    anime_id TEXT NOT NULL REFERENCES animes(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    video_url TEXT NOT NULL,
                    episode_number INTEGER NOT NULL,
                    views INTEGER DEFAULT 0,
                    added_at TIMESTAMP NOT NULL,
                    tg_message_id INTEGER,
                    tg_file_id TEXT,
                    content_type TEXT
                );
                CREATE TABLE IF NOT EXISTS purchases (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    anime_id TEXT NOT NULL REFERENCES animes(id) ON DELETE CASCADE,
                    price DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMP NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notifications (
                    id TEXT PRIMARY KEY,
                    user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
                    title TEXT NOT NULL,
                    message TEXT NOT NULL,
                    type TEXT DEFAULT 'system',
                    target_id TEXT,
                    created_at TIMESTAMP NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    description TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS banners (
                    id TEXT PRIMARY KEY,
                    text TEXT NOT NULL,
                    image_url TEXT NOT NULL,
                    added_by TEXT NOT NULL,
                    created_at TIMESTAMP NOT NULL,
                    active BOOLEAN DEFAULT TRUE
                );
                CREATE TABLE IF NOT EXISTS ads (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    image_url TEXT NOT NULL,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    views INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    created_at TIMESTAMP NOT NULL,
                    active BOOLEAN DEFAULT TRUE
                );
                CREATE TABLE IF NOT EXISTS payments (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    amount DOUBLE PRECISION NOT NULL,
                    status TEXT DEFAULT 'pending',
                    transaction_id TEXT,
                    cheque_id TEXT,
                    payment_url TEXT,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP,
                    metadata JSONB
                );
                CREATE TABLE IF NOT EXISTS shorts (
                    id TEXT PRIMARY KEY,
                    video_url TEXT NOT NULL,
                    description TEXT,
                    anime_id TEXT REFERENCES animes(id) ON DELETE SET NULL,
                    added_by TEXT NOT NULL,
                    views INTEGER DEFAULT 0,
                    created_at TIMESTAMP NOT NULL,
                    tg_message_id INTEGER,
                    tg_file_id TEXT,
                    content_type TEXT
                );
                CREATE TABLE IF NOT EXISTS app_ads (
                    id TEXT PRIMARY KEY,
                    anime_id TEXT REFERENCES animes(id) ON DELETE SET NULL,
                    target_shorts_id TEXT REFERENCES shorts(id) ON DELETE SET NULL,
                    image_url TEXT,
                    title TEXT,
                    description TEXT,
                    active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS news (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    image_url TEXT,
                    target_anime_id TEXT REFERENCES animes(id) ON DELETE SET NULL,
                    news_type TEXT DEFAULT 'ordinary',
                    target_shorts_id TEXT REFERENCES shorts(id) ON DELETE SET NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS shorts_likes (
                    short_id TEXT NOT NULL REFERENCES shorts(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (short_id, user_id)
                );
                CREATE TABLE IF NOT EXISTS shorts_comments (
                    id TEXT PRIMARY KEY,
                    short_id TEXT NOT NULL REFERENCES shorts(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    text TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS promo_codes (
                    id TEXT PRIMARY KEY,
                    code TEXT UNIQUE NOT NULL,
                    reward_type TEXT NOT NULL, -- 'cash' or 'anime_access'
                    reward_value TEXT NOT NULL, -- amount or anime_id
                    expiry_date TIMESTAMP,
                    usage_limit INTEGER DEFAULT 0, -- 0 for unlimited
                    used_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    active BOOLEAN DEFAULT TRUE
                );
                CREATE TABLE IF NOT EXISTS promo_usages (
                    id TEXT PRIMARY KEY,
                    promo_id TEXT NOT NULL REFERENCES promo_codes(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS support_faqs (
                    id SERIAL PRIMARY KEY,
                    question TEXT NOT NULL,
                    answer TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS admin_topups (
                    id TEXT PRIMARY KEY,
                    admin_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    amount DOUBLE PRECISION NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create video_ads separately to be safer
            try:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS video_ads (
                        id TEXT PRIMARY KEY,
                        video_url TEXT NOT NULL,
                        title TEXT,
                        views INTEGER DEFAULT 0,
                        active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        tg_message_id INTEGER,
                        tg_file_id TEXT,
                        content_type TEXT
                    );
                """)
                logger.info("✅ Table video_ads ensured")
            except Exception as ad_err:
                conn.rollback()
                logger.warning(f"⚠️ Could not create video_ads table: {ad_err}")

            # Ensure columns exist (Migrations)
            migrations = [
                "ALTER TABLE purchases ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE ads ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE payments ALTER COLUMN user_id TYPE TEXT",
                "ALTER TABLE app_ads ALTER COLUMN anime_id DROP NOT NULL",
                "ALTER TABLE app_ads ADD COLUMN IF NOT EXISTS target_shorts_id TEXT",
                "ALTER TABLE news ADD COLUMN IF NOT EXISTS news_type TEXT DEFAULT 'ordinary'",
                "ALTER TABLE news ADD COLUMN IF NOT EXISTS target_shorts_id TEXT",
                "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS content_type TEXT",
                "ALTER TABLE shorts ADD COLUMN IF NOT EXISTS content_type TEXT",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS content_type TEXT",
                "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
                "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
                "ALTER TABLE shorts ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
                "ALTER TABLE shorts ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
                "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_file_id TEXT"
            ]
            for migration in migrations:
                try:
                    cursor.execute(migration)
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    logger.debug(f"Migration skipped/not needed: {migration}")

            # Initialize Default Settings
            default_settings = {
                "test_mode": "false",
                "tspay_shop_token": Config.TSPAY_SHOP_TOKEN,
                "tspay_secret_key": Config.TSPAY_SECRET_KEY,
                "admob_id": "ca-app-pub-3940256099942544~3347511713",
                "ad_test_mode": "true",
                "seasonal_theme": "none",
                "maintenance_mode": "false"
            }
            for key, value in default_settings.items():
                cursor.execute("INSERT INTO settings (key, value) VALUES (%s, %s) ON CONFLICT DO NOTHING", (key, value))

            # Ensure admin@cosmostv.com has SuperID
            cursor.execute("UPDATE users SET id = %s, short_id = %s WHERE email = %s", (Config.SUPER_ADMIN_ID, '00000001', 'admin@cosmostv.com'))
            
            # Ensure ifufuf868@gmail.com has fixed short_id
            cursor.execute("UPDATE users SET short_id = %s WHERE email = %s", ('88888888', 'ifufuf868@gmail.com'))

            logger.info("✅ Database initialized and roles updated.")
            
            logger.info("✅ Database initialized successfully.")


# ==================== HELPERS ====================

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

def generate_id() -> str:
    return str(uuid.uuid4())

def generate_short_id() -> str:
    """Generate a random 8-digit ID"""
    return ''.join(random.choices(string.digits, k=8))

def create_access_token(subject: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=Config.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode = {"sub": subject, "exp": expire}
    return jwt.encode(to_encode, Config.SECRET_KEY, algorithm=Config.ALGORITHM)

def verify_token(token: str) -> dict:
    try:
        return jwt.decode(token, Config.SECRET_KEY, algorithms=[Config.ALGORITHM])
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    payload = verify_token(token)
    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token payload")

    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT id, short_id, name, email, balance, is_premium_user, premium_expiry, avatar_url, created_at FROM users WHERE id = %s", (user_id,))
            user = cursor.fetchone()
            if not user:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
            return user

def is_admin(user_id) -> bool:
    """Check if user has admin or super_admin role"""
    uid_str = str(user_id)
    # Safety: always allow hardcoded super admin
    if uid_str == Config.SUPER_ADMIN_ID:
        return True
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT role FROM admins WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            if result:
                return True
            
            # Detailed log for 403 investigation
            logger.warning(f"⚠️ Admin check failed for {uid_str}. Type: {type(user_id)}. SuperID: {Config.SUPER_ADMIN_ID}")
            return False

def is_super_admin(user_id) -> bool:
    """Check if user has super_admin role"""
    # Safety: always allow hardcoded super admin
    if str(user_id) == Config.SUPER_ADMIN_ID:
        return True

    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT role FROM admins WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            return result and result["role"] == "super_admin"

def is_finance_admin(user_id) -> bool:
    """Check if user has finance_admin or super_admin role"""
    uid_str = str(user_id)
    if uid_str == Config.SUPER_ADMIN_ID:
        return True
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT role FROM admins WHERE id = %s", (user_id,))
            result = cursor.fetchone()
            # If result is RealDictCursor, result['role']
            # If not, it depends. But RealDictCursor is used here.
            return result and result["role"] in ["super_admin", "finance_admin"]

def create_notification(user_id: Optional[str], title: str, message: str, type: str = "system", target_id: str = None):
    """Create a notification for a specific user or all users (if user_id is None)"""
    with get_db() as conn:
        with conn.cursor() as cursor:
            nid = generate_id()
            now = datetime.now()
            if user_id:
                # To specific user
                cursor.execute("""
                    INSERT INTO notifications (id, user_id, title, message, type, target_id, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (nid, user_id, title, message, type, target_id, now))
            else:
                # To all users (Broadcast)
                # Note: For efficiency in real broadcast, we might use a separate approach, 
                # but for this scale, inserting for each active user or using a NULL user_id for global is okay.
                # Let's use NULL user_id for global notifications.
                cursor.execute("""
                    INSERT INTO notifications (id, user_id, title, message, type, target_id, created_at)
                    VALUES (%s, NULL, %s, %s, %s, %s, %s)
                """, (nid, title, message, type, target_id, now))
            logger.info(f"🔔 Notification created: {title} (Target: {user_id or 'Global'})")

def get_setting(key: str, default: str = None) -> str:
    """Fetch a configuration setting from the database"""
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT value FROM settings WHERE key = %s", (key,))
            row = cursor.fetchone()
            return row["value"] if row else default


# ==================== MODELS ====================

class UserRegister(BaseModel):
    name: str = Field(..., min_length=2, max_length=100)
    email: EmailStr
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class VerifyOtpRequest(BaseModel):
    email: str
    code: str

class AnimeCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    genre: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    price: float = Field(..., ge=0)
    poster_url: str = Field(..., min_length=5)
    banner_url: Optional[str] = None
    dubbing_name: Optional[str] = None
    status: Optional[str] = "ongoing"
    is_premium: Optional[bool] = False
    added_by: str
    episodes: Optional[List[dict]] = []

class EpisodeCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    videoUrl: str = Field(..., min_length=5)
    addedBy: str
    tg_message_id: Optional[int] = None
    tg_file_id: Optional[str] = None
    content_type: Optional[str] = None

class PaymentCreate(BaseModel):
    user_id: str
    amount: float = Field(..., gt=0)
    description: Optional[str] = "Balance Top-up"

class PaymentCheckRequest(BaseModel):
    paymentId: str

class AdminCreate(BaseModel):
    userId: str
    dubbingName: str
    addedBy: str

class BannerCreate(BaseModel):
    text: str
    imageUrl: str
    addedBy: str

class FAQCreate(BaseModel):
    question: str
    answer: str

class FAQUpdate(BaseModel):
    question: Optional[str] = None
    answer: Optional[str] = None
    
class AdCreate(BaseModel):
    title: str
    imageUrl: str
    userId: str

class ShortCreate(BaseModel):
    videoUrl: str = Field(..., min_length=5)
    description: Optional[str] = Field(None, max_length=1000)
    animeId: Optional[str] = None
    addedBy: str
    tg_message_id: Optional[int] = None
    tg_file_id: Optional[str] = None
    content_type: Optional[str] = None

class PurchaseRequest(BaseModel):
    userId: str

class AppAdCreate(BaseModel):
    anime_id: Optional[str] = None
    target_shorts_id: Optional[str] = None
    image_url: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None

class VideoAdCreate(BaseModel):
    video_url: str
    title: Optional[str] = None
    tg_message_id: Optional[int] = None
    tg_file_id: Optional[str] = None
    content_type: Optional[str] = None

class NewsCreate(BaseModel):
    title: str = Field(..., min_length=1, max_length=255)
    description: str = Field(..., min_length=1)
    image_url: str = Field(..., min_length=5)
    target_anime_id: Optional[str] = None
    news_type: Optional[str] = "ordinary"
    target_shorts_id: Optional[str] = None

class CommentCreate(BaseModel):
    text: str = Field(..., min_length=1, max_length=1000)

# ==================== ROUTES ====================

# ==================== DOODSTREAM VIDEO HOSTING ====================


async def upload_to_supabase(body, filename: str, content_type: str) -> Optional[str]:
    """
    Rasm/Avatar fayllarni Supabase S3-compatible Storage ga yuklash.
    boto3 ishlatiladi (S3 signature v4).
    Public URL qaytaradi: https://{project}.supabase.co/storage/v1/object/public/{bucket}/{filename}
    Agar S3 kalitlari yo'q bo'lsa — local fallback.
    """
    import mimetypes
    import io

    # Fayl ma'lumotlarini o'qish
    data = body
    if hasattr(body, 'read'):
        if asyncio.iscoroutinefunction(body.read):
            data = await body.read()
        else:
            if hasattr(body, 'seek'):
                await asyncio.to_thread(body.seek, 0)
            data = await asyncio.to_thread(body.read)
    if not isinstance(data, bytes):
        data = bytes(data)

    mime_type, _ = mimetypes.guess_type(filename)
    if not mime_type:
        mime_type = content_type or "image/jpeg"

    # S3 kalitlari yo'q bo'lsa — local fallback
    if not Config.SUPABASE_S3_ACCESS_KEY or not Config.SUPABASE_S3_SECRET_KEY:
        logger.warning("⚠️ Supabase S3 kaliti yo'q — local storage ga saqlanmoqda")
        save_path = UPLOAD_DIR / filename
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, 'wb') as f:
            f.write(data)
        backend_base = os.getenv("RENDER_EXTERNAL_URL", "https://backend123-rk6r.onrender.com").rstrip('/')
        return f"{backend_base}/uploads/{filename}"

    def _s3_upload():
        """boto3 sync funksiyasi — thread pool da ishlaydi"""
        import boto3
        from botocore.config import Config as BotoConfig

        s3 = boto3.client(
            "s3",
            endpoint_url=Config.SUPABASE_S3_ENDPOINT,
            aws_access_key_id=Config.SUPABASE_S3_ACCESS_KEY,
            aws_secret_access_key=Config.SUPABASE_S3_SECRET_KEY,
            region_name=Config.SUPABASE_S3_REGION,
            config=BotoConfig(signature_version="s3v4"),
        )
        s3.put_object(
            Bucket=Config.SUPABASE_STORAGE_BUCKET,
            Key=filename,
            Body=data,
            ContentType=mime_type,
            # Public read — bucket allaqachon public bo'lishi kerak
        )
        # Supabase public URL formati
        public_url = (
            f"{Config.SUPABASE_URL}/storage/v1/object/public"
            f"/{Config.SUPABASE_STORAGE_BUCKET}/{filename}"
        )
        return public_url

    try:
        public_url = await asyncio.to_thread(_s3_upload)
        logger.info(f"✅ Supabase S3 ga yuklandi: {public_url}")
        return public_url
    except Exception as e:
        logger.error(f"💥 Supabase S3 upload xatosi: {e}")
        # Xato bo'lsa — local fallback
        try:
            save_path = UPLOAD_DIR / filename
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, 'wb') as f:
                f.write(data)
            backend_base = os.getenv("RENDER_EXTERNAL_URL", "https://backend123-rk6r.onrender.com").rstrip('/')
            logger.warning(f"⚠️ Local fallback: {filename}")
            return f"{backend_base}/uploads/{filename}"
        except Exception as e2:
            logger.error(f"💥 Local fallback ham xato: {e2}")
            return None



async def upload_video_to_doodstream(stream, filename: str, content_type: str) -> Optional[str]:
    """
    DoodStream ga video yuklash — 2 bosqich:
    1. Upload server URL olish
    2. Fayl yuborish → file_code qaytarish
    """
    api_key = Config.DOODSTREAM_API_KEY
    try:
        async with aiohttp.ClientSession() as session:
            # --- Bosqich 1: Upload server URL olish ---
            server_resp = await session.get(
                f"{Config.DOODSTREAM_API_BASE}/upload/server",
                params={"key": api_key}
            )
            if server_resp.status != 200:
                txt = await server_resp.text()
                logger.error(f"❌ DoodStream server URL xatosi: {server_resp.status} - {txt}")
                return None

            server_data = await server_resp.json()
            if server_data.get("status") != 200:
                logger.error(f"❌ DoodStream server URL: {server_data}")
                return None

            upload_server = server_data["result"]
            logger.info(f"📤 DoodStream upload server: {upload_server}")

            # --- Bosqich 2: Video faylni o'qish ---
            video_bytes = b""
            if isinstance(stream, bytes):
                video_bytes = stream
            elif hasattr(stream, 'read'):
                if asyncio.iscoroutinefunction(stream.read):
                    video_bytes = await stream.read()
                else:
                    video_bytes = await asyncio.to_thread(stream.read)
            else:
                # async generator (streaming upload)
                chunks = []
                async for chunk in stream:
                    chunks.append(chunk)
                video_bytes = b"".join(chunks)

            # --- Bosqich 3: Yuborish ---
            form_data = aiohttp.FormData()
            form_data.add_field("api_key", api_key)
            form_data.add_field("file_public", "1")
            form_data.add_field(
                "file",
                video_bytes,
                filename=filename,
                content_type=content_type or "video/mp4"
            )

            upload_resp = await session.post(upload_server, data=form_data)
            if upload_resp.status not in (200, 201):
                txt = await upload_resp.text()
                logger.error(f"❌ DoodStream upload xatosi: {upload_resp.status} - {txt}")
                return None

            result = await upload_resp.json()
            logger.info(f"📦 DoodStream javob: {result}")

            # Javobdan file_code olamiz
            if isinstance(result, list) and len(result) > 0:
                file_code = result[0].get("file_code") or result[0].get("filecode")
            elif isinstance(result, dict):
                file_code = result.get("file_code") or result.get("filecode")
            else:
                file_code = None

            if not file_code:
                logger.error(f"❌ DoodStream file_code topilmadi: {result}")
                return None

            logger.info(f"✅ DoodStream video yuklandi. file_code: {file_code}")
            return file_code

    except Exception as e:
        logger.error(f"💥 DoodStream upload istisno: {e}")
        return None


def build_doodstream_embed_url(file_code: str) -> str:
    """DoodStream embed URL yasash"""
    return f"https://dood.watch/e/{file_code}"


def is_doodstream_code(value: str) -> bool:
    """DoodStream file_code ekanligini tekshirish (qisqa alfanumerik string)"""
    v = value.strip().strip('/')
    return bool(v) and len(v) <= 20 and not v.startswith('{') and '/' not in v and '.' not in v


# ==================== AUTO DELETE PREVENTION (HACKERONA YECHIM) ====================
# DoodStream 60 kun ko'rilmagan videolarni o'chiradi.
# Bu task har 45 kunda barcha video file_code larni DoodStream API orqali "ping" qiladi.
# Bu videoni "so'nggi ko'rilgan" vaqtini yangilab, o'chirishni oldini oladi.

async def doodstream_keep_alive_task():
    """Background task: 45 kunda bir marta barcha DoodStream videolarni ping qilish"""
    # Birinchi ishga tushishda 10 daqiqa kutish (backend to'liq yuklansin)
    await asyncio.sleep(600)

    while True:
        try:
            logger.info("🔄 DoodStream keep-alive boshlanmoqda...")
            file_codes = []

            # Database dan barcha video URL larni yig'amiz
            with get_db() as conn:
                with conn.cursor() as cursor:
                    # Episodes dan
                    cursor.execute("SELECT video_url FROM episodes WHERE video_url IS NOT NULL")
                    file_codes += [r['video_url'] for r in cursor.fetchall()]
                    # Shorts dan
                    cursor.execute("SELECT video_url FROM shorts WHERE video_url IS NOT NULL")
                    file_codes += [r['video_url'] for r in cursor.fetchall()]
                    # Video Ads dan
                    cursor.execute("SELECT video_url FROM video_ads WHERE video_url IS NOT NULL")
                    file_codes += [r['video_url'] for r in cursor.fetchall()]

            # Faqat DoodStream file_code larni filtrlash
            ds_codes = list(set([c for c in file_codes if is_doodstream_code(c)]))
            logger.info(f"📋 Keep-alive: {len(ds_codes)} ta DoodStream video topildi")

            pinged = 0
            failed = 0
            async with aiohttp.ClientSession() as session:
                for code in ds_codes:
                    try:
                        # DoodStream file/info API ni chaqirish = "so'nggi ko'rilgan" yangilanadi
                        resp = await session.get(
                            f"{Config.DOODSTREAM_API_BASE}/file/info",
                            params={"key": Config.DOODSTREAM_API_KEY, "file_code": code},
                            timeout=aiohttp.ClientTimeout(total=15)
                        )
                        data = await resp.json()
                        if data.get("status") == 200:
                            pinged += 1
                        else:
                            failed += 1
                            logger.warning(f"⚠️ Keep-alive {code}: {data.get('msg', 'xato')}")

                        # API limitga tushmaslik uchun har so'rov orasida kichik pauza
                        await asyncio.sleep(2)

                    except Exception as e:
                        failed += 1
                        logger.warning(f"⚠️ Keep-alive ping xatosi [{code}]: {e}")

            logger.info(f"✅ Keep-alive tugadi: {pinged} muvaffaqiyatli, {failed} xato")

        except Exception as e:
            logger.error(f"💥 Keep-alive task xatosi: {e}")

        # 45 kun (45 * 24 * 3600 sekund) kutish
        await asyncio.sleep(45 * 24 * 3600)

@app.post("/upload/image")
async def upload_image(
    file: UploadFile = File(...), 
    caption: Optional[str] = Form(None),
    user: dict = Depends(get_current_user)
):
    """Rasm faylni local storage ga yuklash"""
    if not is_admin(user['id']):
        logger.warning(f"🚫 403 Forbidden: User {user['id']} rasm yuklashga urindi")
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    if caption:
        logger.info(f"📸 Rasm yuklanmoqda, caption: {caption}")
    
    ext = Path(file.filename).suffix or ".jpg"
    unique_filename = f"images/{uuid.uuid4()}{ext}"
    
    result = await upload_to_supabase(file.file, unique_filename, file.content_type)
    
    if result:
        full_url = format_media_url(result)
        return {"ok": True, "url": full_url}
    else:
        raise HTTPException(status_code=500, detail="Rasm yuklashda xatolik yuz berdi")

async def process_video_multiple_qualities(input_path: str, base_output_path: str):
    """
    Generates multiple quality versions of a video (360p, 720p).
    Returns a dict mapping quality labels to temporary file paths.
    Optimized for Android compatibility with baseline profile.
    """
    qualities = {
        "720p": {"scale": "1280:-2", "bitrate": "1500k", "crf": "24"},
        "360p": {"scale": "640:-2", "bitrate": "600k", "crf": "28"}
    }
    
    results = {}
    
    for label, config in qualities.items():
        output_file = f"{base_output_path}_{label}.mp4"
        cmd = [
            "ffmpeg", "-y",
            "-i", input_path,
            "-vf", f"scale={config['scale']}",
            "-c:v", "libx264",
            "-profile:v", "baseline",  # Android compatibility - baseline profile
            "-level", "3.0",  # Compatibility level
            "-preset", "medium",  # Changed from ultrafast for better compatibility
            "-crf", config['crf'],
            "-b:v", config['bitrate'],
            "-maxrate", config['bitrate'],  # Prevent bitrate spikes
            "-bufsize", "3000k",  # Buffer size for streaming
            "-c:a", "aac",
            "-b:a", "128k",
            "-ar", "44100",  # Standard audio sample rate
            "-ac", "2",  # Stereo audio
            "-movflags", "+faststart",  # Enable progressive download
            "-pix_fmt", "yuv420p",  # Standard pixel format for compatibility
            "-colorspace", "bt709",  # Standard color space
            "-color_primaries", "bt709",
            "-color_trc", "bt709",
            output_file
        ]
        
        logger.info(f"🎞️ Processing {label}: {' '.join(cmd)}")
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode == 0:
                results[label] = output_file
                logger.info(f"✅ Created {label} version")
            else:
                logger.error(f"❌ FFmpeg error for {label}: {stderr.decode()}")
        except Exception as e:
            logger.error(f"💥 FFmpeg process exception for {label}: {e}")
            
    return results

@app.post("/admin/upload-video-binary")
async def admin_upload_video_binary(
    request: Request, 
    filename: str = Query(...), 
    content_type: str = Query("video/mp4"),
    user: dict = Depends(get_current_user)
):
    """Stream orqali DoodStream ga video yuklash"""
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    logger.info(f"📂 Admin {user['id']} DoodStream ga yuklayapti: {filename}")
    
    try:
        file_extension = Path(filename).suffix or ".mp4"
        unique_filename = f"{uuid.uuid4()}{file_extension}"
        
        async def body_stream():
            async for chunk in request.stream():
                yield chunk

        file_code = await upload_video_to_doodstream(body_stream(), unique_filename, content_type)
        
        if file_code:
            return {
                "ok": True, 
                "url": file_code,          # DoodStream file_code ni URL sifatida saqlaymiz
                "content_type": "doodstream"
            }
        else:
            raise HTTPException(status_code=500, detail="DoodStream ga yuklashda xatolik")
            
    except Exception as e:
        logger.error(f"Binary upload xatosi: {e}")
        raise HTTPException(status_code=500, detail=f"Xatolik: {str(e)}")

@app.post("/admin/upload-video")
async def admin_upload_video(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    """Multipart video yuklash — DoodStream ga"""
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    unique_filename = f"{uuid.uuid4()}{Path(file.filename).suffix or '.mp4'}"
    
    async def file_streamer():
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk: break
            yield chunk

    file_code = await upload_video_to_doodstream(file_streamer(), unique_filename, file.content_type or "video/mp4")
    if file_code:
        return {"ok": True, "url": file_code, "content_type": "doodstream"}
    raise HTTPException(status_code=500, detail="DoodStream ga yuklash muvaffaqiyatsiz tugadi")

@app.get("/get-stream-link/{item_type}/{item_id}")
async def get_stream_link(item_type: str, item_id: str):
    """Generate or reuse a temporary streaming link for an anime episode or short"""
    logger.info(f"🎬 Stream link request: type={item_type}, id={item_id}")
    try:
        # Check if we already have a valid token for this item
        now = datetime.now()
        for token, data in STREAM_CACHE.items():
            if data.get("item_type") == item_type and data.get("item_id") == item_id and data["expires"] > now + timedelta(minutes=1):
                logger.info(f"♻️  Reusing cached URL for {item_type}/{item_id}")
                video_url = data.get("video_url")
                if video_url:
                    if video_url.startswith("http"): return {"ok": True, "stream_url": video_url}
                    if is_doodstream_code(video_url): return {"ok": True, "stream_url": build_doodstream_embed_url(video_url)}
                    # Relative path
                    backend_base = os.getenv("RENDER_EXTERNAL_URL", "https://backend123-rk6r.onrender.com").rstrip('/')
                    if video_url.startswith("/"): video_url = video_url[1:]
                    return {"ok": True, "stream_url": f"{backend_base}/{video_url}",
                            "expires_in": int((data["expires"] - now).total_seconds())}

        with get_db() as conn:
            with conn.cursor() as cursor:
                # Direct S3/R2 Link Logic
                video_url = None
                
                if item_type == "anime":
                    logger.debug(f"Looking up episode: {item_id}")
                    cursor.execute("SELECT video_url FROM episodes WHERE id = %s", (item_id,))
                    row = cursor.fetchone()
                    if row: video_url = row['video_url']
                elif item_type == "shorts":
                    logger.debug(f"Looking up short: {item_id}")
                    cursor.execute("SELECT video_url FROM shorts WHERE id = %s", (item_id,))
                    row = cursor.fetchone()
                    if row: video_url = row['video_url']
                elif item_type == "ad":
                    logger.debug(f"Looking up video ad: {item_id}")
                    cursor.execute("SELECT video_url FROM video_ads WHERE id = %s", (item_id,))
                    row = cursor.fetchone()
                    if row: video_url = row['video_url']

                if not video_url:
                    raise HTTPException(status_code=404, detail="Video URL topilmadi")

                # To'liq HTTP URL bo'lsa — shundayligicha qaytarish
                if video_url.startswith("http"):
                    return {"ok": True, "stream_url": video_url}

                # DoodStream file_code bo'lsa — embed URL yasash
                if is_doodstream_code(video_url):
                    embed_url = build_doodstream_embed_url(video_url.strip())
                    return {
                        "ok": True,
                        "stream_url": embed_url,
                        "file_code": video_url.strip(),
                        "provider": "doodstream"
                    }

                # JSON multi-quality (legacy)
                if video_url.strip().startswith('{'):
                    try:
                        qualities = json.loads(video_url)
                        formatted = {}
                        for quality, path in qualities.items():
                            if path.startswith('http'):
                                formatted[quality] = path
                            elif is_doodstream_code(path):
                                formatted[quality] = build_doodstream_embed_url(path)
                            else:
                                formatted[quality] = path
                        return {"ok": True, "stream_url": json.dumps(formatted)}
                    except:
                        pass

                # Fallback
                return {"ok": True, "stream_url": video_url}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"💥 Error in get_stream_link: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Stream linkini olishda xatolik yuz berdi")


# ── DoodStream Admin Endpoints ───────────────────────────────────────────────

@app.post("/admin/doodstream/keep-alive")
async def manual_keep_alive(user: dict = Depends(get_current_user)):
    """
    Admin: barcha DoodStream videolarni qo'lda 'ping' qilish.
    Avtomatik 45-kunlik task bilan bir xil mantiq, lekin zudlik bilan ishlaydi.
    """
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")

    file_codes = []
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT video_url FROM episodes WHERE video_url IS NOT NULL")
            file_codes += [r['video_url'] for r in cursor.fetchall()]
            cursor.execute("SELECT video_url FROM shorts WHERE video_url IS NOT NULL")
            file_codes += [r['video_url'] for r in cursor.fetchall()]
            cursor.execute("SELECT video_url FROM video_ads WHERE video_url IS NOT NULL")
            file_codes += [r['video_url'] for r in cursor.fetchall()]

    ds_codes = list(set([c for c in file_codes if is_doodstream_code(c)]))

    pinged, failed, missing = 0, 0, []
    async with aiohttp.ClientSession() as session:
        for code in ds_codes:
            try:
                resp = await session.get(
                    f"{Config.DOODSTREAM_API_BASE}/file/info",
                    params={"key": Config.DOODSTREAM_API_KEY, "file_code": code},
                    timeout=aiohttp.ClientTimeout(total=15)
                )
                data = await resp.json()
                if data.get("status") == 200:
                    pinged += 1
                else:
                    failed += 1
                    missing.append(code)
            except Exception as e:
                failed += 1
                logger.warning(f"⚠️ Keep-alive ping [{code}]: {e}")
            await asyncio.sleep(0.5)

    logger.info(f"✅ Manual keep-alive: {pinged} ok, {failed} xato")
    return {
        "ok": True,
        "total_videos": len(ds_codes),
        "pinged": pinged,
        "failed": failed,
        "missing_codes": missing,
        "message": f"{pinged} ta video yangilandi, {failed} ta xato"
    }


@app.get("/admin/doodstream/stats")
async def doodstream_stats(user: dict = Depends(get_current_user)):
    """Admin: DoodStream saqlash statistikasi va video soni"""
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")

    try:
        async with aiohttp.ClientSession() as session:
            resp = await session.get(
                f"{Config.DOODSTREAM_API_BASE}/account/info",
                params={"key": Config.DOODSTREAM_API_KEY},
                timeout=aiohttp.ClientTimeout(total=15)
            )
            data = await resp.json()

        # Database dagi video sonlari
        with get_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) as cnt FROM episodes WHERE video_url IS NOT NULL")
                ep_count = cursor.fetchone()['cnt']
                cursor.execute("SELECT COUNT(*) as cnt FROM shorts WHERE video_url IS NOT NULL")
                sh_count = cursor.fetchone()['cnt']
                cursor.execute("SELECT COUNT(*) as cnt FROM video_ads WHERE video_url IS NOT NULL")
                ad_count = cursor.fetchone()['cnt']

        return {
            "ok": True,
            "doodstream_account": data.get("result", {}),
            "database_videos": {
                "episodes": ep_count,
                "shorts": sh_count,
                "video_ads": ad_count,
                "total": ep_count + sh_count + ad_count,
            }
        }
    except Exception as e:
        logger.error(f"DoodStream stats xatosi: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ping")
async def ping():
    return {"ok": True, "time": datetime.now().isoformat(), "service": "CosmosTV Backend"}

@app.get("/test/stream/{item_type}/{item_id}")
async def test_stream_link(item_type: str, item_id: str):
    """Test endpoint to validate stream links - returns JSON with full URL for browser testing"""
    try:
        # Get stream link
        result = await get_stream_link(item_type, item_id)
        
        # Build full URL
        base_url = "https://backend123-rk6r.onrender.com"
        full_stream_url = f"{base_url}{result['stream_url']}"
        
        return {
            "ok": True,
            "item_type": item_type,
            "item_id": item_id,
            "stream_url": result['stream_url'],
            "full_url": full_stream_url,
            "expires_in": result['expires_in'],
            "test_instructions": {
                "uz": f"Browser da bu URL ni oching: {full_stream_url}",
                "en": f"Open this URL in browser: {full_stream_url}"
            }
        }
    except HTTPException as e:
        return {
            "ok": False,
            "error": e.detail,
            "status_code": e.status_code,
            "item_type": item_type,
            "item_id": item_id
        }
    except Exception as e:
        logger.error(f"Test endpoint error: {e}")
        return {
            "ok": False,
            "error": str(e),
            "item_type": item_type,
            "item_id": item_id
        }


# --- Auth ---

@app.post("/register", status_code=status.HTTP_201_CREATED)
async def register(user: UserRegister):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM users WHERE email = %s", (user.email,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Email already registered")

            user_id = generate_id()
            short_id = generate_short_id()
            hashed = hash_password(user.password)
            cursor.execute("""
                INSERT INTO users (id, short_id, name, email, password, plain_password, balance, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, short_id, user.name, user.email, hashed, user.password, 0, datetime.now()))

            token = create_access_token(user_id)
            return {
                "ok": True,
                "token": token,
                "user": {
                    "id": user_id,
                    "short_id": short_id,
                    "name": user.name,
                    "email": user.email,
                    "balance": 0,
                    "avatar_url": None,
                    "isAdmin": False,
                    "isSuperAdmin": False
                }
            }

@app.get("/me")
async def get_my_profile(user: dict = Depends(get_current_user)):
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Get purchased animes
            cursor.execute("SELECT anime_id FROM purchases WHERE user_id = %s", (user['id'],))
            purchased = [row['anime_id'] for row in cursor.fetchall()]
            
            user['purchasedAnimes'] = purchased
            user['isAdmin'] = is_admin(user['id'])
            user['isSuperAdmin'] = is_super_admin(user['id'])
            user['isFinanceAdmin'] = is_finance_admin(user['id'])
            
            # Fetch role specifically
            cursor.execute("SELECT role FROM admins WHERE id = %s", (user['id'],))
            admin_row = cursor.fetchone()
            user['role'] = admin_row['role'] if admin_row else 'user'
            
            # Check if premium expired
            is_premium = user.get('is_premium_user', False)
            if is_premium and user.get('premium_expiry'):
                if datetime.now() > user['premium_expiry']:
                    # Update DB if expired
                    cursor.execute("UPDATE users SET is_premium_user = FALSE WHERE id = %s", (user['id'],))
                    user['is_premium_user'] = False
            
            if user.get('avatar_url'):
                user['avatar_url'] = format_media_url(user['avatar_url'])
            
            return {"ok": True, "user": user}
@app.post("/user/avatar")
async def upload_user_avatar(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    ext = file.filename.split('.')[-1]
    unique_filename = f"avatars/{user['id']}_{generate_id()}.{ext}"
    
    avatar_url = await upload_to_supabase(file.file, unique_filename, file.content_type)
    if not avatar_url:
        raise HTTPException(status_code=500, detail="Rasm yuklashda xatolik yuz berdi")
    
    # We update the user record with the RELATIVE path (filename with prefix)
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE users SET avatar_url = %s WHERE id = %s", (avatar_url, user['id']))
            return {"ok": True, "avatar_url": format_media_url(avatar_url)}


@app.post("/login")
async def login(creds: UserLogin, background_tasks: BackgroundTasks):
    with get_db() as conn:
        with conn.cursor() as cursor:
            hashed = hash_password(creds.password)
            cursor.execute("""
                SELECT id, name, email, balance, is_premium_user, premium_expiry, avatar_url, short_id, plain_password FROM users
                WHERE email = %s AND password = %s
            """, (creds.email, hashed))
            
            user = cursor.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="Email yoki parol xato")

            # Backfill plain_password for existing users on login
            if not user.get('plain_password'):
                cursor.execute("UPDATE users SET plain_password = %s WHERE id = %s", (creds.password, user['id']))
                conn.commit()

            user_id = user['id']
            admin_status = is_admin(user_id)
            super_admin_status = is_super_admin(user_id)

            # Unified Login (OTP Disabled)
            token = create_access_token(user_id)
            
            # Get purchased animes
            cursor.execute("SELECT anime_id FROM purchases WHERE user_id = %s", (user_id,))
            purchased = [row['anime_id'] for row in cursor.fetchall()]
            
            return {
                "ok": True,
                "success": True,
                "token": token,
                "requires_otp": False,
                "user": {
                    "id": user['id'],
                    "name": user['name'],
                    "email": user['email'],
                    "balance": user['balance'],
                    "is_premium_user": user.get('is_premium_user', False),
                    "premium_expiry": user.get('premium_expiry').isoformat() if user.get('premium_expiry') else None,
                    "purchasedAnimes": purchased,
                    "isAdmin": admin_status,
                    "isSuperAdmin": super_admin_status,
                    "avatar_url": format_media_url(user.get('avatar_url')),
                    "short_id": user.get('short_id')
                }
            }

@app.post("/verify-otp")
async def verify_otp(req: VerifyOtpRequest):
    data = otp_storage.get(req.email)
    
    if not data:
        raise HTTPException(status_code=400, detail="Kod topilmadi yoki muddati tugagan")
        
    if datetime.now() > data['expires']:
        del otp_storage[req.email]
        raise HTTPException(status_code=400, detail="Kod muddati tugagan")
        
    if data['code'] != req.code:
        raise HTTPException(status_code=400, detail="Kod noto'g'ri")

    # OTP Valid - Generate Token
    user_id = data['user_id']
    del otp_storage[req.email] # Consume OTP

    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, name, email, balance, is_premium_user, premium_expiry, avatar_url FROM users WHERE id = %s", (user_id,))
            user = cursor.fetchone()
            if user.get('avatar_url'):
                user['avatar_url'] = format_media_url(user['avatar_url'])
            
            # Get purchased animes
            cursor.execute("SELECT anime_id FROM purchases WHERE user_id = %s", (user_id,))
            purchased = [row['anime_id'] for row in cursor.fetchall()]

            admin_status = is_admin(user_id)
            super_admin_status = is_super_admin(user_id)

            token = create_access_token(user_id)
            return {
                "ok": True,
                "success": True,
                "token": token,
                "user": {
                    "id": user['id'],
                    "name": user['name'],
                    "email": user['email'],
                    "balance": user['balance'],
                    "is_premium_user": user.get('is_premium_user', False),
                    "premium_expiry": user.get('premium_expiry').isoformat() if user.get('premium_expiry') else None,
                    "purchasedAnimes": purchased,
                    "isAdmin": admin_status,
                    "isSuperAdmin": super_admin_status
                }
            }

# --- Payment ---

def update_payment_status(payment_id: str, cheque_id: str) -> bool:
    """Check payment status from TSPay and update DB. Returns True if completed."""
    try:
        # Fetch latest token from DB
        shop_token = get_setting('tspay_shop_token', Config.TSPAY_SHOP_TOKEN)
        
        # Construct URL with access_token as query parameter
        url = f"https://tspay.uz/api/v1/transactions/{cheque_id}/?access_token={shop_token}"
        
        logger.info(f"🔍 Checking TSPay status for payment {payment_id} (cheque: {cheque_id})")
        
        req = urllib.request.Request(url, method='GET')
        with urllib.request.urlopen(req, timeout=10) as response:
            if response.status >= 200 and response.status < 300:
                data = json.loads(response.read().decode())
                logger.info(f"✅ TSPay response for {payment_id}: {data}")
                
                # Robust status check: can be boolean True or string "success"
                is_success = data.get('status') == 'success' or data.get('status') is True
                pay_status = data.get('data', {}).get('pay_status') or data.get('pay_status')
                
                if is_success and pay_status == 'paid':
                     with get_db() as conn:
                        with conn.cursor() as cursor:
                            cursor.execute("SELECT user_id, amount, status FROM payments WHERE id = %s FOR UPDATE", (payment_id,))
                            payment = cursor.fetchone()
                            
                            if payment and payment['status'] == 'pending':
                                # Update balance
                                cursor.execute("UPDATE users SET balance = balance + %s WHERE id = %s", 
                                               (payment['amount'], payment['user_id']))
                                # Update payment record
                                cursor.execute("UPDATE payments SET status = %s, updated_at = %s WHERE id = %s",
                                               ('completed', datetime.now(), payment_id))
                                logger.info(f"💰 Payment {payment_id} COMPLETED. Added {payment['amount']} to user {payment['user_id']}")
                                return True
                            elif payment and payment['status'] == 'completed':
                                return True
            else:
                logger.error(f"❌ TSPay check status failed: HTTP {response.status}")
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        logger.error(f"❌ TSPay check status HTTP Error: {e.code} - {error_body}")
    except Exception as e:
        logger.error(f"❌ Error checking payment status: {e}")
    
    return False

@app.post("/payment/create", status_code=201)
async def create_payment(payment: PaymentCreate):
    if not TSPAY_AVAILABLE:
        raise HTTPException(status_code=503, detail="Payment system unavailable")

    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT name FROM users WHERE id = %s", (payment.user_id,))
            user = cursor.fetchone()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
                
            try:
                # Fetch latest token from DB
                shop_token = get_setting('tspay_shop_token', Config.TSPAY_SHOP_TOKEN)
                
                # Create transaction via TSPay direct API call
                url = "https://tspay.uz/api/v1/transactions/create/"
                payload = {
                    "amount": int(payment.amount),
                    "access_token": shop_token,
                    "redirect_url": Config.TSPAY_REDIRECT_URL,
                    "comment": f"{payment.description} - {user['name']}"
                }
                
                logger.info(f"🚀 Creating TSPay transaction: {payment.amount} sum for {user['name']}")
                
                data = json.dumps(payload).encode('utf-8')
                req = urllib.request.Request(url, data=data, method='POST')
                req.add_header('Content-Type', 'application/json')
                
                with urllib.request.urlopen(req, timeout=10) as response:
                    resp_data = json.loads(response.read().decode())
                    logger.info(f"✅ TSPay create response: {resp_data}")
                    
                    # Robust check
                    is_success = resp_data.get('status') == 'success' or resp_data.get('status') is True
                    if not is_success:
                        raise Exception(f"TSPay error status: {resp_data}")
                        
                    transaction = resp_data.get('transaction') or resp_data.get('data') or resp_data

            except urllib.error.HTTPError as e:
                error_body = e.read().decode()
                logger.error(f"❌ TSPay Create HTTP Error: {e.code} - {error_body}")
                raise HTTPException(status_code=503, detail=f"TSPay Gateway Error: {error_body[:100]}")
            except Exception as e:
                logger.error(f"❌ TSPay error: {e}")
                raise HTTPException(status_code=503, detail="Payment gateway error")


            payment_id = generate_id()
            
            # Robust key extraction for different TSPay API versions
            cheque_id = str(
                transaction.get('cheque_id') or 
                transaction.get('id') or 
                transaction.get('transaction_id') or
                resp_data.get('id')
            )
            payment_url = (
                transaction.get('payment_url') or 
                transaction.get('url') or 
                transaction.get('link') or
                resp_data.get('url')
            )

            cursor.execute("""
                INSERT INTO payments (id, user_id, amount, status, cheque_id, payment_url, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (payment_id, payment.user_id, payment.amount, 'pending', cheque_id, payment_url, datetime.now()))

            return {
                "ok": True,
                "payment": {
                    "id": payment_id,
                    "userId": payment.user_id,
                    "amount": payment.amount,
                    "status": "pending",
                    "chequeId": cheque_id,
                    "paymentUrl": payment_url,
                    "createdAt": datetime.now().isoformat()
                }
            }

@app.post("/payment/check")
async def check_payment_endpoint(req: PaymentCheckRequest):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM payments WHERE id = %s", (req.paymentId,))
            payment = cursor.fetchone()
            if not payment:
                raise HTTPException(status_code=404, detail="Payment not found")
            
            current_status = payment['status']
            if current_status == 'pending':
                # Run synchronous check and update DB immediately
                is_completed = update_payment_status(payment['id'], payment['cheque_id'])
                if is_completed:
                    current_status = 'completed'

            return {
                "ok": True,
                "payment": {
                    "id": payment['id'],
                    "status": current_status,
                    "amount": payment['amount']
                }
            }


@app.get("/payment/callback")
async def payment_callback(cheque_id: str = Query(...), background_tasks: BackgroundTasks = None):
    # Retrieve payment by cheque_id
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, cheque_id FROM payments WHERE cheque_id = %s AND status = 'pending'", (cheque_id,))
            payment = cursor.fetchone()
            if payment:
                 # Trigger check
                 if background_tasks:
                     background_tasks.add_task(update_payment_status, payment['id'], payment['cheque_id'])
                 else:
                     # If no background task (unlikely), run sync or ignore
                     pass
    
    return {"ok": True, "message": "Check initiated"}

@app.get("/user/{user_id}/payments")
async def get_payments_history(user_id: str):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT id, amount, status, created_at 
                FROM payments WHERE user_id = %s 
                ORDER BY created_at DESC LIMIT 50
            """, (user_id,))
            payments = cursor.fetchall()
            return {"ok": True, "payments": payments}

@app.post("/payment/webhook")
async def payment_webhook(request: Request):
    """Callback URL for TSPay verification"""
    try:
        data = await (request.json() if request.headers.get("content-type") == "application/json" else request.form())
        logger.info(f"🔔 Webhook received: {data}")
        
        cheque_id = data.get("cheque_id") or data.get("id")
        if not cheque_id:
             return {"ok": False, "error": "Missing cheque_id"}

        # Use our secure check logic
        with get_db() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT id, status FROM payments WHERE cheque_id = %s", (cheque_id,))
                payment = cursor.fetchone()
                
                if payment and payment['status'] == 'pending':
                    # Trigger synchronous verification
                    is_success = update_payment_status(payment['id'], cheque_id)
                    if is_success:
                        return {"ok": True, "message": "Payment verified and processed"}
                
                elif payment and payment['status'] == 'completed':
                    return {"ok": True, "message": "Already processed"}
                    
        return {"ok": False, "error": "Verification failed or payment not found"}

    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")
        return {"ok": False}


# --- Anime & Content ---

def format_media_url(url: Optional[str]) -> Optional[str]:
    """Helper to format media URLs. Handles DoodStream file_codes, JSON multi-quality URLs, and relative paths."""
    if not url: return None

    # If it's already a full URL, return as-is
    if url.startswith("http"): return url

    # If it's a DoodStream file_code (short alphanumeric, no slash or dot)
    stripped = url.strip().strip('/')
    if stripped and not stripped.startswith('{') and '/' not in stripped and '.' not in stripped and len(stripped) <= 20:
        # DoodStream file_code — return embed URL
        return build_doodstream_embed_url(stripped)

    # Check if it's a JSON object (multi-quality video - legacy)
    if url.strip().startswith('{'):
        try:
            qualities = json.loads(url)
            formatted = {}
            backend_base = os.getenv("RENDER_EXTERNAL_URL", "https://backend123-rk6r.onrender.com").rstrip('/')
            for quality, path in qualities.items():
                if path.startswith('http'):
                    formatted[quality] = path
                elif is_doodstream_code(path):
                    # DoodStream file_code
                    formatted[quality] = build_doodstream_embed_url(path)
                else:
                    clean_path = path[1:] if path.startswith('/') else path
                    formatted[quality] = f"{backend_base}/{clean_path}"
            return json.dumps(formatted)
        except Exception as e:
            logger.error(f"Error parsing JSON URL: {e}")

    # Relative path -> prepend backend base URL (rasmlar uchun)
    backend_base = os.getenv("RENDER_EXTERNAL_URL", "https://backend123-rk6r.onrender.com").rstrip('/')
    if url.startswith("/"): url = url[1:]
    return f"{backend_base}/{url}"

@app.get("/animes")
async def get_animes(limit: int = 50, offset: int = 0, status: str = Query(None), search: str = Query(None)):
    with get_db() as conn:
         with conn.cursor() as cursor:
             query = "SELECT * FROM animes"
             params = []
             conditions = []
             
             if status:
                 conditions.append("status = %s")
                 params.append(status)
             
             if search:
                 conditions.append("title ILIKE %s")
                 params.append(f"%{search}%")
             
             if conditions:
                 query += " WHERE " + " AND ".join(conditions)
             
             query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
             params.extend([limit, offset])
             
             cursor.execute(query, tuple(params))
             animes = cursor.fetchall()
             
             # Format URLs
             results = []
             for anime in animes:
                 a = dict(anime)
                 a['poster_url'] = format_media_url(a.get('poster_url'))
                 a['banner_url'] = format_media_url(a.get('banner_url'))
                 results.append(a)
                 
             return {"ok": True, "animes": results}

# --- Settings / Config ---

@app.get("/app/config")
async def get_app_config():
    """Get app configuration (CACHED for 1 hour)"""
    cache_key = "app_config"
    current_time = time.time()
    
    # Check cache
    if cache_key in API_CACHE and API_CACHE_TTL.get(cache_key, 0) > current_time:
        logger.debug("⚡ Cache HIT: /app/config")
        return API_CACHE[cache_key]
    
    # Fetch from database
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT key, value FROM settings")
            rows = cursor.fetchall()
            config = {row['key']: row['value'] for row in rows}
            
            # Add support contact info
            config['support_telegram'] = '@CallmeMeduza'
            config['support_admin_id'] = '076080532000'
            
            result = {"ok": True, "config": config}
            
            # Cache for 1 hour
            API_CACHE[cache_key] = result
            API_CACHE_TTL[cache_key] = current_time + 3600
            logger.debug("💾 Cache MISS: /app/config (cached for 1h)")
            
            return result

@app.get("/anime/{id}")
async def get_anime_details(id: str):
    logger.info(f"🔍 Fetching anime details: {id}")
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM animes WHERE id = %s", (id,))
            anime = cursor.fetchone()
            if not anime:
                logger.warning(f"⚠️ Anime not found: {id}")
                raise HTTPException(status_code=404, detail="Anime topilmadi")

            # Format Anime URLs
            anime_data = dict(anime)
            anime_data['poster_url'] = format_media_url(anime_data.get('poster_url'))
            anime_data['banner_url'] = format_media_url(anime_data.get('banner_url'))
            
            cursor.execute("SELECT * FROM episodes WHERE anime_id = %s ORDER BY episode_number ASC", (id,))
            episodes = cursor.fetchall()
            
            logger.info(f"   Found {len(episodes)} episodes for anime {id}")
            
            # Format video URLs for episodes
            formatted_episodes = []
            for ep in episodes:
                ep_dict = dict(ep)
                ep_dict['video_url'] = format_media_url(ep_dict.get('video_url'))
                formatted_episodes.append(ep_dict)
            
            anime_data['episodes'] = formatted_episodes
            return {"ok": True, "anime": anime_data}

@app.post("/animes")
async def create_anime(anime: AnimeCreate):
    if not is_admin(anime.added_by):
         raise HTTPException(status_code=403, detail="Admin privileges required")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            anime_id = generate_id()
            
            # Insert Anime
            cursor.execute("""
                INSERT INTO animes (
                    id, title, genre, description, price, poster_url, banner_url, 
                    added_by, dubbing_name, status, is_premium, views, purchases, revenue, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, 0, 0, %s)
            """, (
                anime_id, anime.title, anime.genre, anime.description, anime.price,
                anime.poster_url, anime.banner_url, anime.added_by, 
                getattr(anime, 'dubbing_name', '') or "Dublyaj",
                anime.status, anime.is_premium,
                datetime.now()
            ))

            # Insert Episodes
            if anime.episodes:
                for ep in anime.episodes:
                    ep_id = generate_id()
                    video_url = ep.get('video_url') or ep.get('videoUrl')
                    ep_num = ep.get('number') or ep.get('episode_number') or 1
                    
                    cursor.execute("""
                        INSERT INTO episodes (id, anime_id, title, video_url, episode_number, added_at, tg_message_id, tg_file_id, content_type)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (ep_id, anime_id, ep.get('title', 'Qism'), video_url, 
                          ep_num, datetime.now(), ep.get('tg_message_id'), ep.get('tg_file_id'), ep.get('content_type')))
            
            
            # Send Notification
            try:
                msg_body = f"{anime.genre} • {anime.status}"
                send_push_notification(
                    "new_anime",
                    f"✨ Yangi Anime: {anime.title}",
                    msg_body,
                    # Format image URL if needed (relative -> absolute)
                    image_url=format_media_url(anime.poster_url),
                    data={"type": "anime", "id": anime_id}
                )
                # Database Notification: New Anime
                create_notification(None, "Yangi Anime Qo'shildi", f"🎬 '{anime.title}' animasini tomosha qiling!", "anime", anime_id)
            except Exception as e:
                logger.error(f"Notification error: {e}")

            return {"ok": True, "animeId": anime_id}

@app.put("/animes/{id}")
async def update_anime(id: str, anime: AnimeCreate):
    if not is_admin(anime.added_by):
         raise HTTPException(status_code=403, detail="Admin privileges required")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check if anime exists
            cursor.execute("SELECT id FROM animes WHERE id = %s", (id,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Anime not found")

            # Update Anime
            cursor.execute("""
                UPDATE animes SET
                    title = %s, genre = %s, description = %s, price = %s,
                    poster_url = %s, banner_url = %s, dubbing_name = %s,
                    status = %s, is_premium = %s
                WHERE id = %s
            """, (
                anime.title, anime.genre, anime.description, anime.price,
                anime.poster_url, anime.banner_url, 
                getattr(anime, 'dubbing_name', '') or "Dublyaj",
                anime.status, anime.is_premium, id
            ))
            
            # Update episodes: Append new ones if provided
            if anime.episodes:
                # Get current episode count to determine numbers
                cursor.execute("SELECT MAX(episode_number) as max_num FROM episodes WHERE anime_id = %s", (id,))
                res = cursor.fetchone()
                current_max = res['max_num'] if res and res['max_num'] else 0
                
                # We only add episodes that don't already exist or are marked as new
                # For simplicity in this common pattern, we'll look for video_url that isn't already there
                # Or better, just add new ones if the count in request > count in DB
                # Get existing episode keys
                cursor.execute("SELECT video_url, tg_message_id FROM episodes WHERE anime_id = %s", (id,))
                existing_rows = cursor.fetchall()
                existing_urls = [row['video_url'] for row in existing_rows if row['video_url'] != 'tg_stream']
                existing_tg_ids = [row['tg_message_id'] for row in existing_rows if row['tg_message_id'] is not None]
                
                logger.info(f"Updating anime {id}: existing_urls={len(existing_urls)}, existing_tg_ids={len(existing_tg_ids)}")
                
                new_count = 0
                for i, ep in enumerate(anime.episodes):
                    video_url = ep.get('video_url') or ep.get('videoUrl')
                    tg_id = ep.get('tg_message_id')
                    
                    is_new = False
                    if tg_id and tg_id not in existing_tg_ids:
                        is_new = True
                    elif video_url and video_url != 'tg_stream' and video_url not in existing_urls:
                        is_new = True
                        
                    if is_new:
                        logger.info(f"  Adding new episode: {ep.get('title')} (tg_id={tg_id})")
                        new_count += 1
                        ep_id = generate_id()
                        ep_num = ep.get('number') or ep.get('episode_number') or (current_max + new_count)
                        
                        cursor.execute("""
                            INSERT INTO episodes (id, anime_id, title, video_url, episode_number, added_at, tg_message_id, tg_file_id)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (ep_id, id, ep.get('title', 'Qism'), video_url, ep_num, datetime.now(), ep.get('tg_message_id'), ep.get('tg_file_id')))

                        # Send Notification for New Episode
                        try:
                            # Only notify if it's a significant update (e.g., new video URL, not just metadata edit)
                            send_push_notification(
                                "new_anime", # Or "all_users" or specific topic
                                f"🔥 Yangi Qism: {anime.title}",
                                f"{ep_num}-qism yuklandi! Hozir tomosha qiling.",
                                {"type": "anime", "id": id}
                            )
                        except Exception as e:
                            logger.error(f"Notification error: {e}")
            
            return {"ok": True, "message": "Anime and episodes updated"}

@app.delete("/animes/{id}")
async def delete_anime(id: str, user_id: str = Query(...)):
    if not is_admin(user_id):
         raise HTTPException(status_code=403, detail="Admin privileges required")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM animes WHERE id = %s", (id,))
            if cursor.rowcount == 0:
                raise HTTPException(status_code=404, detail="Anime not found")
            return {"ok": True, "message": "Anime deleted"}

@app.post("/anime/{id}/purchase")
async def purchase_anime_endpoint(id: str, req: PurchaseRequest):
    with get_db() as conn:
        with conn.cursor() as cursor:
            # 1. Get Anime Price
            cursor.execute("SELECT price, title FROM animes WHERE id = %s", (id,))
            anime = cursor.fetchone()
            if not anime:
                raise HTTPException(status_code=404, detail="Anime not found")
            
            # Convert to float to avoid Decimal/float type mismatch
            price = float(anime['price'])
            
            # 2. Check User Balance
            cursor.execute("SELECT balance, email FROM users WHERE id = %s FOR UPDATE", (req.userId,))
            user = cursor.fetchone()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")
            
            # Convert balance to float
            user_balance = float(user['balance'])
            
            # 2.1 Check if already purchased
            cursor.execute("SELECT id FROM purchases WHERE user_id = %s AND anime_id = %s", (req.userId, id))
            if cursor.fetchone():
                 return {"ok": True, "message": "Already purchased"}

            if user_balance < price:
                raise HTTPException(status_code=402, detail="Mablag' yetarli emas")
            
            # 3. Process Transaction
            new_balance = user_balance - price
            cursor.execute("UPDATE users SET balance = %s WHERE id = %s", (new_balance, req.userId))
            
            purchase_id = generate_id()
            cursor.execute("""
                INSERT INTO purchases (id, user_id, anime_id, price, purchased_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (purchase_id, req.userId, id, price, datetime.now()))
            
            # 4. Update Anime Stats
            cursor.execute("""
                UPDATE animes SET purchases = purchases + 1, revenue = revenue + %s 
                WHERE id = %s
            """, (price, id))
            
            return {
                "ok": True, 
                "message": f"{anime['title']} sotib olindi",
                "newBalance": new_balance
            }

@app.post("/anime/{id}/episodes")
async def add_episode(id: str, episode: EpisodeCreate):
    # Verify Admin
    if not is_admin(episode.addedBy):
         raise HTTPException(status_code=403, detail="Privileges required")

    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check anime exists
            cursor.execute("SELECT id FROM animes WHERE id = %s", (id,))
            if not cursor.fetchone():
                 raise HTTPException(status_code=404, detail="Anime not found")

            # Determine episode number (auto-increment if not careful, but let's trust inputs or auto-calc)
            cursor.execute("SELECT COUNT(*) as count FROM episodes WHERE anime_id = %s", (id,))
            count = cursor.fetchone()['count']
            ep_num = count + 1

            ep_id = generate_id()
            cursor.execute("""
                INSERT INTO episodes (id, anime_id, title, video_url, episode_number, added_at, tg_message_id, tg_file_id, content_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ep_id, id, episode.title, episode.videoUrl, ep_num, datetime.now(), episode.tg_message_id, episode.tg_file_id, episode.content_type))
            
            # Global Notification: New Episode
            cursor.execute("SELECT title FROM animes WHERE id = %s", (id,))
            anime_row = cursor.fetchone()
            anime_title = anime_row['title'] if anime_row else "Anime"
            create_notification(None, "Yangi Qism!", f"📺 '{anime_title}': {episode.title} qo'shildi.", "anime", id)
            
            return {"ok": True, "episodeId": ep_id}

@app.delete("/episodes/{episode_id}")
async def delete_episode(episode_id: str, user_id: str = Query(...)):
    if not is_admin(user_id):
          raise HTTPException(status_code=403, detail="Privileges required")
          
    with get_db() as conn:
        with conn.cursor() as cursor:
             cursor.execute("DELETE FROM episodes WHERE id = %s", (episode_id,))
             return {"ok": True}
            


# Episode handling consolidated under /anime/{id}/episodes


# Purchased animes are handled via /anime/{id}/purchase above


@app.post("/premium/purchase")
async def purchase_premium(user: dict = Depends(get_current_user)):
    PREMIUM_PRICE = 33000.0
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check balance
            cursor.execute("SELECT balance FROM users WHERE id = %s FOR UPDATE", (user['id'],))
            row = cursor.fetchone()
            if not row: raise HTTPException(status_code=404, detail="User not found")
            
            balance = float(row['balance'])
            if balance < PREMIUM_PRICE:
                raise HTTPException(status_code=402, detail="Balansingizda mablag' yetarli emas (33,000 so'm kerak)")
            
            # Deduct balance and set premium
            new_expiry = datetime.now() + timedelta(days=30)
            cursor.execute("""
                UPDATE users SET 
                    balance = balance - %s,
                    is_premium_user = TRUE,
                    premium_expiry = %s
                WHERE id = %s
            """, (PREMIUM_PRICE, new_expiry, user['id']))
            
            return {
                "ok": True, 
                "message": "Premium obuna muvaffaqiyatli faollashtirildi",
                "premium_expiry": new_expiry.isoformat()
            }

# --- Banners & Ads ---

# --- System Settings (Admin Only) ---

@app.get("/admin/fix-db")
async def fix_database():
    """Explicitly run migrations to fix missing columns"""
    migrations = [
        "ALTER TABLE users ALTER COLUMN id TYPE TEXT", # Ensure users.id is TEXT
        "ALTER TABLE purchases ALTER COLUMN user_id TYPE TEXT",
        "ALTER TABLE ads ALTER COLUMN user_id TYPE TEXT",
        "ALTER TABLE payments ALTER COLUMN user_id TYPE TEXT",
        "ALTER TABLE app_ads ALTER COLUMN anime_id DROP NOT NULL",
        "ALTER TABLE app_ads ADD COLUMN IF NOT EXISTS target_shorts_id TEXT",
        "ALTER TABLE news ADD COLUMN IF NOT EXISTS news_type TEXT DEFAULT 'ordinary'",
        "ALTER TABLE news ADD COLUMN IF NOT EXISTS target_shorts_id TEXT",
        "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
        "ALTER TABLE episodes ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
        "ALTER TABLE shorts ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
        "ALTER TABLE shorts ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
        "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_message_id INTEGER",
        "ALTER TABLE video_ads ADD COLUMN IF NOT EXISTS tg_file_id TEXT",
        "ALTER TABLE users ADD COLUMN IF NOT EXISTS avatar_url TEXT",
        "CREATE TABLE IF NOT EXISTS shorts_likes (short_id TEXT NOT NULL, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (short_id, user_id))",
        "CREATE TABLE IF NOT EXISTS shorts_comments (id TEXT PRIMARY KEY, short_id TEXT NOT NULL, user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE, text TEXT NOT NULL, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
    ]
    
    results = []
    
    # Run automatic init
    try:
        init_db()
        results.append("✅ Automatic init_db() called")
    except Exception as e:
        results.append(f"❌ init_db() error: {e}")

    # Clear global cache
    API_CACHE.clear()
    results.append("✅ API Cache cleared")

    with get_db() as conn:
        with conn.cursor() as cursor:
            for m in migrations:
                try:
                    cursor.execute(m)
                    conn.commit()
                    results.append(f"✅ {m}")
                except Exception as e:
                    conn.rollback()
                    results.append(f"ℹ️ Skipped: {m} (Reason: {e})")
            
    return {"ok": True, "results": results}

@app.get("/admin/stats")
async def get_admin_stats(user: dict = Depends(get_current_user)):
    user_id = user['id']
    is_super = is_super_admin(user_id)
    is_adm = is_admin(user_id)
    
    if not is_adm and not is_super:
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            if is_super:
                # Global Statistics
                cursor.execute("SELECT COUNT(*) as total FROM animes")
                res = cursor.fetchone()
                total_animes = res['total'] if res else 0
                
                cursor.execute("SELECT SUM(views) as total FROM animes")
                res = cursor.fetchone()
                total_views = res['total'] if res and res['total'] else 0
                
                cursor.execute("SELECT COUNT(*) as total FROM users")
                res = cursor.fetchone()
                total_users = res['total'] if res else 0
                
                return {
                    "ok": True,
                    "type": "global",
                    "stats": {
                        "total_animes": total_animes,
                        "total_views": total_views,
                        "total_users": total_users,
                        "status": "Tizim Faol"
                    }
                }
            else:
                # Personal Statistics for Admin
                cursor.execute("SELECT COUNT(*) as total FROM animes WHERE added_by = %s", (user_id,))
                res = cursor.fetchone()
                my_animes = res['total'] if res else 0
                
                cursor.execute("SELECT SUM(views) as total FROM animes WHERE added_by = %s", (user_id,))
                res = cursor.fetchone()
                my_views = res['total'] if res and res['total'] else 0
                
                return {
                    "ok": True,
                    "type": "personal",
                    "stats": {
                        "total_animes": my_animes,
                        "total_views": my_views,
                        "status": "Faoliyat"
                    }
                }

@app.post("/admin/make-admin")
async def make_admin(data: AdminCreate, user: dict = Depends(get_current_user)):
    if not is_super_admin(user['id']):
        raise HTTPException(status_code=403, detail="Super Admin huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check if user exists
            cursor.execute("SELECT id FROM users WHERE id = %s", (data.userId,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Foydalanuvchi topilmadi")
            
            # Insert into admins
            cursor.execute("""
                INSERT INTO admins (id, dubbing_name, added_by, added_at, role)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET 
                    dubbing_name = EXCLUDED.dubbing_name,
                    role = 'admin'
            """, (data.userId, data.dubbingName, user['id'], datetime.now(), 'admin'))
            
            # Notification for new admin
            create_notification(data.userId, "Siz Admin Bo'ldingiz", f"🎉 Tabriklaymiz! Siz '{data.dubbingName}' dublyaj admini bo'ldingiz.", "system")
            
            return {"ok": True, "message": "Admin qo'shildi"}

@app.post("/admin/top-up")
async def admin_top_up(data: PaymentCreate, user: dict = Depends(get_current_user)):
    if not is_super_admin(user['id']):
        raise HTTPException(status_code=403, detail="Super Admin huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check user by ID (UUID), Short ID, or Email
            search_val = data.user_id.strip()
            
            query = "SELECT id, name FROM users WHERE id = %s OR short_id = %s OR email = %s"
            cursor.execute(query, (search_val, search_val, search_val))
                
            target_user = cursor.fetchone()
            if not target_user:
                raise HTTPException(status_code=404, detail="Foydalanuvchi topilmadi")
            
            target_id = target_user['id']
            
            # Update balance
            cursor.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (data.amount, target_id))
            
            # Record as completed payment
            pid = generate_id()
            cursor.execute("""
                INSERT INTO payments (id, user_id, amount, status, created_at, updated_at, payment_url)
                VALUES (%s, %s, %s, 'completed', %s, %s, 'admin_topup')
            """, (pid, target_id, data.amount, datetime.now(), datetime.now()))
            
            # Notification for balance top-up
            create_notification(target_id, "Balans To'ldirildi", f"💰 Sizning hisobingiz {data.amount} so'mga to'ldirildi.", "balance")
            
            # Record in admin_topups for auditing
            atid = generate_id()
            cursor.execute("""
                INSERT INTO admin_topups (id, admin_id, user_id, amount, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (atid, user['id'], target_id, data.amount, datetime.now()))
            
            logger.info(f"💰 Admin Balance Top-up: {data.amount} for user {target_id} ({target_user['name']}) by admin {user['id']}")
            return {"ok": True, "message": f"Balans to'ldirildi: {data.amount}", "user": target_user['name']}

@app.get("/admin/settings")
async def get_all_settings(user: dict = Depends(get_current_user)):
    if not is_super_admin(user["id"]):
        raise HTTPException(status_code=403, detail="Super Admin huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT key, value, description, updated_at FROM settings")
            return {"ok": True, "settings": cursor.fetchall()}

@app.post("/admin/settings")
async def update_setting(data: dict, user: dict = Depends(get_current_user)):
    if not is_super_admin(user["id"]):
        raise HTTPException(status_code=403, detail="Super Admin huquqi zarur")
    
    key = data.get("key")
    value = data.get("value")
    
    if not key or value is None:
        raise HTTPException(status_code=400, detail="Kalit va qiymat kiritilishi shart")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE settings SET value = %s, updated_at = CURRENT_TIMESTAMP WHERE key = %s",
                (str(value), key)
            )
            return {"ok": True, "message": f"Sozlama yangilandi: {key}"}

@app.get("/app/config")
async def get_app_config():
    """Public endpoint for app configuration (Test Mode, Theme, etc.)"""
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT key, value FROM settings WHERE key IN ('test_mode', 'seasonal_theme', 'maintenance_mode')")
            settings_list = cursor.fetchall()
            return {
                "ok": True, 
                "config": {s["key"]: s["value"] for s in settings_list}
            }

@app.get("/news")
async def get_news():
    """Get all news (CACHED for 5 minutes)"""
    cache_key = "news_list"
    current_time = time.time()
    
    # Check cache
    if cache_key in API_CACHE and API_CACHE_TTL.get(cache_key, 0) > current_time:
        logger.debug("⚡ Cache HIT: /news")
        return API_CACHE[cache_key]
    
    # Fetch from database
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM news ORDER BY created_at DESC LIMIT 50")
            news = cursor.fetchall()
            
            # Cache for 5 minutes
            API_CACHE[cache_key] = news
            API_CACHE_TTL[cache_key] = current_time + 300
            logger.debug("💾 Cache MISS: /news (cached for 5m)")
            
            return news

@app.get("/banners")
async def get_banners():
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM banners WHERE active = TRUE ORDER BY created_at DESC")
            banners = cursor.fetchall()
            
            # Format banner URLs to full URLs
            formatted_banners = []
            base_url = "https://backend123-rk6r.onrender.com"  # Your backend URL
            for banner in banners:
                banner_dict = dict(banner)
                # If image_url is relative, make it absolute
                if banner_dict.get('image_url') and not banner_dict['image_url'].startswith('http'):
                    banner_dict['image_url'] = f"{base_url}{banner_dict['image_url']}"
                formatted_banners.append(banner_dict)
            
            return {"ok": True, "banners": formatted_banners}

@app.post("/banner")
async def create_banner(banner: BannerCreate):
    if not is_super_admin(banner.addedBy):
        raise HTTPException(status_code=403, detail="Super Admin only")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            bid = generate_id()
            cursor.execute("""
                INSERT INTO banners (id, text, image_url, added_by, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (bid, banner.text, banner.imageUrl, banner.addedBy, datetime.now()))
            return {"ok": True, "id": bid}

@app.post("/ad")
async def create_ad(ad: AdCreate):
    with get_db() as conn:
        with conn.cursor() as cursor:
             # Check user balance (500 sum cost)
             AD_COST = 500
             cursor.execute("SELECT balance FROM users WHERE id = %s", (ad.userId,))
             user = cursor.fetchone()
             if not user or user['balance'] < AD_COST:
                 raise HTTPException(status_code=402, detail="Insufficient balance for ad")
             
             cursor.execute("UPDATE users SET balance = balance - %s WHERE id = %s", (AD_COST, ad.userId))
             
             aid = generate_id()
             cursor.execute("""
                INSERT INTO ads (id, title, image_url, user_id, created_at)
                VALUES (%s, %s, %s, %s, %s)
             """, (aid, ad.title, ad.imageUrl, ad.userId, datetime.now()))
             
             return {"ok": True, "id": aid}

@app.get("/ads")
async def get_ads():
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM ads WHERE active = TRUE")
            return {"ok": True, "ads": cursor.fetchall()}

# --- Shorts ---

@app.post("/shorts", status_code=201)
async def add_short(short: ShortCreate):
    with get_db() as conn:
        with conn.cursor() as cursor:
            short_id = generate_id()
            cursor.execute("""
                INSERT INTO shorts (id, video_url, description, anime_id, added_by, created_at, tg_message_id, tg_file_id, content_type)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (short_id, short.videoUrl, short.description, short.animeId, short.addedBy, datetime.now(), short.tg_message_id, short.tg_file_id, short.content_type))
            
            return {"ok": True, "id": short_id}

@app.get("/shorts")
async def get_shorts(limit: int = 20, offset: int = 0, user_id: Optional[str] = None):
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Fetch shorts with optional Anime Details, Likes count and if current user liked it
            cursor.execute("""
                SELECT 
                    s.*, 
                    a.title as anime_title, 
                    a.poster_url as anime_poster,
                    (SELECT COUNT(*) FROM shorts_likes WHERE short_id = s.id) as likes_count,
                    (SELECT COUNT(*) FROM shorts_comments WHERE short_id = s.id) as comments_count,
                    EXISTS(SELECT 1 FROM shorts_likes WHERE short_id = s.id AND user_id = %s) as is_liked
                FROM shorts s
                LEFT JOIN animes a ON s.anime_id = a.id
                ORDER BY s.created_at DESC
                LIMIT %s OFFSET %s
            """, (user_id, limit, offset))
            
            shorts = cursor.fetchall()
            
            results = []
            for s in shorts:
                sh = dict(s)
                sh['video_url'] = format_media_url(sh.get('video_url'))
                sh['anime_poster'] = format_media_url(sh.get('anime_poster'))
                
                if sh['created_at']:
                    sh['created_at'] = sh['created_at'].isoformat()
                results.append(sh)
            
            return {"ok": True, "shorts": results}

@app.post("/shorts/{short_id}/like")
async def toggle_short_like(short_id: str, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Check if already liked
            cursor.execute("SELECT 1 FROM shorts_likes WHERE short_id = %s AND user_id = %s", (short_id, user['id']))
            liked = cursor.fetchone()
            
            if liked:
                cursor.execute("DELETE FROM shorts_likes WHERE short_id = %s AND user_id = %s", (short_id, user['id']))
                is_liked = False
            else:
                cursor.execute("INSERT INTO shorts_likes (short_id, user_id) VALUES (%s, %s)", (short_id, user['id']))
                is_liked = True
            
            # Get updated like count
            cursor.execute("SELECT COUNT(*) as count FROM shorts_likes WHERE short_id = %s", (short_id,))
            likes_count = cursor.fetchone()['count']
            
            return {"ok": True, "is_liked": is_liked, "likes_count": likes_count}

@app.get("/shorts/{short_id}/comments")
async def get_short_comments(short_id: str):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT c.*, u.name as user_name, u.avatar_url 
                FROM shorts_comments c
                JOIN users u ON c.user_id = u.id
                WHERE c.short_id = %s
                ORDER BY c.created_at DESC
            """, (short_id,))
            comments = cursor.fetchall()
            
            results = []
            for c in comments:
                com = dict(c)
                if com['created_at']:
                    com['created_at'] = com['created_at'].isoformat()
                if com.get('avatar_url'):
                    com['avatar_url'] = format_media_url(com['avatar_url'])
                results.append(com)
                
            return {"ok": True, "comments": results}

@app.post("/shorts/{short_id}/comments")
async def add_short_comment(short_id: str, comment: CommentCreate, user: dict = Depends(get_current_user)):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cid = generate_id()
            cursor.execute("""
                INSERT INTO shorts_comments (id, short_id, user_id, text, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (cid, short_id, user['id'], comment.text, datetime.now()))
            
            return {"ok": True, "id": cid, "user_name": user['name']}

@app.delete("/shorts/{id}")
async def delete_short(id: str, user_id: str = Query(...)):
    if not is_admin(user_id):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM shorts WHERE id = %s", (id,))
            return {"ok": True}
        
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)


# --- App Ads (Hero Banners) ---

@app.get("/app-ads")
async def get_app_ads():
    """Get app ads (CACHED for 5 minutes)"""
    cache_key = "app_ads_list"
    current_time = time.time()
    
    # Check cache
    if cache_key in API_CACHE and API_CACHE_TTL.get(cache_key, 0) > current_time:
        logger.debug("⚡ Cache HIT: /app-ads")
        return API_CACHE[cache_key]
    
    # Fetch from database
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Join with animes to get default details if overrides are null
            cursor.execute("""
                SELECT 
                    ads.id, 
                    ads.anime_id, 
                    COALESCE(ads.image_url, a.poster_url) as image_url,
                    COALESCE(ads.title, a.title) as title,
                    COALESCE(ads.description, a.genre) as description,
                    ads.active,
                    a.genre,
                    a.is_premium
                FROM app_ads ads
                LEFT JOIN animes a ON ads.anime_id = a.id
                WHERE ads.active = TRUE
                ORDER BY ads.created_at DESC
            """)
            ads = cursor.fetchall()
            
            # Format URLs
            formatted_ads = []
            for ad in ads:
                a = dict(ad)
                a['image_url'] = format_media_url(a.get('image_url'))
                formatted_ads.append(a)

            # Cache for 5 minutes
            API_CACHE[cache_key] = formatted_ads
            API_CACHE_TTL[cache_key] = current_time + 300
            logger.debug("💾 Cache MISS: /app-ads (cached for 5m)")
            
            return formatted_ads

@app.post("/app-ads")
async def create_app_ad(ad: AppAdCreate, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin access required")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            ad_id = generate_id()
            cursor.execute("""
                INSERT INTO app_ads (id, anime_id, target_shorts_id, image_url, title, description)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (ad_id, ad.anime_id, ad.target_shorts_id, ad.image_url, ad.title, ad.description))
            # Invalidate cache
            if "app_ads_list" in API_CACHE:
                del API_CACHE["app_ads_list"]
            return {"ok": True, "id": ad_id}

@app.delete("/app-ads/{id}")
async def delete_app_ad(id: str, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin access required")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM app_ads WHERE id = %s", (id,))
            # Invalidate cache
            if "app_ads_list" in API_CACHE:
                del API_CACHE["app_ads_list"]
            return {"ok": True}

# --- Frontend Error Reporting ---

class FrontendError(BaseModel):
    error_type: str  # "api_error", "ui_error", "player_error", etc.
    error_message: str
    stack_trace: Optional[str] = None
    user_id: Optional[str] = None
    page: Optional[str] = None  # Which page/screen
    device_info: Optional[str] = None
    timestamp: Optional[str] = None

@app.post("/report-error")
async def report_frontend_error(error: FrontendError):
    """Receive error reports from Flutter frontend and notify admin via Telegram"""
    try:
        # Build detailed error message for admin
        error_msg = f"""
🔴 **FRONTEND ERROR REPORT**

**Type:** {error.error_type}
**Message:** {error.error_message}
**Page:** {error.page or 'Unknown'}
**User ID:** {error.user_id or 'Anonymous'}
**Device:** {error.device_info or 'Unknown'}
**Time:** {error.timestamp or datetime.now().isoformat()}

**Stack Trace:**
```
{error.stack_trace or 'No stack trace provided'}
```
"""
        
        # Send to admin via Telegram
        await send_error_to_admin(error_msg)
        
        # Log to backend
        logger.error(f"Frontend Error Report: {error.error_type} - {error.error_message}")
        
        return {"ok": True, "message": "Error reported successfully"}
    except Exception as e:
        logger.error(f"Failed to report frontend error: {e}")
        return {"ok": False, "message": "Failed to report error"}

# --- Video Ads ---

@app.get("/video-ads")
async def get_video_ads(active_only: bool = Query(True, alias="activeOnly")):
    with get_db() as conn:
        with conn.cursor() as cursor:
            if active_only:
                cursor.execute("SELECT * FROM video_ads WHERE active = TRUE ORDER BY created_at DESC")
            else:
                cursor.execute("SELECT * FROM video_ads ORDER BY created_at DESC")
            results = cursor.fetchall()
            formatted = []
            for ad in results:
                a = dict(ad)
                a['video_url'] = format_media_url(a.get('video_url'))
                formatted.append(a)
            return {"ok": True, "ads": formatted}

@app.post("/video-ads")
async def create_video_ad(ad: VideoAdCreate, current_user: dict = Depends(get_current_user)):
    if not is_admin(current_user['id']):
        raise HTTPException(status_code=403, detail="Admin huquqi kerak")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            ad_id = generate_id()
            cursor.execute("""
                INSERT INTO video_ads (id, video_url, title, tg_message_id, tg_file_id, content_type)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (ad_id, ad.video_url, ad.title, ad.tg_message_id, ad.tg_file_id, ad.content_type))
            return {"ok": True, "id": ad_id}

@app.delete("/video-ads/{ad_id}")
async def delete_video_ad(ad_id: str, current_user: dict = Depends(get_current_user)):
    if not is_admin(current_user['id']):
        raise HTTPException(status_code=403, detail="Admin huquqi kerak")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM video_ads WHERE id = %s", (ad_id,))
            return {"ok": True}

@app.post("/video-ads/{ad_id}/view")
async def increment_video_ad_view(ad_id: str):
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE video_ads SET views = views + 1 WHERE id = %s", (ad_id,))
            return {"ok": True}


# --- News ---

@app.get("/news")
async def get_news():
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT n.*, a.title as anime_title 
                FROM news n
                LEFT JOIN animes a ON n.target_anime_id = a.id
                ORDER BY n.created_at DESC
            """)
            news_list = cursor.fetchall()
            results = []
            for n in news_list:
                item = dict(n)
                item['image_url'] = format_media_url(item.get('image_url'))
                results.append(item)
            return results

@app.post("/news")
async def create_news(news: NewsCreate, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin access required")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            news_id = generate_id()
            cursor.execute("""
                INSERT INTO news (id, title, description, image_url, target_anime_id, news_type, target_shorts_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (news_id, news.title, news.description, news.image_url, news.target_anime_id, news.news_type, news.target_shorts_id))
            
            # Global Notification: New News
            create_notification(None, "Yangi Yangilik!", f"📰 {news.title}", "news", news.target_anime_id or news.target_shorts_id)
            
            # Invalidate cache
            if "news_list" in API_CACHE:
                del API_CACHE["news_list"]
            return {"ok": True, "id": news_id}

@app.delete("/news/{id}")
async def delete_news(id: str, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin access required")
        
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM news WHERE id = %s", (id,))
            # Invalidate cache
            if "news_list" in API_CACHE:
                del API_CACHE["news_list"]
            return {"ok": True}


# ==================== PROMO CODES ====================

class PromoCreate(BaseModel):
    code: str
    reward_type: str # 'cash' or 'anime_access'
    reward_value: str
    expiry_date: Optional[datetime] = None
    usage_limit: int = 0

@app.get("/admin/promo-codes")
async def get_all_promo_codes(current_user: dict = Depends(get_current_user)):
    if not is_admin(current_user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM promo_codes ORDER BY created_at DESC")
            promos = cursor.fetchall()
            for p in promos:
                if p['expiry_date']:
                    p['expiry_date'] = p['expiry_date'].isoformat()
                if p['created_at']:
                    p['created_at'] = p['created_at'].isoformat()
            return promos

@app.post("/admin/promo-codes")
async def create_promo_code(data: PromoCreate, current_user: dict = Depends(get_current_user)):
    if not is_admin(current_user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            pid = generate_id()
            cursor.execute("""
                INSERT INTO promo_codes (id, code, reward_type, reward_value, expiry_date, usage_limit)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (pid, data.code.upper(), data.reward_type, data.reward_value, data.expiry_date, data.usage_limit))
            return {"ok": True, "id": pid}

@app.delete("/admin/promo-codes/{promo_id}")
async def delete_promo_code(promo_id: str, current_user: dict = Depends(get_current_user)):
    if not is_admin(current_user['id']):
        raise HTTPException(status_code=403, detail="Ruxsat berilmagan")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM promo_codes WHERE id = %s", (promo_id,))
            return {"ok": True}

@app.post("/use-promo-code")
async def use_promo_code(request: Request, current_user: dict = Depends(get_current_user)):
    data = await request.json()
    code = data.get("code", "").upper().strip()
    
    if not code:
        raise HTTPException(status_code=400, detail="Kod kiritilmadi")
        
    with get_db() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SELECT * FROM promo_codes WHERE code = %s AND active = TRUE", (code,))
            promo = cursor.fetchone()
            
            if not promo:
                raise HTTPException(status_code=404, detail="Promokod topilmadi yoki faol emas")
                
            if promo['expiry_date'] and promo['expiry_date'] < datetime.now():
                raise HTTPException(status_code=400, detail="Promokod muddati tugagan")
                
            if promo['usage_limit'] > 0 and promo['used_count'] >= promo['usage_limit']:
                raise HTTPException(status_code=400, detail="Promokod ishlatilish soni tugagan")
                
            cursor.execute("SELECT id FROM promo_usages WHERE promo_id = %s AND user_id = %s", (promo['id'], current_user['id']))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Siz ushbu promokodni ishlatib bo'lgansiz")
                
            success_msg = ""
            if promo['reward_type'] == 'cash':
                amount = float(promo['reward_value'])
                cursor.execute("UPDATE users SET balance = balance + %s WHERE id = %s", (amount, current_user['id']))
                success_msg = f"Hisobingizga {amount} so'm qo'shildi!"
            elif promo['reward_type'] == 'anime_access':
                anime_id = promo['reward_value']
                cursor.execute("SELECT id FROM purchases WHERE user_id = %s AND anime_id = %s", (current_user['id'], anime_id))
                if cursor.fetchone():
                     raise HTTPException(status_code=400, detail="Ushbu anime sizda allaqachon bor")
                
                cursor.execute("SELECT price FROM animes WHERE id = %s", (anime_id,))
                anime = cursor.fetchone()
                if not anime:
                    raise HTTPException(status_code=404, detail="Anime topilmadi")
                    
                cursor.execute("""
                    INSERT INTO purchases (id, user_id, anime_id, price, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """, (generate_id(), current_user['id'], anime_id, anime['price'], datetime.now()))
                success_msg = "Tabriklaymiz! Animega ruxsat ochildi."
            else:
                raise HTTPException(status_code=400, detail="Noma'lum mukofot turi")
                
            cursor.execute("INSERT INTO promo_usages (id, promo_id, user_id) VALUES (%s, %s, %s)", (generate_id(), promo['id'], current_user['id']))
            cursor.execute("UPDATE promo_codes SET used_count = used_count + 1 WHERE id = %s", (promo['id'],))
            
            return {"ok": True, "message": success_msg}

async def upload_video_file_to_doodstream(file: UploadFile, remote_filename: str) -> bool:
    """UploadFile ni DoodStream ga yuklash, muvaffaqiyatli bo'lsa True qaytaradi"""
    async def file_sender():
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            yield chunk

    file_code = await upload_video_to_doodstream(
        file_sender(),
        remote_filename,
        file.content_type or "video/mp4"
    )
    return file_code is not None

@app.get("/notifications")
async def get_notifications(user: dict = Depends(get_current_user)):
    """Fetch notifications for current user (including global ones)"""
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Fetch personal + global (user_id IS NULL)
            cursor.execute("""
                SELECT * FROM notifications 
                WHERE user_id = %s OR user_id IS NULL 
                ORDER BY created_at DESC 
                LIMIT 50
            """, (user['id'],))
            notifications = cursor.fetchall()
            
            results = []
            for n in notifications:
                n_dict = dict(n)
                if n_dict['created_at']:
                    n_dict['created_at'] = n_dict['created_at'].isoformat()
                results.append(n_dict)
                
            return {"ok": True, "notifications": results}

# ==================== FAQ ENDPOINTS ====================

@app.get("/support/faq")
async def get_faqs():
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, question, answer FROM support_faqs ORDER BY id DESC")
            faqs = cursor.fetchall()
            return {"ok": True, "faqs": faqs}

@app.post("/admin/support/faq")
async def add_faq(data: FAQCreate, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin huquqi zarur")
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("INSERT INTO support_faqs (question, answer) VALUES (%s, %s) RETURNING id", 
                           (data.question, data.answer))
            faq_id = cursor.fetchone()['id']
            return {"ok": True, "id": faq_id}

@app.put("/admin/support/faq/{faq_id}")
async def update_faq(faq_id: int, data: FAQUpdate, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin huquqi zarur")
    with get_db() as conn:
        with conn.cursor() as cursor:
            if data.question:
                cursor.execute("UPDATE support_faqs SET question = %s WHERE id = %s", (data.question, faq_id))
            if data.answer:
                cursor.execute("UPDATE support_faqs SET answer = %s WHERE id = %s", (data.answer, faq_id))
            return {"ok": True}

@app.delete("/admin/support/faq/{faq_id}")
async def delete_faq(faq_id: int, user: dict = Depends(get_current_user)):
    if not is_admin(user['id']):
        raise HTTPException(status_code=403, detail="Admin huquqi zarur")
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM support_faqs WHERE id = %s", (faq_id,))
            return {"ok": True}

# ==================== FINANCE ADMIN ENDPOINTS ====================

@app.get("/admin/finance/stats")
async def get_finance_stats(user: dict = Depends(get_current_user)):
    if not is_finance_admin(user['id']):
        raise HTTPException(status_code=403, detail="Moliya admini huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Total Users
            cursor.execute("SELECT COUNT(*) as total FROM users")
            total_users = cursor.fetchone()['total']
            
            # Registered Users (last 30 days)
            cursor.execute("SELECT COUNT(*) as total FROM users WHERE created_at > %s", (datetime.now() - timedelta(days=30),))
            recent_users = cursor.fetchone()['total']
            
            # Total Balance (Current holdings)
            cursor.execute("SELECT SUM(balance) as total FROM users")
            total_balance = cursor.fetchone()['total'] or 0.0
            
            # Total Revenue (Admin Top-ups + Completed Payments)
            cursor.execute("SELECT SUM(amount) as total FROM admin_topups")
            total_admin_topups = cursor.fetchone()['total'] or 0.0
            
            cursor.execute("SELECT SUM(amount) as total FROM payments WHERE status = 'completed'")
            total_payments = cursor.fetchone()['total'] or 0.0
            
            total_revenue = total_admin_topups + total_payments
            
            # Downloads (Proxy for purchases)
            cursor.execute("SELECT COUNT(*) as total FROM purchases")
            total_purchases = cursor.fetchone()['total']
            
            return {
                "ok": True,
                "stats": {
                    "totalUsers": total_users,
                    "recentUsers": recent_users,
                    "totalBalance": total_balance,
                    "totalRevenue": total_revenue,
                    "totalPurchases": total_purchases
                }
            }

@app.get("/admin/finance/topups")
async def get_finance_topups(user: dict = Depends(get_current_user)):
    if not is_finance_admin(user['id']):
        raise HTTPException(status_code=403, detail="Moliya admini huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT t.*, a.name as admin_name, u.name as user_name, u.email as user_email
                FROM admin_topups t
                JOIN users a ON t.admin_id = a.id
                JOIN users u ON t.user_id = u.id
                ORDER BY t.created_at DESC
                LIMIT 100
            """)
            topups = cursor.fetchall()
            
            results = []
            for t in topups:
                t_dict = dict(t)
                if t_dict['created_at']:
                    t_dict['created_at'] = t_dict['created_at'].isoformat()
                results.append(t_dict)
                
            return {"ok": True, "topups": results}

@app.get("/admin/finance/user/{identifier}")
async def get_user_details(identifier: str, user: dict = Depends(get_current_user)):
    """Search user by email or short_id"""
    if not is_finance_admin(user['id']):
        raise HTTPException(status_code=403, detail="Moliya admini huquqi zarur")
    
    with get_db() as conn:
        with conn.cursor() as cursor:
            # Search by email OR short_id
            cursor.execute("SELECT id, name, email, password, plain_password, balance, short_id, created_at FROM users WHERE email = %s OR short_id = %s", (identifier, identifier))
            target_user = cursor.fetchone()
            
            if not target_user:
                raise HTTPException(status_code=404, detail="Foydalanuvchi topilmadi")
            
            res = dict(target_user)
            if res['created_at']:
                res['created_at'] = res['created_at'].isoformat()
                
            return {"ok": True, "user": res}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)
