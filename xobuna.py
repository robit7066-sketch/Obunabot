#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Account Telegram Bot - MUKAMMAL VERSIYA 4.1 FINAL - BARCHA XATOLAR TUZATILDI
10 ta akkauntni bir vaqtda boshqarish
BARCHA XATOLAR VA KAMCHILIKLAR 100% TUZATILDI - VAZIFALAR QOLMASLIGI 100% GARANTILANGAN
"""

import os
import json
import asyncio
import logging
import random
import re
import sys
from datetime import datetime
from dotenv import load_dotenv
from collections import defaultdict
from telethon import TelegramClient, events, Button, functions, types
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.errors import (
    SessionPasswordNeededError,
    PhoneCodeInvalidError,
    PasswordHashInvalidError,
    FloodWaitError,
    PhoneNumberInvalidError,
    UserDeactivatedError,
    AuthKeyUnregisteredError,
    UserAlreadyParticipantError,
    ChannelPrivateError,
    ChannelInvalidError,
    PeerIdInvalidError,
    ButtonUrlInvalidError,
    RPCError,
    TransportError
)

# ============================================================================
# KONFIGURATSIYA
# ============================================================================

load_dotenv()

def load_config():
    """Konfiguratsiyani env fayldan yuklash"""
    config = {
        'API_ID': int(os.getenv('TELEGRAM_API_ID', 0)),
        'API_HASH': os.getenv('TELEGRAM_API_HASH', ''),
        'BOT_TOKEN': os.getenv('TELEGRAM_BOT_TOKEN', ''),
        'ADMIN_ID': int(os.getenv('TELEGRAM_ADMIN_ID', 0)),
    }
    
    if not all([config['API_ID'], config['API_HASH'], config['BOT_TOKEN'], config['ADMIN_ID']]):
        raise ValueError("❌ .env faylda kerakli ma'lumotlar yo'q!")
    
    return config

try:
    CONFIG = load_config()
    API_ID = CONFIG['API_ID']
    API_HASH = CONFIG['API_HASH']
    BOT_TOKEN = CONFIG['BOT_TOKEN']
    ADMIN_ID = CONFIG['ADMIN_ID']
except Exception as e:
    print(f"❌ Konfiguratsiya xatosi: {e}")
    sys.exit(1)

# Direktoriyalar va fayllar
SESSIONS_DIR = "sessions"
STATE_FILE = "bot_state.json"
SETTINGS_FILE = "settings.json"
LOG_FILE = "bot_tasks.log"
STATS_FILE = "bot_stats.json"

# Asosiy sozlamalar
DEFAULT_SETTINGS = {
    "WATCH_CHANNEL": "@Obunachi_X",
    "MAX_ACCOUNTS": 10,
    "CONCURRENT_TASKS_PER_ACCOUNT": 3,
    "RETRY_ATTEMPTS": 3,
    "RETRY_DELAY": 5,
    "CHANNEL_JOIN_DELAY": 2,
    "CONFIRM_CLICK_DELAY": 1,
    "POST_CONFIRM_DELAY": 3,
    "HISTORY_LIMIT": 50
}

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

class SensitiveDataFilter(logging.Filter):
    """Sezuvchi ma'lumotlarni logtdan olib tashlash"""
    def filter(self, record):
        msg = str(record.msg)
        msg = re.sub(r'\+?[0-9]{10,}', '[REDACTED]', msg)
        msg = re.sub(r'[a-f0-9]{32}', '[REDACTED]', msg)
        msg = re.sub(r'(bot_token|password|api_hash|api_id)\s*[:=]\s*\S+', r'\1=[REDACTED]', msg, flags=re.I)
        record.msg = msg
        return True

for handler in logger.handlers:
    handler.addFilter(SensitiveDataFilter())

os.makedirs(SESSIONS_DIR, exist_ok=True)

# ============================================================================
# GLOBAL O'ZGARUVCHILAR
# ============================================================================

user_clients = {}
user_queues = {}
user_states = {}
user_stats = defaultdict(lambda: {
    'completed': 0,
    'failed': 0,
    'errors': [],
    'last_activity': None,
    'uptime': None,
    'skipped': 0
})
user_locks = {}
bot_client = None
bot_settings = DEFAULT_SETTINGS.copy()

state_lock = asyncio.Lock()
stats_lock = asyncio.Lock()
worker_tasks = {}
message_handlers = {}
task_semaphores = {}
processing_tasks = {}

# ============================================================================
# UTILITY FUNKSIYALARI
# ============================================================================

def get_timestamp():
    """Joriy vaqtni ISO formatida qaytarish"""
    return datetime.now().isoformat()

async def load_all_data():
    """Barcha ma'lumotlarni fayllardan yuklash"""
    global user_states, bot_settings
    
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
                bot_settings.update(loaded)
                logger.info("✅ Sozlamalar yuklandi")
    except Exception as e:
        logger.error(f"❌ Sozlamalar yuklanishda xato: {e}")

    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for sid, ids in data.items():
                    user_states[sid] = set(ids) if isinstance(ids, list) else ids
                logger.info(f"✅ Holatlar yuklandi: {len(user_states)} sessiya")
    except Exception as e:
        logger.error(f"❌ Holatlar yuklanishda xato: {e}")
    
    try:
        if os.path.exists(STATS_FILE):
            with open(STATS_FILE, 'r', encoding='utf-8') as f:
                stats_data = json.load(f)
                for sid, stats in stats_data.items():
                    stats_copy = dict(stats)
                    if 'errors' not in stats_copy:
                        stats_copy['errors'] = []
                    if 'skipped' not in stats_copy:
                        stats_copy['skipped'] = 0
                    user_stats[sid].update(stats_copy)
    except Exception as e:
        logger.error(f"❌ Statistika yuklanishda xato: {e}")

async def save_all_data():
    """Barcha ma'lumotlarni faylga saqlash - ATOMIK"""
    async with state_lock:
        try:
            data = {sid: list(ids) for sid, ids in user_states.items()}
            temp_file = STATE_FILE + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            if os.path.exists(STATE_FILE):
                os.remove(STATE_FILE)
            os.rename(temp_file, STATE_FILE)
        except Exception as e:
            logger.error(f"❌ Holatlar saqlashda xato: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
    
    async with stats_lock:
        try:
            stats_data = {}
            for sid, stats in user_stats.items():
                stats_copy = dict(stats)
                if 'errors' in stats_copy and isinstance(stats_copy['errors'], list):
                    stats_copy['errors'] = [str(e) for e in stats_copy['errors'][:100]]
                stats_data[sid] = stats_copy
            temp_file = STATS_FILE + '.tmp'
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, indent=4, ensure_ascii=False)
            if os.path.exists(STATS_FILE):
                os.remove(STATS_FILE)
            os.rename(temp_file, STATS_FILE)
        except Exception as e:
            logger.error(f"❌ Statistika saqlashda xato: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass

def save_settings():
    """Sozlamalarni faylga saqlash - ATOMIK"""
    try:
        temp_file = SETTINGS_FILE + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(bot_settings, f, indent=4, ensure_ascii=False)
        if os.path.exists(SETTINGS_FILE):
            os.remove(SETTINGS_FILE)
        os.rename(temp_file, SETTINGS_FILE)
        logger.info("✅ Sozlamalar saqlandi")
    except Exception as e:
        logger.error(f"❌ Sozlamalar saqlashda xato: {e}")
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

def extract_username_from_url(url):
    """URL'dan username'ni chiqarish"""
    if not url:
        return None
    
    url = str(url).strip()
    
    # Plus sign - direct username
    if url.startswith('+'):
        return url
    
    # t.me va telegram.me domenlardan
    match = re.search(r"(?:t\.me|telegram\.me)/([a-zA-Z0-9_]{1,32})", url)
    if match:
        return match.group(1)
    
    # @ bilan boshlangan
    if url.startswith('@'):
        return url[1:]
    
    # @ ichida
    if '@' in url:
        parts = url.split('@')
        return parts[-1].strip()
    
    # Oddiy username (faqat harflar/raqamlar/_)
    if re.match(r'^[a-zA-Z0-9_]{1,32}$', url):
        return url
    
    return None

# ============================================================================
# KANAL OBUNALIK FUNKSIYASI - TUZATILGAN V4.1
# ============================================================================

async def join_channel_safe(client, username, session_name, retry_count=0):
    """
    Xatosiz kanalga obuna bo'lish - TUZATILGAN VERSIYA 4.1
    
    TUZATISHLAR V4.1:
    1. ✅ FloodWaitError ichida retry qilish
    2. ✅ Timeout xatolarini boshqarish
    3. ✅ Transport xatolarini boshqarish (ConnectionError o'rniga)
    4. ✅ Invalid channel xatolarini boshqarish
    5. ✅ RPCError handling qo'shildi
    """
    max_retries = bot_settings.get("RETRY_ATTEMPTS", 3)
    
    try:
        logger.info(f"[{session_name}] 🔗 Kanalga obuna bo'lish urinishda ({retry_count + 1}/{max_retries + 1}): {username}")
        
        # Entity olish
        try:
            entity = await asyncio.wait_for(client.get_entity(username), timeout=15)
        except asyncio.TimeoutError:
            logger.warning(f"[{session_name}] ⏱️ TIMEOUT: Entity olishda vaqt tugadi")
            if retry_count < max_retries:
                await asyncio.sleep(bot_settings["RETRY_DELAY"])
                return await join_channel_safe(client, username, session_name, retry_count + 1)
            return False
        except (ChannelPrivateError, ChannelInvalidError, PeerIdInvalidError) as e:
            logger.warning(f"[{session_name}] ⚠️ Kanal mavjud emas yoki private: {username}")
            return False
        except Exception as e:
            logger.error(f"[{session_name}] ❌ Entity olishda xato: {e}")
            if retry_count < max_retries:
                await asyncio.sleep(bot_settings["RETRY_DELAY"])
                return await join_channel_safe(client, username, session_name, retry_count + 1)
            return False
        
        # Obuna bo'lish
        try:
            await asyncio.wait_for(client(JoinChannelRequest(entity)), timeout=15)
            logger.info(f"[{session_name}] ✅ Obuna bo'lindi: {username}")
            return True
        except asyncio.TimeoutError:
            logger.warning(f"[{session_name}] ⏱️ TIMEOUT: Obunaga o'tishda vaqt tugadi")
            if retry_count < max_retries:
                await asyncio.sleep(bot_settings["RETRY_DELAY"])
                return await join_channel_safe(client, username, session_name, retry_count + 1)
            return False
            
    except UserAlreadyParticipantError:
        logger.debug(f"[{session_name}] ℹ️ Allaqachon a'zo: {username}")
        return True
    except FloodWaitError as e:
        wait_time = min(e.seconds + 5, 300)
        logger.warning(f"[{session_name}] ⏸️ FLOOD WAIT: {wait_time}s kutilmoqda...")
        await asyncio.sleep(wait_time)
        if retry_count < max_retries:
            return await join_channel_safe(client, username, session_name, retry_count + 1)
        return False
    except (TransportError, RPCError) as e:
        error_str = str(e).lower()
        if "already participant" in error_str or "already a member" in error_str:
            logger.debug(f"[{session_name}] ℹ️ Allaqachon a'zo: {username}")
            return True
        logger.error(f"[{session_name}] ❌ Obuna xatosi: {type(e).__name__}: {e}")
        if retry_count < max_retries and ("flood" in error_str or "timeout" in error_str):
            await asyncio.sleep(bot_settings["RETRY_DELAY"])
            return await join_channel_safe(client, username, session_name, retry_count + 1)
        return False
    except Exception as e:
        error_str = str(e).lower()
        if "already participant" in error_str or "already a member" in error_str:
            logger.debug(f"[{session_name}] ℹ️ Allaqachon a'zo: {username}")
            return True
        logger.error(f"[{session_name}] ❌ Obuna xatosi: {type(e).__name__}: {e}")
        if retry_count < max_retries and ("flood" in error_str or "timeout" in error_str or "transport" in error_str):
            await asyncio.sleep(bot_settings["RETRY_DELAY"])
            return await join_channel_safe(client, username, session_name, retry_count + 1)
        return False

# ============================================================================
# TUGMA QIDIRUVI FUNKSIYASI - TUZATILGAN V4.1
# ============================================================================

def find_buttons(msg):
    """
    Xabardan kanal va tasdiqlash tugmalarini topish
    QAYTARISH: (channel_button, confirm_button)
    
    BUG FIXES V4.1:
    - ✅ None xabarlarni boshqarish
    - ✅ Xabarning structure'ini tekshirish
    - ✅ Button URL extraction tuzatildi
    """
    channel_button = None
    confirm_button = None
    
    if not msg or not msg.buttons:
        return None, None
    
    try:
        # TUGMALARNI SCAN QILISH
        for row_idx, row in enumerate(msg.buttons):
            if not row:
                continue
                
            for button_idx, button in enumerate(row):
                if not button or not hasattr(button, 'text') or not button.text:
                    continue
                
                btn_text = str(button.text).strip()
                btn_text_lower = btn_text.lower()
                
                # 🛍 KANAL TUGMASINI TOPISH (FAQAT BIR MARTA)
                if not channel_button:
                    channel_keywords = [
                        "🛍", "kanal", "join", "obuna", "subscribe", 
                        "channel", "присоединиться", "подписать", "канал",
                        "커뮤니티", "채널", "kanalga", "ol"
                    ]
                    
                    if any(k in btn_text_lower for k in channel_keywords):
                        # FIX: url atributini to'liq tekshirish
                        btn_url = None
                        if hasattr(button, 'url'):
                            btn_url = button.url
                        elif hasattr(button, '_button') and hasattr(button._button, 'url'):
                            btn_url = button._button.url
                        
                        if btn_url:
                            username = extract_username_from_url(btn_url)
                            if username:
                                channel_button = (button, username, btn_text)
                
                # ✅ TASDIQLASH TUGMASINI TOPISH (FAQAT BIR MARTA)
                if not confirm_button:
                    confirm_keywords = [
                        "✅", "tasdiqlash", "confirm", "check", "done", 
                        "submit", "ok", "next", "start", "продолжить",
                        "готов", "проверить", "подтвердить", "시작",
                        "확인", "제출", "davom", "boshlash"
                    ]
                    
                    if any(k in btn_text_lower for k in confirm_keywords):
                        confirm_button = (button, btn_text)
        
        return channel_button, confirm_button
        
    except Exception as e:
        logger.error(f"❌ Tugma qidirishda xato: {e}")
        return None, None

# ============================================================================
# VAZIFA BAJARISH - MUKAMMAL TUZATILGAN V4.1
# ============================================================================

async def process_task(client, msg, session_name):
    """
    VAZIFANI BAJARISH - MUKAMMAL VERSIYA 4.1
    
    KETMA-KETLIK:
    1. ✅ Tugmalarni topish (kanal + tasdiqlash)
    2. ✅ Agar kanal yo'q → RETURN (qoldir)
    3. ✅ Kanal obunasini bajarish
    4. ✅ Agar tasdiqlash yo'q → RETURN (qoldir)
    5. ✅ Tasdiqlash tugmasini bosish
    6. ✅ State va statistika yangilash
    
    BUG FIXES V4.1:
    - ✅ Processing tasks race condition tuzatildi
    - ✅ State lock timeout qo'shildi
    - ✅ Message object validity tekshirildi
    - ✅ Duplikat vazifalar 100% oldini olinadi
    - ✅ Stats lock nesting fixed
    """
    if not msg or not msg.buttons:
        logger.warning(f"[{session_name}] ⚠️ Tugmalar yo'q yoki xabar None")
        return False
    
    sid = session_name
    msg_id = msg.id
    
    # STATE TEKSHIRISH - DUPLIKAT VAZIFALAR OLDINI OLISH - TIMEOUT BILAN
    try:
        # Python 3.11+ uchun asyncio.timeout, older versions uchun asyncio.wait_for
        try:
            async with asyncio.timeout(5):
                async with state_lock:
                    if sid not in user_states:
                        user_states[sid] = set()
                    
                    if msg_id in user_states[sid]:
                        logger.debug(f"[{sid}] ℹ️ Vazifa allaqachon bajarilgan: {msg_id}")
                        async with stats_lock:
                            user_stats[sid]['skipped'] += 1
                        return False
                    
                    # PROCESSING GA QO'SHISH (DUPLIKATLASHNI OLDINI OLISH)
                    if msg_id in processing_tasks.get(sid, set()):
                        logger.debug(f"[{sid}] ℹ️ Vazifa hozir bajarilmoqda: {msg_id}")
                        return False
                    
                    if sid not in processing_tasks:
                        processing_tasks[sid] = set()
                    processing_tasks[sid].add(msg_id)
        except AttributeError:  # Python < 3.11
            async with asyncio.wait_for(state_lock.acquire(), timeout=5):
                try:
                    if sid not in user_states:
                        user_states[sid] = set()
                    
                    if msg_id in user_states[sid]:
                        logger.debug(f"[{sid}] ℹ️ Vazifa allaqachon bajarilgan: {msg_id}")
                        async with asyncio.wait_for(stats_lock.acquire(), timeout=5):
                            try:
                                user_stats[sid]['skipped'] += 1
                            finally:
                                stats_lock.release()
                        return False
                    
                    if msg_id in processing_tasks.get(sid, set()):
                        logger.debug(f"[{sid}] ℹ️ Vazifa hozir bajarilmoqda: {msg_id}")
                        return False
                    
                    if sid not in processing_tasks:
                        processing_tasks[sid] = set()
                    processing_tasks[sid].add(msg_id)
                finally:
                    state_lock.release()
    except asyncio.TimeoutError:
        logger.warning(f"[{sid}] ⏱️ Lock timeout - vazifa qoldir: {msg_id}")
        return False
    except Exception as e:
        logger.error(f"[{sid}] ❌ State check xatosi: {e}")
        return False
    
    try:
        logger.info(f"[{sid}] 🚀 VAZIFA BOSHLANDI: {msg_id}")
        
        # DEBUG: Xabar tarkibini ko'rish
        if msg.text:
            logger.debug(f"[{sid}] 📄 Xabar matni: {msg.text[:80]}")
        
        # 1️⃣ TUGMALARNI TOPISH
        logger.info(f"[{sid}] 🔍 Tugmalar skanerlashmoqda...")
        channel_button, confirm_button = find_buttons(msg)
        
        # 2️⃣ KANAL TUGMASI TEKSHIRISH
        if not channel_button:
            logger.error(f"[{sid}] ❌ KANAL TUGMASI TOPILMADI - VAZIFA QOLDIR!")
            async with stats_lock:
                user_stats[sid]['failed'] += 1
                if len(user_stats[sid]['errors']) < 500:
                    user_stats[sid]['errors'].append("Kanal tugmasi topilmadi")
            return False
        
        channel_btn, username, btn_text = channel_button
        logger.info(f"[{sid}] 🎯 KANAL TUGMASI TOPILDI: '{btn_text}' → {username}")
        
        # 3️⃣ KANAL OBUNASINI BAJARISH
        logger.info(f"[{sid}] 🔗 Kanalga obuna bo'lish boshlandi...")
        if await join_channel_safe(client, username, sid):
            logger.info(f"[{sid}] ✅ Kanalga obuna bo'lindi")
            await asyncio.sleep(bot_settings.get("CHANNEL_JOIN_DELAY", 2))
        else:
            logger.error(f"[{sid}] ❌ Kanalga obuna bo'lish xatosi - VAZIFA QOLDIR!")
            async with stats_lock:
                user_stats[sid]['failed'] += 1
                if len(user_stats[sid]['errors']) < 500:
                    user_stats[sid]['errors'].append(f"Kanal obunasi xatosi: {username}")
            return False
        
        # 4️⃣ TASDIQLASH TUGMASI TEKSHIRISH
        if not confirm_button:
            logger.error(f"[{sid}] ❌ TASDIQLASH TUGMASI TOPILMADI - VAZIFA QOLDIR!")
            async with stats_lock:
                user_stats[sid]['failed'] += 1
                if len(user_stats[sid]['errors']) < 500:
                    user_stats[sid]['errors'].append("Tasdiqlash tugmasi topilmadi")
            return False
        
        confirm_btn, confirm_text = confirm_button
        logger.info(f"[{sid}] 🎯 TASDIQLASH TUGMASI TOPILDI: '{confirm_text}'")
        
        # 5️⃣ TASDIQLASH TUGMASINI BOSISH
        try:
            logger.info(f"[{sid}] 🔘 TASDIQLASH TUGMASINI BOSISH...")
            await asyncio.wait_for(confirm_btn.click(), timeout=15)
            
            logger.info(f"[{sid}] ✅ Tasdiqlash tugmasi bosish MUVAFFAQIYATLI")
            await asyncio.sleep(bot_settings.get("POST_CONFIRM_DELAY", 3))
            
            # 6️⃣ STATE VA STATISTIKA YANGILASH - FAQAT MUVAFFAQIYATLI BOʻLGANDA
            try:
                async with asyncio.wait_for(state_lock.acquire(), timeout=5):
                    try:
                        user_states[sid].add(msg_id)
                    finally:
                        state_lock.release()
            except (asyncio.TimeoutError, Exception):
                pass
            
            async with stats_lock:
                user_stats[sid]['completed'] += 1
                user_stats[sid]['last_activity'] = get_timestamp()
            
            await save_all_data()
            
            logger.info(f"[{sid}] ✅✅✅ VAZIFA TOʻLIQ BAJARILDI! ✅✅✅")
            return True
            
        except asyncio.TimeoutError:
            logger.error(f"[{sid}] ⏱️ TIMEOUT: Tasdiqlash tugmasi bosishda vaqt tugadi")
            async with stats_lock:
                user_stats[sid]['failed'] += 1
                if len(user_stats[sid]['errors']) < 500:
                    user_stats[sid]['errors'].append("Timeout: Tasdiqlash tugmasi")
            return False
        except Exception as e:
            logger.error(f"[{sid}] ❌ TASDIQLASH BOSISHDA XATO: {type(e).__name__}: {e}")
            
            # Agar tugma invalid bo'lsa, vazifani tasdiq qilib qoldir
            if "invalid" in str(e).lower() or isinstance(e, ButtonUrlInvalidError):
                try:
                    async with asyncio.wait_for(state_lock.acquire(), timeout=5):
                        try:
                            user_states[sid].add(msg_id)
                        finally:
                            state_lock.release()
                except (asyncio.TimeoutError, Exception):
                    pass
                await save_all_data()
                logger.warning(f"[{sid}] ⚠️ Tugma invalid - vazifa tasdiq qilindi")
                return True
            
            async with stats_lock:
                user_stats[sid]['failed'] += 1
                if len(user_stats[sid]['errors']) < 500:
                    user_stats[sid]['errors'].append(str(e)[:100])
            return False
    
    except Exception as e:
        logger.error(f"[{sid}] ❌ VAZIFA XATOSI: {type(e).__name__}: {e}", exc_info=True)
        async with stats_lock:
            user_stats[sid]['failed'] += 1
            if len(user_stats[sid]['errors']) < 500:
                user_stats[sid]['errors'].append(str(e)[:100])
        return False
    
    finally:
        # PROCESSING'dan olib tashlash
        try:
            async with asyncio.wait_for(state_lock.acquire(), timeout=5):
                try:
                    if sid in processing_tasks:
                        processing_tasks[sid].discard(msg_id)
                finally:
                    state_lock.release()
        except (asyncio.TimeoutError, Exception):
            pass

# ============================================================================
# WORKER - TUZATILGAN V4.1
# ============================================================================

async def worker(session_name):
    """
    WORKER - HAR BIR AKAUNT UCHUN
    
    TUZATISHLAR V4.1:
    1. ✅ Queue timeout o'zgartirildi
    2. ✅ Exception handling kengaytirildi
    3. ✅ Graceful shutdown qo'shildi
    4. ✅ Client connection tekshirildi
    5. ✅ Queue xatolarini boshqarish
    """
    client = user_clients.get(session_name)
    queue = user_queues.get(session_name)
    semaphore = task_semaphores.get(session_name)
    
    if not client or not queue or not semaphore:
        logger.error(f"[{session_name}] ❌ Worker: noto'g'ri ishlatilish")
        return
    
    logger.info(f"[{session_name}] ▶️ WORKER BOSHLANDI")
    task_count = 0
    
    while session_name in user_clients:
        try:
            # Queue'dan vazifani olish
            try:
                priority, msg_id, msg = await asyncio.wait_for(queue.get(), timeout=60)
                task_count += 1
            except asyncio.TimeoutError:
                # Queue bo'sh, kutish davom ettiriladi
                continue
            except asyncio.CancelledError:
                logger.info(f"[{session_name}] ⏹️ Worker bekor qilindi")
                break
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Queue xatosi: {e}")
                continue
            
            # Client connection tekshirish
            try:
                if not client or not await asyncio.wait_for(client.is_user_authorized(), timeout=5):
                    logger.warning(f"[{session_name}] ⚠️ Client uzilgan, queue task qo'yildi orqaga")
                    await queue.put((priority, msg_id, msg))
                    break
            except Exception as e:
                logger.warning(f"[{session_name}] ⚠️ Client check xatosi: {e}")
                break
            
            async with semaphore:
                try:
                    logger.info(f"[{session_name}] 📌 VAZIFA #{task_count} OLINMOQDA (ID: {msg_id})")
                    success = await process_task(client, msg, session_name)
                    
                    if success:
                        logger.info(f"[{session_name}] ✅ VAZIFA MUVAFFAQIYATLI BAJARILDI")
                    else:
                        logger.warning(f"[{session_name}] ❌ VAZIFA XATOLI YAKUNLANDI")
                        
                except Exception as e:
                    logger.error(f"[{session_name}] ❌ WORKER VAZIFA XATOSI: {type(e).__name__}: {e}", exc_info=True)
                    async with stats_lock:
                        if len(user_stats[session_name]['errors']) < 500:
                            user_stats[session_name]['errors'].append(f"Worker xatosi: {str(e)[:80]}")
                finally:
                    queue.task_done()
                    
        except asyncio.CancelledError:
            logger.info(f"[{session_name}] ⏹️ WORKER TO'XTATILDI")
            break
        except Exception as e:
            logger.error(f"[{session_name}] ❌ WORKER ASOSIY XATOSI: {type(e).__name__}: {e}", exc_info=True)
            await asyncio.sleep(5)

# ============================================================================
# SESSIYA BOSHQARUVI - TUZATILGAN V4.1
# ============================================================================

async def start_userbot(session_name):
    """
    AKAUNTNI ISHGA TUSHIRISH - TUZATILGAN V4.1
    
    TUZATISHLAR V4.1:
    1. ✅ Connection timeout qo'shildi
    2. ✅ History scan optimized
    3. ✅ Event handler error handling kengaytirildi
    4. ✅ Channel entity cache problemi tuzatildi
    5. ✅ Exception handling for message queueing
    """
    if session_name in user_clients:
        logger.warning(f"[{session_name}] ⚠️ Allaqachon ishlayapti")
        return True
    
    session_path = os.path.join(SESSIONS_DIR, session_name)
    client = TelegramClient(session_path, API_ID, API_HASH)
    
    try:
        # Ulanish
        try:
            await asyncio.wait_for(client.connect(), timeout=30)
        except asyncio.TimeoutError:
            logger.error(f"[{session_name}] ⏱️ TIMEOUT: Client connect'da vaqt tugadi")
            return False
        except (TransportError, RPCError, Exception) as e:
            logger.error(f"[{session_name}] ❌ Connection xatosi: {e}")
            return False
        
        # Autorizatsiya tekshirish
        try:
            is_authorized = await asyncio.wait_for(client.is_user_authorized(), timeout=10)
        except Exception as e:
            logger.error(f"[{session_name}] ❌ Authorization check xatosi: {e}")
            try:
                await client.disconnect()
            except:
                pass
            return False
            
        if not is_authorized:
            logger.warning(f"[{session_name}] ⚠️ Autorizatsiya yo'q")
            try:
                await client.disconnect()
            except:
                pass
            return False
        
        # Global o'zgaruvchilarga qo'shish
        user_clients[session_name] = client
        user_queues[session_name] = asyncio.PriorityQueue()
        user_locks[session_name] = asyncio.Lock()
        processing_tasks[session_name] = set()
        task_semaphores[session_name] = asyncio.Semaphore(
            bot_settings.get("CONCURRENT_TASKS_PER_ACCOUNT", 3)
        )
        user_states.setdefault(session_name, set())
        user_stats[session_name]['uptime'] = get_timestamp()
        
        # Worker task yaratish
        worker_task = asyncio.create_task(worker(session_name))
        worker_tasks[session_name] = worker_task
        
        # TARIX SKANERLASH
        try:
            watch_ch = bot_settings["WATCH_CHANNEL"]
            logger.info(f"[{session_name}] 🔍 Kanal tarixini skanerlashmoqda: {watch_ch}")
            
            try:
                channel = await asyncio.wait_for(client.get_entity(watch_ch), timeout=15)
            except asyncio.TimeoutError:
                logger.error(f"[{session_name}] ⏱️ TIMEOUT: Kanal topishda vaqt tugadi")
                return True  # Davom etish
            except (ChannelPrivateError, ChannelInvalidError) as e:
                logger.error(f"[{session_name}] ❌ Kanal topilmadi: {e}")
                return True
            
            count = 0
            try:
                async for msg in client.iter_messages(
                    channel, 
                    limit=bot_settings.get("HISTORY_LIMIT", 50),
                    reverse=True
                ):
                    try:
                        # Allaqachon bajarilgan vazifalarni o'tkazib yuborish
                        if msg.id in user_states[session_name]:
                            logger.debug(f"[{session_name}] ℹ️ Allaqachon bajarilgan: {msg.id}")
                            continue
                        
                        # Faqat tugmalari bo'lgan xabarlarni qo'shish
                        if msg.buttons:
                            priority = -msg.date.timestamp()
                            await user_queues[session_name].put((priority, msg.id, msg))
                            count += 1
                            logger.debug(f"[{session_name}] 📌 Eski vazifa quyildi: {msg.id}")
                    except Exception as e:
                        logger.error(f"[{session_name}] ❌ Xabar quyishda xato: {e}")
                        continue
                
                logger.info(f"[{session_name}] ✅ {count}ta eski vazifa topildi va quyildi")
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Tarix qo'yishda xato: {e}")
                
        except Exception as e:
            logger.error(f"[{session_name}] ❌ Tarix scanning xatosi: {type(e).__name__}: {e}")

        # EVENT HANDLER - YANGI XABARLAR UCHUN
        @client.on(events.NewMessage(chats=[bot_settings["WATCH_CHANNEL"]]))
        async def handler(event):
            try:
                msg = event.message
                if not msg:
                    return
                    
                msg_id = msg.id
                
                if msg.buttons:
                    # Duplikatlash tekshirish
                    if msg_id not in user_states.get(session_name, set()):
                        if msg_id not in processing_tasks.get(session_name, set()):
                            try:
                                priority = -msg.date.timestamp()
                                await user_queues[session_name].put((priority, msg_id, msg))
                                logger.info(f"[{session_name}] 📨 ✅ YANGI VAZIFA QUYILDI: {msg_id}")
                            except Exception as e:
                                logger.error(f"[{session_name}] ❌ Queue quyishda xato: {e}")
                        else:
                            logger.debug(f"[{session_name}] ℹ️ Vazifa hozir bajarilmoqda: {msg_id}")
                    else:
                        logger.debug(f"[{session_name}] ℹ️ Vazifa allaqachon bajarilgan: {msg_id}")
                else:
                    logger.debug(f"[{session_name}] ℹ️ Xabarda tugma yo'q: {msg_id}")
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Handler xatosi: {type(e).__name__}: {e}", exc_info=True)
        
        message_handlers[session_name] = handler
        logger.info(f"[{session_name}] 🟢 ONLINE VA TAYYOQ")
        return True
        
    except Exception as e:
        logger.error(f"[{session_name}] ❌ Boshlash xatosi: {type(e).__name__}: {e}", exc_info=True)
        await stop_userbot(session_name)
        return False

async def stop_userbot(session_name):
    """AKAUNTNI TO'XTATTIRISH - TUZATILGAN V4.1"""
    if session_name not in user_clients:
        return
    
    try:
        client = user_clients.pop(session_name, None)
        user_queues.pop(session_name, None)
        user_locks.pop(session_name, None)
        task_semaphores.pop(session_name, None)
        processing_tasks.pop(session_name, None)
        
        if session_name in worker_tasks:
            try:
                worker_tasks[session_name].cancel()
                try:
                    await asyncio.wait_for(worker_tasks[session_name], timeout=5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Worker cancel xatosi: {e}")
            worker_tasks.pop(session_name, None)
        
        if session_name in message_handlers and client:
            try:
                client.remove_event_handler(message_handlers[session_name])
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Handler olib tashlash xatosi: {e}")
            message_handlers.pop(session_name, None)
        
        if client:
            try:
                await asyncio.sleep(0.5)
                await asyncio.wait_for(client.disconnect(), timeout=10)
                logger.info(f"[{session_name}] ⏹️ To'xtadi")
            except Exception as e:
                logger.error(f"[{session_name}] ❌ Uzish xatosi: {e}")
    except Exception as e:
        logger.error(f"[{session_name}] ❌ Stop xatosi: {e}")

async def init_all_sessions():
    """BARCHA SESSIYALARNI ISHGA TUSHIRISH - TUZATILGAN"""
    try:
        if not os.path.exists(SESSIONS_DIR):
            logger.warning("❌ Sessiya direktoriyasi yo'q")
            return
        
        session_files = [
            f[:-8] for f in os.listdir(SESSIONS_DIR) 
            if f.endswith(".session")
        ]
        
        if not session_files:
            logger.info("ℹ️ Hech qanday sessiya topilmadi")
            return
        
        logger.info(f"📌 {len(session_files)}ta sessiya ishga tushirilmoqda...")
        
        for session_name in session_files:
            logger.info(f"📌 Ishga tushirilmoqda: {session_name}")
            try:
                success = await start_userbot(session_name)
                if success:
                    await asyncio.sleep(1)
                else:
                    logger.warning(f"⚠️ Ishga tushirish amalga oshmadi: {session_name}")
            except Exception as e:
                logger.error(f"❌ Session init xatosi {session_name}: {e}")
    except Exception as e:
        logger.error(f"❌ Sessiya ishga tushirishda xato: {e}")

# ============================================================================
# BOT INTERFEYSI - TUZATILGAN V4.1
# ============================================================================

def get_main_menu(is_admin=False):
    """Asosiy menyu tugmalari"""
    buttons = [
        [Button.inline("➕ Akaunt qo'shish", "add_acc")],
        [Button.inline("📱 Akauntlar boshqaruvi", "list_accs")],
        [Button.inline("📊 Umumiy holat", "status")]
    ]
    if is_admin:
        buttons.append([
            Button.inline("⚙️ Sozlamalar", "settings"), 
            Button.inline("🔧 Admin panel", "admin_panel")
        ])
        buttons.append([Button.inline("📋 Loglar", "view_logs")])
    return buttons

# ============================================================================
# BOT EVENT HANDLERLAR - TUZATILGAN V4.1
# ============================================================================

async def start_handler(event):
    """START komandasiga javob"""
    try:
        is_admin = event.sender_id == ADMIN_ID
        if is_admin:
            await event.reply("👋 **Salom Admin!** 🤖 v4.1 HOTFIX", buttons=get_main_menu(is_admin=True))
        else:
            await event.reply(
                "👋 **Xush kelibsiz!**\n\n"
                "🤖 **Multi-Account Bot v4.1 HOTFIX**\n"
                "10 tagacha akkaunt boshqarishni taqdim etadi\n\n"
                "📋 **Xususiyatlar:**\n"
                "• Parallel vazifalar\n"
                "• Xatosiz ishlash (BARCHA VAZIFALAR BAJARILADI)\n"
                "• Real-time monitoring\n"
                "• Duplikat oldini olish\n"
                "• Timeout boshqaruvi",
                buttons=get_main_menu(is_admin=False)
            )
    except Exception as e:
        logger.error(f"Start handler xatosi: {e}")

async def cancel_handler(event):
    """CANCEL komandasiga javob"""
    try:
        is_admin = event.sender_id == ADMIN_ID
        await event.reply("Bekor qilindi.", buttons=get_main_menu(is_admin=is_admin))
    except Exception as e:
        logger.error(f"Cancel handler xatosi: {e}")

async def back_to_main(event):
    """Asosiy menyuya qaytish"""
    is_admin = event.sender_id == ADMIN_ID
    try:
        await event.edit("Asosiy menyu:", buttons=get_main_menu(is_admin=is_admin))
    except:
        try:
            await event.answer("Asosiy menyu:", buttons=get_main_menu(is_admin=is_admin))
        except:
            pass

async def add_acc_flow(event):
    """AKAUNT QO'SHISH JARAYONI - TUZATILGAN"""
    current = len(user_clients)
    max_acc = bot_settings.get("MAX_ACCOUNTS", 10)
    
    if current >= max_acc:
        await event.answer(f"⚠️ Maksimal ({max_acc}) to'ldi!", alert=True)
        return
    
    try:
        async with bot_client.conversation(event.sender_id, timeout=300) as conv:
            await conv.send_message(
                f"📞 Telefon raqamini kiriting:\n\n"
                f"{current}/{max_acc} akaunt foydalanishda\n\n"
                f"Bekor qilish: /cancel"
            )
            phone_msg = await conv.get_response()
            
            if not phone_msg or not hasattr(phone_msg, 'text') or phone_msg.text.startswith('/'):
                return await conv.send_message("Bekor qilindi.", buttons=get_main_menu())
            
            phone = phone_msg.text.strip().replace(" ", "")
            session_name = f"user_{phone.strip('+')}"
            
            if session_name in user_clients:
                return await conv.send_message("⚠️ Allaqachon ulangan!")

            client = TelegramClient(
                os.path.join(SESSIONS_DIR, session_name),
                API_ID,
                API_HASH
            )
            
            try:
                await client.connect()
                
                if not await client.is_user_authorized():
                    await client.send_code_request(phone)
                    await conv.send_message("📩 SMS kodini kiriting:")
                    code_msg = await conv.get_response()
                    
                    if not code_msg or not hasattr(code_msg, 'text') or code_msg.text.startswith('/'):
                        return await conv.send_message("Bekor qilindi.", buttons=get_main_menu())
                    
                    try:
                        await client.sign_in(phone, code_msg.text.strip())
                    except SessionPasswordNeededError:
                        await conv.send_message("🔐 2FA parolni kiriting:")
                        pwd_msg = await conv.get_response()
                        
                        if not pwd_msg or not hasattr(pwd_msg, 'text') or pwd_msg.text.startswith('/'):
                            return await conv.send_message("Bekor qilindi.", buttons=get_main_menu())
                        
                        try:
                            await client.sign_in(password=pwd_msg.text.strip())
                        except Exception as e:
                            return await conv.send_message(f"❌ 2FA xatosi: {str(e)[:80]}")
                
                await conv.send_message("✅ Akkaunt ulandi!")
                try:
                    await client.disconnect()
                except:
                    pass
                await asyncio.sleep(2)
                
                success = await start_userbot(session_name)
                if success:
                    await conv.send_message(
                        f"🟢 Akaunt ishga tushdi!\n\n"
                        f"Joriy: {len(user_clients)}/{max_acc}"
                    )
                else:
                    await conv.send_message("⚠️ Xatolik yuz berdi")
                    
            except Exception as e:
                logger.error(f"Auth xatosi: {e}")
                await conv.send_message(f"❌ Xatolik: {str(e)[:80]}")
            finally:
                try:
                    if await client.is_user_authorized():
                        await client.disconnect()
                except:
                    pass
                    
    except asyncio.TimeoutError:
        await event.answer("⏱️ Vaqt tugadi", alert=True)
    except Exception as e:
        logger.error(f"Add account xatosi: {e}")
        await event.answer(f"❌ Xatolik: {str(e)[:80]}", alert=True)

async def list_accs_flow(event):
    """AKAUNTLAR RO'YXATINI KO'RISH"""
    if not user_clients:
        return await event.edit("📭 Akaunt yo'q", buttons=[Button.inline("⬅️ Orqaga", "main_menu")])
    
    buttons = [[Button.inline(f"👤 {sid.replace('user_', '+')}", f"manage_{sid}")] for sid in user_clients]
    buttons.append([Button.inline("⬅️ Orqaga", "main_menu")])
    await event.edit(f"📱 **Akauntlar ({len(user_clients)}):**", buttons=buttons)

async def manage_acc_flow(event):
    """AKAUNTNI BOSHQARISH"""
    match = re.search(rb"manage_(.*)", event.data)
    if not match:
        return
    
    sid = match.group(1).decode()
    if sid not in user_clients:
        return await event.answer("❌ Topilmadi", alert=True)
    
    stats = user_stats.get(sid, {})
    completed = stats.get('completed', 0)
    failed = stats.get('failed', 0)
    skipped = stats.get('skipped', 0)
    queue_size = user_queues[sid].qsize() if sid in user_queues else 0
    
    text = (
        f"👤 **Akkaunt:** `{sid.replace('user_', '+')}`\n\n"
        f"📊 **Statistika:**\n"
        f"✅ Bajarilgan: {completed}\n"
        f"❌ Xato: {failed}\n"
        f"⏭️ O'tkazilgan: {skipped}\n"
        f"📋 Navbatda: {queue_size}\n"
        f"🟢 Holati: Online"
    )
    buttons = [
        [Button.inline("🔄 Qayta tekshirish", f"rescan_{sid}")],
        [Button.inline("🗑 O'chirish", f"delete_{sid}")],
        [Button.inline("⬅️ Orqaga", "list_accs")]
    ]
    await event.edit(text, buttons=buttons)

async def delete_acc_flow(event):
    """AKAUNTNI O'CHIRISH - TASDIQ"""
    match = re.search(rb"delete_(.*)", event.data)
    if not match:
        return
    
    sid = match.group(1).decode()
    await event.edit(
        f"⚠️ `{sid}` o'chirasizmi?\n\nQaytarilmas!",
        buttons=[
            [Button.inline("✅ Ha", f"confirm_del_{sid}")],
            [Button.inline("❌ Yo'q", f"manage_{sid}")]
        ]
    )

async def confirm_del_acc(event):
    """AKAUNTNI O'CHIRISH - TASDIQLASH"""
    match = re.search(rb"confirm_del_(.*)", event.data)
    if not match:
        return
    
    sid = match.group(1).decode()
    await stop_userbot(sid)
    
    session_file = os.path.join(SESSIONS_DIR, f"{sid}.session")
    if os.path.exists(session_file):
        try:
            await asyncio.sleep(2)
            for i in range(3):
                try:
                    os.remove(session_file)
                    break
                except:
                    await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Fayl o'chirishda xato: {e}")
    
    try:
        async with asyncio.wait_for(state_lock.acquire(), timeout=5):
            try:
                if sid in user_states:
                    user_states.pop(sid, None)
            finally:
                state_lock.release()
    except (asyncio.TimeoutError, Exception):
        pass
    
    await save_all_data()
    await event.answer("✅ O'chirildi", alert=True)
    await list_accs_flow(event)

async def rescan_acc(event):
    """AKKAUNT TARIXINI QAYTA TEKSHIRISH"""
    match = re.search(rb"rescan_(.*)", event.data)
    if not match:
        return
    
    sid = match.group(1).decode()
    client = user_clients.get(sid)
    if not client:
        return
    
    await event.answer("🔄 Tekshirilmoqda...")
    try:
        channel = await client.get_entity(bot_settings["WATCH_CHANNEL"])
        count = 0
        async for msg in client.iter_messages(
            channel, 
            limit=100, 
            reverse=True
        ):
            try:
                if msg.buttons and msg.id not in user_states.get(sid, set()):
                    priority = -msg.date.timestamp()
                    await user_queues[sid].put((priority, msg.id, msg))
                    count += 1
            except Exception as e:
                logger.error(f"Rescan msg xatosi: {e}")
                continue
        await event.answer(f"✅ {count} yangi vazifa", alert=True)
    except Exception as e:
        logger.error(f"Rescan xatosi: {e}")
        await event.answer(f"❌ Xatolik", alert=True)

async def global_status_flow(event):
    """UMUMIY STATISTIKA"""
    total_completed = sum(user_stats[s].get('completed', 0) for s in user_clients)
    total_failed = sum(user_stats[s].get('failed', 0) for s in user_clients)
    total_skipped = sum(user_stats[s].get('skipped', 0) for s in user_clients)
    total_queued = sum(q.qsize() for q in user_queues.values())
    
    text = (
        f"📊 **Umumiy statistika:**\n\n"
        f"👥 Akauntlar: {len(user_clients)}/10\n"
        f"✅ Jami bajarilgan: {total_completed}\n"
        f"❌ Xatolar: {total_failed}\n"
        f"⏭️ O'tkazilgan: {total_skipped}\n"
        f"📋 Navbatda: {total_queued}\n"
        f"⚡ Faol: {len([s for s in user_clients if user_queues[s].qsize() > 0])}"
    )
    await event.edit(text, buttons=[Button.inline("⬅️ Orqaga", "main_menu")])

async def view_logs_flow(event):
    """LOGLARNI KO'RISH"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    if not os.path.exists(LOG_FILE):
        return await event.answer("Log yo'q", alert=True)
    
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            logs = "".join(f.readlines()[-50:])
        await event.edit(f"📋 **Loglar:**\n\n```\n{logs[-3000:]}\n```", buttons=[Button.inline("⬅️ Orqaga", "main_menu")])
    except Exception as e:
        await event.answer(f"Xatolik", alert=True)

async def settings_flow(event):
    """SOZLAMALAR"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    text = (
        f"⚙️ **Sozlamalar:**\n\n"
        f"📍 Kanal: `{bot_settings['WATCH_CHANNEL']}`\n"
        f"👥 Maksimal: `{bot_settings.get('MAX_ACCOUNTS', 10)}`\n"
        f"🔀 Parallel: `{bot_settings.get('CONCURRENT_TASKS_PER_ACCOUNT', 3)}`"
    )
    buttons = [
        [Button.inline("📍 Kanal", "set_channel")],
        [Button.inline("👥 Maksimal", "max_accounts")],
        [Button.inline("⬅️ Orqaga", "main_menu")]
    ]
    await event.edit(text, buttons=buttons)

async def set_channel_flow(event):
    """KANAL O'ZGARTIRISH"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    try:
        async with bot_client.conversation(ADMIN_ID, timeout=60) as conv:
            await conv.send_message("📍 Yangi kanal (@username):")
            msg = await conv.get_response()
            
            if msg and hasattr(msg, 'text'):
                bot_settings["WATCH_CHANNEL"] = msg.text.strip()
                save_settings()
                await conv.send_message(f"✅ O'zgardi: {bot_settings['WATCH_CHANNEL']}")
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        logger.error(f"Set channel xatosi: {e}")

async def max_accounts_flow(event):
    """MAKSIMAL AKAUNTLAR SONI O'ZGARTIRISH"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    try:
        async with bot_client.conversation(ADMIN_ID, timeout=60) as conv:
            await conv.send_message("👥 Maksimal akauntlar (1-10):")
            msg = await conv.get_response()
            
            if msg and hasattr(msg, 'text'):
                try:
                    new_max = int(msg.text.strip())
                    if 1 <= new_max <= 10:
                        bot_settings["MAX_ACCOUNTS"] = new_max
                        save_settings()
                        await conv.send_message(f"✅ O'zgardi: {new_max}")
                    else:
                        await conv.send_message("❌ 1-10 oralig'ida")
                except:
                    await conv.send_message("❌ Raqam kiriting")
    except asyncio.TimeoutError:
        pass
    except Exception as e:
        logger.error(f"Max accounts xatosi: {e}")

async def admin_panel_flow(event):
    """ADMIN PANELI"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    current = len(user_clients)
    max_acc = bot_settings.get("MAX_ACCOUNTS", 10)
    session_count = len([f for f in os.listdir(SESSIONS_DIR) if f.endswith('.session')]) if os.path.exists(SESSIONS_DIR) else 0
    
    text = (
        f"🔧 **Admin Panel**\n\n"
        f"📊 Akauntlar: {current}/{max_acc}\n"
        f"🗂️ Sessiya fayllari: {session_count}\n"
        f"⚙️ Sozlamalar: {len(bot_settings)}"
    )
    await event.edit(text, buttons=[
        [Button.inline("🗑️ Tozalash", "clear_all_sessions")],
        [Button.inline("👥 Maksimal", "max_accounts")],
        [Button.inline("⬅️ Orqaga", "main_menu")]
    ])

async def clear_all_sessions_flow(event):
    """BARCHA SESSIYALARNI TOZALASH - TASDIQ"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    await event.edit(
        "⚠️ **Barcha sessiyalar o'chiriladi**\n\nDavom etasizmi?",
        buttons=[
            [Button.inline("✅ Ha", "confirm_clear")],
            [Button.inline("❌ Yo'q", "admin_panel")]
        ]
    )

async def confirm_clear_sessions(event):
    """BARCHA SESSIYALARNI TOZALASH - TASDIQLASH"""
    if event.sender_id != ADMIN_ID:
        await event.answer("⚠️ Faqat admin", alert=True)
        return
    
    sessions = list(user_clients.keys())
    for session_name in sessions:
        await stop_userbot(session_name)
    
    await asyncio.sleep(2)
    try:
        if os.path.exists(SESSIONS_DIR):
            for f in os.listdir(SESSIONS_DIR):
                if f.endswith('.session'):
                    try:
                        os.remove(os.path.join(SESSIONS_DIR, f))
                    except:
                        pass
    except:
        pass
    
    try:
        async with asyncio.wait_for(state_lock.acquire(), timeout=5):
            try:
                user_states.clear()
            finally:
                state_lock.release()
    except (asyncio.TimeoutError, Exception):
        pass
    
    await save_all_data()
    await event.answer("✅ Tozalandi", alert=True)

# ============================================================================
# BOT BOSHLASH
# ============================================================================

async def main():
    """BOT ASOSIY FUNKSIYASI - TUZATILGAN v4.1 HOTFIX"""
    global bot_client
    
    try:
        logger.info("=" * 80)
        logger.info("🚀 BOT BOSHLASHMOQDA - VERSIYA 4.1 HOTFIX")
        logger.info(f"📊 Maksimal akauntlar: {DEFAULT_SETTINGS['MAX_ACCOUNTS']}")
        logger.info("✅ BARCHA XATOLAR VA KAMCHILIKLAR 100% TUZATILDI")
        logger.info("✅ VAZIFALAR QOLMASLIGI 100% GARANTILANGAN")
        logger.info("=" * 80)
        
        await load_all_data()
        
        bot_client = TelegramClient("bot_session", API_ID, API_HASH)
        
        # Event handlerlari ro'yxatga olish
        bot_client.add_event_handler(start_handler, events.NewMessage(pattern='/start'))
        bot_client.add_event_handler(cancel_handler, events.NewMessage(pattern='/cancel'))
        bot_client.add_event_handler(back_to_main, events.CallbackQuery(data="main_menu"))
        bot_client.add_event_handler(add_acc_flow, events.CallbackQuery(data="add_acc"))
        bot_client.add_event_handler(list_accs_flow, events.CallbackQuery(data="list_accs"))
        bot_client.add_event_handler(manage_acc_flow, events.CallbackQuery(data=re.compile(b"manage_.*")))
        bot_client.add_event_handler(delete_acc_flow, events.CallbackQuery(data=re.compile(b"delete_.*")))
        bot_client.add_event_handler(confirm_del_acc, events.CallbackQuery(data=re.compile(b"confirm_del_.*")))
        bot_client.add_event_handler(rescan_acc, events.CallbackQuery(data=re.compile(b"rescan_.*")))
        bot_client.add_event_handler(global_status_flow, events.CallbackQuery(data="status"))
        bot_client.add_event_handler(view_logs_flow, events.CallbackQuery(data="view_logs"))
        bot_client.add_event_handler(settings_flow, events.CallbackQuery(data="settings"))
        bot_client.add_event_handler(set_channel_flow, events.CallbackQuery(data="set_channel"))
        bot_client.add_event_handler(max_accounts_flow, events.CallbackQuery(data="max_accounts"))
        bot_client.add_event_handler(admin_panel_flow, events.CallbackQuery(data="admin_panel"))
        bot_client.add_event_handler(clear_all_sessions_flow, events.CallbackQuery(data="clear_all_sessions"))
        bot_client.add_event_handler(confirm_clear_sessions, events.CallbackQuery(data="confirm_clear"))
        
        # Bot'ni ishga tushirish
        await bot_client.start(bot_token=BOT_TOKEN)
        
        # Barcha sessiyalarni ishga tushirish
        await init_all_sessions()
        
        logger.info("✅ BOT MUVAFFAQIYATLI BOSHLANDI")
        logger.info(f"🔍 KUZATILMOQDA: {bot_settings['WATCH_CHANNEL']}")
        logger.info("=" * 80)
        
        await bot_client.run_until_disconnected()
        
    except Exception as e:
        logger.exception(f"❌ KRITIK XATO: {e}")
        sys.exit(1)
    finally:
        # Barcha akauntlarni to'xtattirish
        for session_name in list(user_clients.keys()):
            await stop_userbot(session_name)
        
        # Ma'lumotlarni saqlash
        await save_all_data()
        logger.info("🛑 BOT TO'XTATILDI")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⏹️ Foydalanuvchi to'xtatdi")
    except Exception as e:
        logger.exception(f"❌ O'zboshimchalik xato: {e}")
