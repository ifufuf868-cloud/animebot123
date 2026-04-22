
import firebase_admin
from firebase_admin import credentials, messaging
import logging
from typing import Optional

logger = logging.getLogger("CosmosTV.Notifications")

# Global app instance
_firebase_app = None

def init_firebase():
    """Initialize Firebase Admin SDK"""
    global _firebase_app
    try:
        # Check if already initialized
        if _firebase_app:
            return

        # Attempt to load credentials
        cred = credentials.Certificate("firebase_credentials.json")
        _firebase_app = firebase_admin.initialize_app(cred)
        logger.info("✅ Firebase Admin SDK Initialized")
    except Exception as e:
        logger.warning(f"⚠️ Firebase Init Failed (Notification will be disabled): {e}")

def send_push_notification(
    topic: str,
    title: str,
    body: str,
    image_url: Optional[str] = None,
    data: Optional[dict] = None
) -> bool:
    """Send FCM notification to a topic"""
    if not _firebase_app:
        logger.warning("🚫 Firebase not initialized, skipping notification.")
        return False
        
    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title=title,
                body=body,
                image=image_url
            ),
            data=data or {},
            topic=topic,
        )
        response = messaging.send(message)
        logger.info(f"📲 Notification sent to '{topic}': {response}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send notification: {e}")
        return False
