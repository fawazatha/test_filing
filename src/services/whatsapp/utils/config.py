from dotenv import load_dotenv

import os 
import logging


load_dotenv(override=True)


logging.basicConfig(
    # filename='wa_workflow.log',  
    level=logging.INFO, 
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
    )

LOGGER = logging.getLogger(__name__)
LOGGER.info("Init Global Variable")

ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_FROM_NUMBER=os.getenv("TWILIO_FROM_NUMBER")
TEMPLATE_SID=os.getenv("TEMPLATE_SID")


SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')
