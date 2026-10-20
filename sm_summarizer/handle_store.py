"""Platform handle library store.
Manages pre-configured official handles and analyst-created custom handles for social media platforms.
Primary: shaktidb_social (PostgreSQL table sm_platform_handles)
Fallback: Local JSON file (data/platform_handles.json)
"""

import json
import logging
import os
import threading
import psycopg2
from sm_summarizer.summarizer_service import DB_CONFIG

logger = logging.getLogger("sm_summarizer.handle_store")

_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(_DATA_DIR, exist_ok=True)
_HANDLES_FILE = os.path.join(_DATA_DIR, "platform_handles.json")
_file_lock = threading.Lock()

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sm_platform_handles (
    id            SERIAL PRIMARY KEY,
    platform      TEXT NOT NULL,
    handle        TEXT NOT NULL,
    display_name  TEXT,
    category      TEXT DEFAULT 'General',
    is_custom     BOOLEAN DEFAULT false,
    added_by      BIGINT DEFAULT 1,
    created_at    TIMESTAMPTZ DEFAULT now(),
    UNIQUE(platform, handle)
);
"""

# Default curated seed handles extracted from platform database tables
DEFAULT_SEED_HANDLES = [
    {
        "platform": "baidu",
        "handle": "解放军",
        "display_name": "解放军",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "中国军队",
        "display_name": "中国军队",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "西部战区",
        "display_name": "西部战区",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "东部战区",
        "display_name": "东部战区",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "南部战区",
        "display_name": "南部战区",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "新疆军区",
        "display_name": "新疆军区",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "西藏军区",
        "display_name": "西藏军区",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "空军演习",
        "display_name": "空军演习",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "火箭军",
        "display_name": "火箭军",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "军事演习",
        "display_name": "军事演习",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "边境建设",
        "display_name": "边境建设",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "中印边境",
        "display_name": "中印边境",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "People's Liberation Army",
        "display_name": "People's Liberation Army",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA",
        "display_name": "PLA",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA training",
        "display_name": "PLA training",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA exercise",
        "display_name": "PLA exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA drill",
        "display_name": "PLA drill",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA modernization",
        "display_name": "PLA modernization",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA combat readiness",
        "display_name": "PLA combat readiness",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA field forces",
        "display_name": "PLA field forces",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA leadership",
        "display_name": "PLA leadership",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA purge",
        "display_name": "PLA purge",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Western Theater Command",
        "display_name": "Western Theater Command",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "WTC",
        "display_name": "WTC",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA Western Theater Command",
        "display_name": "PLA Western Theater Command",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Eastern Theater Command",
        "display_name": "Eastern Theater Command",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Southern Theater Command",
        "display_name": "Southern Theater Command",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "76th Group Army",
        "display_name": "76th Group Army",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "77th Group Army",
        "display_name": "77th Group Army",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "76 GA",
        "display_name": "76 GA",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "77 GA",
        "display_name": "77 GA",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Group Army PLA",
        "display_name": "Group Army PLA",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Xinjiang Military District",
        "display_name": "Xinjiang Military District",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "XMD",
        "display_name": "XMD",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Tibet Military District",
        "display_name": "Tibet Military District",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "TMD",
        "display_name": "TMD",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Xinjiang military exercise",
        "display_name": "Xinjiang military exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Tibet military exercise",
        "display_name": "Tibet military exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Tibet infrastructure",
        "display_name": "Tibet infrastructure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Xinjiang infrastructure",
        "display_name": "Xinjiang infrastructure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLAAF",
        "display_name": "PLAAF",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "People's Liberation Army Air Force",
        "display_name": "People's Liberation Army Air Force",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLAAF training",
        "display_name": "PLAAF training",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLAAF exercise",
        "display_name": "PLAAF exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese military aviation",
        "display_name": "Chinese military aviation",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "J-20 fighter",
        "display_name": "J-20 fighter",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "J-16 fighter",
        "display_name": "J-16 fighter",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "H-6 bomber",
        "display_name": "H-6 bomber",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLARF",
        "display_name": "PLARF",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "People's Liberation Army Rocket Force",
        "display_name": "People's Liberation Army Rocket Force",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Rocket Force exercise",
        "display_name": "Rocket Force exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese missile launch",
        "display_name": "Chinese missile launch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese missile test",
        "display_name": "Chinese missile test",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "DF-17",
        "display_name": "DF-17",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "DF-21",
        "display_name": "DF-21",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "DF-26",
        "display_name": "DF-26",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "hypersonic missile China",
        "display_name": "hypersonic missile China",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "LAC standoff",
        "display_name": "LAC standoff",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Eastern Ladakh",
        "display_name": "Eastern Ladakh",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Arunachal border",
        "display_name": "Arunachal border",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Doklam",
        "display_name": "Doklam",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Depsang",
        "display_name": "Depsang",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Demchok",
        "display_name": "Demchok",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "India China military",
        "display_name": "India China military",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "India China border infrastructure",
        "display_name": "India China border infrastructure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "china border",
        "display_name": "china border",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "सीमा तनाव",
        "display_name": "सीमा तनाव",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "border tension",
        "display_name": "border tension",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "military movement",
        "display_name": "military movement",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "defence news",
        "display_name": "defence news",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "security alert",
        "display_name": "security alert",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLAN",
        "display_name": "PLAN",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "People's Liberation Army Navy",
        "display_name": "People's Liberation Army Navy",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese naval exercise",
        "display_name": "Chinese naval exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese warship",
        "display_name": "Chinese warship",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese aircraft carrier",
        "display_name": "Chinese aircraft carrier",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Type 055 destroyer",
        "display_name": "Type 055 destroyer",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Type 052D destroyer",
        "display_name": "Type 052D destroyer",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "South China Sea exercise",
        "display_name": "South China Sea exercise",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Indian Ocean deployment",
        "display_name": "Indian Ocean deployment",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese presence Indian Ocean",
        "display_name": "Chinese presence Indian Ocean",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China maritime infrastructure",
        "display_name": "China maritime infrastructure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese naval base",
        "display_name": "Chinese naval base",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "String of Pearls",
        "display_name": "String of Pearls",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Maritime Silk Road",
        "display_name": "Maritime Silk Road",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "IOR security",
        "display_name": "IOR security",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Gwadar Port",
        "display_name": "Gwadar Port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Hambantota Port",
        "display_name": "Hambantota Port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Kyaukpyu Port",
        "display_name": "Kyaukpyu Port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chittagong Port",
        "display_name": "Chittagong Port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Bagamoyo Port",
        "display_name": "Bagamoyo Port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Djibouti naval base",
        "display_name": "Djibouti naval base",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China port project",
        "display_name": "China port project",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China overseas port",
        "display_name": "China overseas port",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "BRI maritime project",
        "display_name": "BRI maritime project",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Belt and Road Initiative",
        "display_name": "Belt and Road Initiative",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China border infrastructure",
        "display_name": "China border infrastructure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China border village",
        "display_name": "China border village",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China military road",
        "display_name": "China military road",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China military airfield",
        "display_name": "China military airfield",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Tibet railway",
        "display_name": "Tibet railway",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Sichuan Tibet Railway",
        "display_name": "Sichuan Tibet Railway",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Brahmaputra dam",
        "display_name": "Brahmaputra dam",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Yarlung Tsangpo dam",
        "display_name": "Yarlung Tsangpo dam",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment Pakistan",
        "display_name": "Chinese investment Pakistan",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment Nepal",
        "display_name": "Chinese investment Nepal",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment Bangladesh",
        "display_name": "Chinese investment Bangladesh",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment ASEAN",
        "display_name": "Chinese investment ASEAN",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment Africa",
        "display_name": "Chinese investment Africa",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment Europe",
        "display_name": "Chinese investment Europe",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese investment South America",
        "display_name": "Chinese investment South America",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China infrastructure project",
        "display_name": "China infrastructure project",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China energy security",
        "display_name": "China energy security",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China energy transition",
        "display_name": "China energy transition",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China fossil fuel imports",
        "display_name": "China fossil fuel imports",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China renewable energy",
        "display_name": "China renewable energy",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China LNG imports",
        "display_name": "China LNG imports",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China oil imports",
        "display_name": "China oil imports",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China coal production",
        "display_name": "China coal production",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Zhang Youxia",
        "display_name": "Zhang Youxia",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Central Military Commission",
        "display_name": "Central Military Commission",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CMC China",
        "display_name": "CMC China",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese military corruption",
        "display_name": "Chinese military corruption",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "satellite imagery China",
        "display_name": "satellite imagery China",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "military satellite imagery",
        "display_name": "military satellite imagery",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China military base",
        "display_name": "China military base",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China border map",
        "display_name": "China border map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "India China border map",
        "display_name": "India China border map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "PLA infographic",
        "display_name": "PLA infographic",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "military infographic",
        "display_name": "military infographic",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "strategic map",
        "display_name": "strategic map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Indian Ocean map",
        "display_name": "Indian Ocean map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "South China Sea map",
        "display_name": "South China Sea map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Belt and Road map",
        "display_name": "Belt and Road map",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Long March 7A",
        "display_name": "Long March 7A",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Long March 7",
        "display_name": "Long March 7",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CZ-7A",
        "display_name": "CZ-7A",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Wenchang launch",
        "display_name": "Wenchang launch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Wenchang Space Launch Site",
        "display_name": "Wenchang Space Launch Site",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CASC",
        "display_name": "CASC",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Aerospace Science and Technology Corporation",
        "display_name": "China Aerospace Science and Technology Corporation",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "ChinaSat 4B",
        "display_name": "ChinaSat 4B",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Zhongxing 4B",
        "display_name": "Zhongxing 4B",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "YF-100 engine",
        "display_name": "YF-100 engine",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "rocket explosion China",
        "display_name": "rocket explosion China",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "launch failure China",
        "display_name": "launch failure China",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chinese rocket anomaly",
        "display_name": "Chinese rocket anomaly",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "flight termination system",
        "display_name": "flight termination system",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China orbital launch failure",
        "display_name": "China orbital launch failure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chang'e 7",
        "display_name": "Chang'e 7",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CALT",
        "display_name": "CALT",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Academy of Launch Vehicle Technology",
        "display_name": "China Academy of Launch Vehicle Technology",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Jiuquan launch",
        "display_name": "Jiuquan launch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Xichang launch",
        "display_name": "Xichang launch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Taiyuan launch",
        "display_name": "Taiyuan launch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China National Radio launch report",
        "display_name": "China National Radio launch report",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Long March explosion",
        "display_name": "Long March explosion",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "LongMarch7A",
        "display_name": "LongMarch7A",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "LongMarch",
        "display_name": "LongMarch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "ChinaSat4B",
        "display_name": "ChinaSat4B",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Zhongxing4B",
        "display_name": "Zhongxing4B",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Wenchang",
        "display_name": "Wenchang",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "RocketLaunch",
        "display_name": "RocketLaunch",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "LaunchFailure",
        "display_name": "LaunchFailure",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "SpaceNews",
        "display_name": "SpaceNews",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "ChinaSpace",
        "display_name": "ChinaSpace",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Chang'e7",
        "display_name": "Chang'e7",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "YF100",
        "display_name": "YF100",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "china myanmar",
        "display_name": "china myanmar",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "china-myanmar",
        "display_name": "china-myanmar",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Myanmar Economic Corridor",
        "display_name": "China Myanmar Economic Corridor",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CMEC",
        "display_name": "CMEC",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Myanmar Oil Pipeline",
        "display_name": "China Myanmar Oil Pipeline",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Myanmar Gas Pipeline",
        "display_name": "China Myanmar Gas Pipeline",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "China Pakistan",
        "display_name": "China Pakistan",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "CPEC",
        "display_name": "CPEC",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "china pakistan economic corridor",
        "display_name": "china pakistan economic corridor",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "china pakistan military cooperation",
        "display_name": "china pakistan military cooperation",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "BRICS summit illness",
        "display_name": "BRICS summit illness",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Xi Jinping health BRICS",
        "display_name": "Xi Jinping health BRICS",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "BRICS leader health",
        "display_name": "BRICS leader health",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Powerus drone",
        "display_name": "Powerus drone",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Powerus UAV",
        "display_name": "Powerus UAV",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Powerus defense",
        "display_name": "Powerus defense",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "Powerus military",
        "display_name": "Powerus military",
        "category": "Baidu Keywords",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "india navy",
        "display_name": "india navy",
        "category": "India Navy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indian navy",
        "display_name": "indian navy",
        "category": "India Navy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "defence",
        "display_name": "defence",
        "category": "Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "defense",
        "display_name": "defense",
        "category": "Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indian defence",
        "display_name": "indian defence",
        "category": "Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan",
        "display_name": "pakistan",
        "category": "Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cjp",
        "display_name": "cjp",
        "category": "Cjp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chief justice pakistan",
        "display_name": "chief justice pakistan",
        "category": "Cjp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chief justice of pakistan",
        "display_name": "chief justice of pakistan",
        "category": "Cjp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "neet scam",
        "display_name": "neet scam",
        "category": "Neet Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "neet paper leak",
        "display_name": "neet paper leak",
        "category": "Neet Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "neet exam scam",
        "display_name": "neet exam scam",
        "category": "Neet Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china myanmar",
        "display_name": "china myanmar",
        "category": "China Myanmar",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china-myanmar",
        "display_name": "china-myanmar",
        "category": "China Myanmar",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china pakistan",
        "display_name": "china pakistan",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cpec",
        "display_name": "cpec",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china-pakistan",
        "display_name": "china-pakistan",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sino-pakistani",
        "display_name": "sino-pakistani",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china pakistan economic corridor",
        "display_name": "china pakistan economic corridor",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china pakistan military cooperation",
        "display_name": "china pakistan military cooperation",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china arms sales pakistan",
        "display_name": "china arms sales pakistan",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china pakistan nuclear cooperation",
        "display_name": "china pakistan nuclear cooperation",
        "category": "China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kpsc scam",
        "display_name": "kpsc scam",
        "category": "Kpsc Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kpsc corruption",
        "display_name": "kpsc corruption",
        "category": "Kpsc Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "karnataka public service commission scam",
        "display_name": "karnataka public service commission scam",
        "category": "Kpsc Scam",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "politics",
        "display_name": "politics",
        "category": "Politics",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "commerce",
        "display_name": "commerce",
        "category": "Commerce",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "finance",
        "display_name": "finance",
        "category": "Finance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "space launch site",
        "display_name": "space launch site",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "satellite launch center",
        "display_name": "satellite launch center",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jiuquan launch center",
        "display_name": "jiuquan launch center",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "xichang launch center",
        "display_name": "xichang launch center",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "taiyuan launch center",
        "display_name": "taiyuan launch center",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "wenchang launch site",
        "display_name": "wenchang launch site",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "long march rocket launch",
        "display_name": "long march rocket launch",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "casc launch",
        "display_name": "casc launch",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "beidou satellite launch",
        "display_name": "beidou satellite launch",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "suparco launch",
        "display_name": "suparco launch",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan satellite launch",
        "display_name": "pakistan satellite launch",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "paksat",
        "display_name": "paksat",
        "category": "Space Launch Sites China Pakistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nuclear storage site",
        "display_name": "nuclear storage site",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nuclear testing site",
        "display_name": "nuclear testing site",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kahuta nuclear facility",
        "display_name": "kahuta nuclear facility",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sargodha missile base",
        "display_name": "sargodha missile base",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "khushab reactor",
        "display_name": "khushab reactor",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lop nur test site",
        "display_name": "lop nur test site",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nuclear missile silo",
        "display_name": "nuclear missile silo",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "missile silo field",
        "display_name": "missile silo field",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nuclear command authority",
        "display_name": "nuclear command authority",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nuclear stockpile",
        "display_name": "nuclear stockpile",
        "category": "Nuclear Storage Testing Sites",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china pakistan space cooperation",
        "display_name": "china pakistan space cooperation",
        "category": "China Pakistan Space",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan space program",
        "display_name": "pakistan space program",
        "category": "China Pakistan Space",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "suparco china",
        "display_name": "suparco china",
        "category": "China Pakistan Space",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "journal of engineering and technology",
        "display_name": "journal of engineering and technology",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "acta astronautica",
        "display_name": "acta astronautica",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "defence science journal",
        "display_name": "defence science journal",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "s&t journal",
        "display_name": "s&t journal",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "aerospace engineering journal",
        "display_name": "aerospace engineering journal",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ieee aerospace",
        "display_name": "ieee aerospace",
        "category": "St Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla rocket force",
        "display_name": "pla rocket force",
        "category": "Pla Rocket Force",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china nuclear silo",
        "display_name": "china nuclear silo",
        "category": "Pla Rocket Force",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china missile silo field",
        "display_name": "china missile silo field",
        "category": "Pla Rocket Force",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china nuclear modernization",
        "display_name": "china nuclear modernization",
        "category": "Pla Rocket Force",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hypersonic missile",
        "display_name": "hypersonic missile",
        "category": "Hypersonic Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hypersonic glide vehicle",
        "display_name": "hypersonic glide vehicle",
        "category": "Hypersonic Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hypersonic test",
        "display_name": "hypersonic test",
        "category": "Hypersonic Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "shaheen missile",
        "display_name": "shaheen missile",
        "category": "Shaheen Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "shaheen missile test",
        "display_name": "shaheen missile test",
        "category": "Shaheen Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan missile program",
        "display_name": "pakistan missile program",
        "category": "Shaheen Missile",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "beidou",
        "display_name": "beidou",
        "category": "Beidou",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "beidou satellite",
        "display_name": "beidou satellite",
        "category": "Beidou",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "beidou launch",
        "display_name": "beidou launch",
        "category": "Beidou",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "insurgency northeast india",
        "display_name": "insurgency northeast india",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "unlf",
        "display_name": "unlf",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "united national liberation front",
        "display_name": "united national liberation front",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla rpf",
        "display_name": "pla rpf",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manipur peoples army",
        "display_name": "manipur peoples army",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "prepak pro",
        "display_name": "prepak pro",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kykl",
        "display_name": "kykl",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kanglei yawol kanna lup",
        "display_name": "kanglei yawol kanna lup",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kcp",
        "display_name": "kcp",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kangleipak communist party",
        "display_name": "kangleipak communist party",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nscn im",
        "display_name": "nscn im",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national socialist council nagaland",
        "display_name": "national socialist council nagaland",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ulfa",
        "display_name": "ulfa",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ulfa independent",
        "display_name": "ulfa independent",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kuki national army",
        "display_name": "kuki national army",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kna",
        "display_name": "kna",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kuki chin army",
        "display_name": "kuki chin army",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "arambai tenggol",
        "display_name": "arambai tenggol",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "meitei leepun",
        "display_name": "meitei leepun",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cocomi",
        "display_name": "cocomi",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "coordinating committee manipur integrity",
        "display_name": "coordinating committee manipur integrity",
        "category": "Northeast India Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manipur violence",
        "display_name": "manipur violence",
        "category": "Manipur Ethnic Conflict",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "meitei kuki conflict",
        "display_name": "meitei kuki conflict",
        "category": "Manipur Ethnic Conflict",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manipur ethnic clash",
        "display_name": "manipur ethnic clash",
        "category": "Manipur Ethnic Conflict",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manipur unrest",
        "display_name": "manipur unrest",
        "category": "Manipur Ethnic Conflict",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manipur militants",
        "display_name": "manipur militants",
        "category": "Manipur Ethnic Conflict",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "naga insurgency",
        "display_name": "naga insurgency",
        "category": "Naga Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nscn khaplang",
        "display_name": "nscn khaplang",
        "category": "Naga Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "naga nationalist forum",
        "display_name": "naga nationalist forum",
        "category": "Naga Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "naga peace talks",
        "display_name": "naga peace talks",
        "category": "Naga Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "framework agreement naga",
        "display_name": "framework agreement naga",
        "category": "Naga Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indo myanmar border",
        "display_name": "indo myanmar border",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "myanmar drug trafficking",
        "display_name": "myanmar drug trafficking",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "golden triangle narcotics",
        "display_name": "golden triangle narcotics",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "myanmar insurgent groups india",
        "display_name": "myanmar insurgent groups india",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cross border smuggling myanmar",
        "display_name": "cross border smuggling myanmar",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "myanmar arms smuggling",
        "display_name": "myanmar arms smuggling",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "free movement regime india myanmar",
        "display_name": "free movement regime india myanmar",
        "category": "Indo Myanmar Transnational Crime",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "india myanmar border fencing",
        "display_name": "india myanmar border fencing",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "illegal migration northeast india",
        "display_name": "illegal migration northeast india",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "border security force northeast",
        "display_name": "border security force northeast",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "rohingya migration india",
        "display_name": "rohingya migration india",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "assam border security",
        "display_name": "assam border security",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "porous border india myanmar",
        "display_name": "porous border india myanmar",
        "category": "Border Security Illegal Migration",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "golden triangle drug trade",
        "display_name": "golden triangle drug trade",
        "category": "Drug Trafficking Regional",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "methamphetamine trafficking myanmar",
        "display_name": "methamphetamine trafficking myanmar",
        "category": "Drug Trafficking Regional",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "narco terrorism northeast india",
        "display_name": "narco terrorism northeast india",
        "category": "Drug Trafficking Regional",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "drug smuggling manipur",
        "display_name": "drug smuggling manipur",
        "category": "Drug Trafficking Regional",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "opium cultivation myanmar",
        "display_name": "opium cultivation myanmar",
        "category": "Drug Trafficking Regional",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "rajeev bhattacharya journalist",
        "display_name": "rajeev bhattacharya journalist",
        "category": "Subject Matter Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sreeparna banerjee orf",
        "display_name": "sreeparna banerjee orf",
        "category": "Subject Matter Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pushpita das idsa",
        "display_name": "pushpita das idsa",
        "category": "Subject Matter Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "insurgency analyst northeast india",
        "display_name": "insurgency analyst northeast india",
        "category": "Subject Matter Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "border security expert india",
        "display_name": "border security expert india",
        "category": "Subject Matter Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "south asian terrorism portal",
        "display_name": "south asian terrorism portal",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "satp",
        "display_name": "satp",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "institute for defence studies and analyses",
        "display_name": "institute for defence studies and analyses",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "idsa",
        "display_name": "idsa",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "observer research foundation",
        "display_name": "observer research foundation",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "orf",
        "display_name": "orf",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "united nations office on drugs and code",
        "display_name": "united nations office on drugs and code",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "unodc",
        "display_name": "unodc",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "centre for east and northeast regional studies",
        "display_name": "centre for east and northeast regional studies",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ceners k",
        "display_name": "ceners k",
        "category": "Think Tanks Databases",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "facebook group insurgent",
        "display_name": "facebook group insurgent",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "whatsapp group northeast",
        "display_name": "whatsapp group northeast",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "instagram militant page",
        "display_name": "instagram militant page",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "twitter insurgency handle",
        "display_name": "twitter insurgency handle",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tiktok myanmar",
        "display_name": "tiktok myanmar",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "wechat china border",
        "display_name": "wechat china border",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "snapchat smuggling network",
        "display_name": "snapchat smuggling network",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "messenger group insurgent",
        "display_name": "messenger group insurgent",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "douyin china",
        "display_name": "douyin china",
        "category": "Social Media Platforms",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh politics",
        "display_name": "bangladesh politics",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh election",
        "display_name": "bangladesh election",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh government",
        "display_name": "bangladesh government",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sheikh hasina",
        "display_name": "sheikh hasina",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "muhammad yunus",
        "display_name": "muhammad yunus",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh interim government",
        "display_name": "bangladesh interim government",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh unrest",
        "display_name": "bangladesh unrest",
        "category": "Bangladesh Political Situation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh army",
        "display_name": "bangladesh army",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh armed forces",
        "display_name": "bangladesh armed forces",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh defence force",
        "display_name": "bangladesh defence force",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bd military",
        "display_name": "bd military",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh military news",
        "display_name": "bangladesh military news",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh navy",
        "display_name": "bangladesh navy",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh air force",
        "display_name": "bangladesh air force",
        "category": "Bangladesh Military Defence",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jamaat e islami",
        "display_name": "jamaat e islami",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bji",
        "display_name": "bji",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh nationalist party",
        "display_name": "bangladesh nationalist party",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bnp bangladesh",
        "display_name": "bnp bangladesh",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "shafiqur rahman",
        "display_name": "shafiqur rahman",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national citizen party bangladesh",
        "display_name": "national citizen party bangladesh",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ncp bangladesh",
        "display_name": "ncp bangladesh",
        "category": "Jamaat E Islami Bnp",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kachin national organisation",
        "display_name": "kachin national organisation",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "arakan army",
        "display_name": "arakan army",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "absdf",
        "display_name": "absdf",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "all burma students democratic front",
        "display_name": "all burma students democratic front",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "myanmar bangladesh border",
        "display_name": "myanmar bangladesh border",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "rohingya crisis bangladesh",
        "display_name": "rohingya crisis bangladesh",
        "category": "Bangladesh Myanmar Border",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "biiss",
        "display_name": "biiss",
        "category": "Bangladesh Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh institute of international and strategic studies",
        "display_name": "bangladesh institute of international and strategic studies",
        "category": "Bangladesh Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bipss",
        "display_name": "bipss",
        "category": "Bangladesh Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh institute of peace and security studies",
        "display_name": "bangladesh institute of peace and security studies",
        "category": "Bangladesh Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national defence college bangladesh",
        "display_name": "national defence college bangladesh",
        "category": "Bangladesh Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chatham house",
        "display_name": "chatham house",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "european policy centre",
        "display_name": "european policy centre",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tillotoma foundation",
        "display_name": "tillotoma foundation",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "claws",
        "display_name": "claws",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "centre for land warfare studies",
        "display_name": "centre for land warfare studies",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "centre for geopolitics and security in realism studies",
        "display_name": "centre for geopolitics and security in realism studies",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indian council of world affairs",
        "display_name": "indian council of world affairs",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "icwa",
        "display_name": "icwa",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "on think tanks",
        "display_name": "on think tanks",
        "category": "South Asia Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "impri india",
        "display_name": "impri india",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "impact and policy research institute",
        "display_name": "impact and policy research institute",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "synergia foundation",
        "display_name": "synergia foundation",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "institute of strategic studies islamabad",
        "display_name": "institute of strategic studies islamabad",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "issi pakistan",
        "display_name": "issi pakistan",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "the peninsula foundation",
        "display_name": "the peninsula foundation",
        "category": "Policy Research Institutes",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "centre for policy dialogue bangladesh",
        "display_name": "centre for policy dialogue bangladesh",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cpd bangladesh",
        "display_name": "cpd bangladesh",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "brac institute of governance and development",
        "display_name": "brac institute of governance and development",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bigd",
        "display_name": "bigd",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh institute of development studies",
        "display_name": "bangladesh institute of development studies",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bids",
        "display_name": "bids",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh enterprise institute",
        "display_name": "bangladesh enterprise institute",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bei",
        "display_name": "bei",
        "category": "Bangladesh Economic Governance",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bdmofa",
        "display_name": "bdmofa",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bji official",
        "display_name": "bji official",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh army official",
        "display_name": "bangladesh army official",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh air force defence",
        "display_name": "bangladesh air force defence",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "military bma bd",
        "display_name": "military bma bd",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "defseca bd",
        "display_name": "defseca bd",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "rab bangladesh",
        "display_name": "rab bangladesh",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh_bnp",
        "display_name": "bangladesh_bnp",
        "category": "Social Media Handles Bangladesh",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan liberation army",
        "display_name": "balochistan liberation army",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bla attack",
        "display_name": "bla attack",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan republican army",
        "display_name": "balochistan republican army",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bra",
        "display_name": "bra",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan national army",
        "display_name": "balochistan national army",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bna",
        "display_name": "bna",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan republican guard",
        "display_name": "balochistan republican guard",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "brg",
        "display_name": "brg",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "majeed brigade",
        "display_name": "majeed brigade",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch raaji ajoi sangar",
        "display_name": "baloch raaji ajoi sangar",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bras",
        "display_name": "bras",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan insurgency",
        "display_name": "balochistan insurgency",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch separatist",
        "display_name": "baloch separatist",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch militants",
        "display_name": "baloch militants",
        "category": "Balochistan Insurgency",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch yakjehti committee",
        "display_name": "baloch yakjehti committee",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "byc",
        "display_name": "byc",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "mahrang baloch",
        "display_name": "mahrang baloch",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "gwadar bachao andolan",
        "display_name": "gwadar bachao andolan",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "haq do movement",
        "display_name": "haq do movement",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch national movement",
        "display_name": "baloch national movement",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bnm",
        "display_name": "bnm",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "voice of baloch missing persons",
        "display_name": "voice of baloch missing persons",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "vbmp",
        "display_name": "vbmp",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "free balochistan movement",
        "display_name": "free balochistan movement",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch human rights organization",
        "display_name": "baloch human rights organization",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bhro",
        "display_name": "bhro",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch human rights council",
        "display_name": "baloch human rights council",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bhrc",
        "display_name": "bhrc",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "baloch students organization",
        "display_name": "baloch students organization",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bso",
        "display_name": "bso",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "voice of balochistan",
        "display_name": "voice of balochistan",
        "category": "Baloch Political Movements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lashkar e balochistan",
        "display_name": "lashkar e balochistan",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "leb",
        "display_name": "leb",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tehreek e nifaz e aman balochistan",
        "display_name": "tehreek e nifaz e aman balochistan",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tnab",
        "display_name": "tnab",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan muttahida mahaz",
        "display_name": "balochistan muttahida mahaz",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lashkar e khorasan",
        "display_name": "lashkar e khorasan",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lek",
        "display_name": "lek",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sarfaraz peace force",
        "display_name": "sarfaraz peace force",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "siraj raisani united front",
        "display_name": "siraj raisani united front",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "shafiq mengal group",
        "display_name": "shafiq mengal group",
        "category": "Balochistan Militant Factions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "khyber pakhtunkhwa security",
        "display_name": "khyber pakhtunkhwa security",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "kp militancy",
        "display_name": "kp militancy",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pashtun tahafuz movement",
        "display_name": "pashtun tahafuz movement",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ptm",
        "display_name": "ptm",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "punjabi taliban",
        "display_name": "punjabi taliban",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tribal areas pakistan",
        "display_name": "tribal areas pakistan",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "fata militancy",
        "display_name": "fata militancy",
        "category": "Khyber Pakhtunkhwa Militancy",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tehreek e taliban pakistan",
        "display_name": "tehreek e taliban pakistan",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ttp",
        "display_name": "ttp",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lashkar e jhangvi",
        "display_name": "lashkar e jhangvi",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lej",
        "display_name": "lej",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jamaat ul ahrar",
        "display_name": "jamaat ul ahrar",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jua",
        "display_name": "jua",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hizb ul ahrar",
        "display_name": "hizb ul ahrar",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hua",
        "display_name": "hua",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jaish ul adl",
        "display_name": "jaish ul adl",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jundullah",
        "display_name": "jundullah",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sipah e sahaba pakistan",
        "display_name": "sipah e sahaba pakistan",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ssp",
        "display_name": "ssp",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tariq geedar group",
        "display_name": "tariq geedar group",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tgg",
        "display_name": "tgg",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lashkar e islam",
        "display_name": "lashkar e islam",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "lei",
        "display_name": "lei",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ansar ul islam",
        "display_name": "ansar ul islam",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "aui",
        "display_name": "aui",
        "category": "Pakistan Militant Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "islamic movement of uzbekistan",
        "display_name": "islamic movement of uzbekistan",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "imu",
        "display_name": "imu",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "islamic jihad group",
        "display_name": "islamic jihad group",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ijg",
        "display_name": "ijg",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "turkestan islamic party",
        "display_name": "turkestan islamic party",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tip",
        "display_name": "tip",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "east turkestan islamic movement",
        "display_name": "east turkestan islamic movement",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "etim",
        "display_name": "etim",
        "category": "Central Asian Jihadist Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "islamic state khorasan province",
        "display_name": "islamic state khorasan province",
        "category": "Islamic State Khorasan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "iskp",
        "display_name": "iskp",
        "category": "Islamic State Khorasan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "daesh khorasan",
        "display_name": "daesh khorasan",
        "category": "Islamic State Khorasan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "isis-k attack",
        "display_name": "isis-k attack",
        "category": "Islamic State Khorasan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "difa e pakistan council",
        "display_name": "difa e pakistan council",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "dpc",
        "display_name": "dpc",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tehreek e labaik pakistan",
        "display_name": "tehreek e labaik pakistan",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tlp",
        "display_name": "tlp",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hizb ut tahrir",
        "display_name": "hizb ut tahrir",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hut",
        "display_name": "hut",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ahle sunnat wal jamaat",
        "display_name": "ahle sunnat wal jamaat",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "aswj",
        "display_name": "aswj",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tehreek ghalba e islam",
        "display_name": "tehreek ghalba e islam",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tgi",
        "display_name": "tgi",
        "category": "Pakistan Religious Political Groups",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national counter terrorism authority",
        "display_name": "national counter terrorism authority",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nacta",
        "display_name": "nacta",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national action plan pakistan",
        "display_name": "national action plan pakistan",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nap",
        "display_name": "nap",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "frontier crimes regulation",
        "display_name": "frontier crimes regulation",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "fcr",
        "display_name": "fcr",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national accountability bureau",
        "display_name": "national accountability bureau",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nab",
        "display_name": "nab",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "inter services public relations",
        "display_name": "inter services public relations",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "ispr",
        "display_name": "ispr",
        "category": "Pakistan Counter Terrorism Bodies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "institute for the study of war",
        "display_name": "institute for the study of war",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "khorasan diary",
        "display_name": "khorasan diary",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "allsource analysis",
        "display_name": "allsource analysis",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "war analyst pakistan",
        "display_name": "war analyst pakistan",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pak afghan matters",
        "display_name": "pak afghan matters",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "afghan analyst baloch",
        "display_name": "afghan analyst baloch",
        "category": "Think Tanks Experts",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bakhtar news agency",
        "display_name": "bakhtar news agency",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "islamabad post",
        "display_name": "islamabad post",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "afghanistan times",
        "display_name": "afghanistan times",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "tolo news",
        "display_name": "tolo news",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "express tribune",
        "display_name": "express tribune",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan narrative",
        "display_name": "pakistan narrative",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "balochistan insight",
        "display_name": "balochistan insight",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "osinttv",
        "display_name": "osinttv",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "hurriyat radio pashto",
        "display_name": "hurriyat radio pashto",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "brief pk",
        "display_name": "brief pk",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "iea urdu official",
        "display_name": "iea urdu official",
        "category": "News Media Pakistan Afghanistan",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan navy",
        "display_name": "pakistan navy",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "dgpr pak navy",
        "display_name": "dgpr pak navy",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sri lanka navy official",
        "display_name": "sri lanka navy official",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "mndf maldives",
        "display_name": "mndf maldives",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "maritime malaysia",
        "display_name": "maritime malaysia",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "royal thai navy",
        "display_name": "royal thai navy",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indonesian navy",
        "display_name": "indonesian navy",
        "category": "Regional Navies Social Media",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangabandhu sheikh mujibur rahman maritime university",
        "display_name": "bangabandhu sheikh mujibur rahman maritime university",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bsmrmu",
        "display_name": "bsmrmu",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "national defence university",
        "display_name": "national defence university",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "naval academy",
        "display_name": "naval academy",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pakistan navy war college",
        "display_name": "pakistan navy war college",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "royal thai naval academy",
        "display_name": "royal thai naval academy",
        "category": "Naval Academies Training Institutions",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "bangladesh maritime journal",
        "display_name": "bangladesh maritime journal",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "maritime journal id",
        "display_name": "maritime journal id",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indonesian maritime journal",
        "display_name": "indonesian maritime journal",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "jurnal maritim indonesia",
        "display_name": "jurnal maritim indonesia",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sri lanka navy journal",
        "display_name": "sri lanka navy journal",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "insight magazine navy",
        "display_name": "insight magazine navy",
        "category": "Maritime Journals Asia",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "journal of engineering and technology maritime",
        "display_name": "journal of engineering and technology maritime",
        "category": "Naval Engineering Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "journal of naval science and engineering",
        "display_name": "journal of naval science and engineering",
        "category": "Naval Engineering Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "journal of marine engineering technology",
        "display_name": "journal of marine engineering technology",
        "category": "Naval Engineering Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "journal of the indian ocean region",
        "display_name": "journal of the indian ocean region",
        "category": "Naval Engineering Journals",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "us naval institute proceedings",
        "display_name": "us naval institute proceedings",
        "category": "Us Naval Publications",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "naval war college review",
        "display_name": "naval war college review",
        "category": "Us Naval Publications",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "seaways magazine",
        "display_name": "seaways magazine",
        "category": "Us Naval Publications",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "the navigator maritime",
        "display_name": "the navigator maritime",
        "category": "Us Naval Publications",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china maritime reports",
        "display_name": "china maritime reports",
        "category": "China Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "shipbuilding of china",
        "display_name": "shipbuilding of china",
        "category": "China Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china naval modernization",
        "display_name": "china naval modernization",
        "category": "China Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla navy",
        "display_name": "pla navy",
        "category": "China Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "stimson center maritime",
        "display_name": "stimson center maritime",
        "category": "Regional Maritime Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "geopolitical monitor navy",
        "display_name": "geopolitical monitor navy",
        "category": "Regional Maritime Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "mima bulletin",
        "display_name": "mima bulletin",
        "category": "Regional Maritime Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "maritime institute of malaysia",
        "display_name": "maritime institute of malaysia",
        "category": "Regional Maritime Think Tanks",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "arab gulf journal",
        "display_name": "arab gulf journal",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "scientia militaria",
        "display_name": "scientia militaria",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "western indian ocean marine science association",
        "display_name": "western indian ocean marine science association",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "wiomsa",
        "display_name": "wiomsa",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "intergovernmental authority on development",
        "display_name": "intergovernmental authority on development",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "igad maritime",
        "display_name": "igad maritime",
        "category": "Gulf Africa Maritime Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "turkiye navy",
        "display_name": "turkiye navy",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "gulf countries navy",
        "display_name": "gulf countries navy",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sri lanka navy",
        "display_name": "sri lanka navy",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "maldives national defence force",
        "display_name": "maldives national defence force",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "mauritius coast guard",
        "display_name": "mauritius coast guard",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "seychelles coast guard",
        "display_name": "seychelles coast guard",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china navy",
        "display_name": "china navy",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indonesia navy",
        "display_name": "indonesia navy",
        "category": "Indian Ocean Region Countries",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla movements",
        "display_name": "pla movements",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chinese military aircraft",
        "display_name": "chinese military aircraft",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla navy deployment",
        "display_name": "pla navy deployment",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china military exercise",
        "display_name": "china military exercise",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla air force intrusion",
        "display_name": "pla air force intrusion",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chinese warplane adiz",
        "display_name": "chinese warplane adiz",
        "category": "Pla Military Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china aerospace studies institute",
        "display_name": "china aerospace studies institute",
        "category": "China Aerospace Osint",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "casi report",
        "display_name": "casi report",
        "category": "China Aerospace Osint",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china space program",
        "display_name": "china space program",
        "category": "China Aerospace Osint",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "pla satellite launch",
        "display_name": "pla satellite launch",
        "category": "China Aerospace Osint",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china military modernization",
        "display_name": "china military modernization",
        "category": "China Aerospace Osint",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "mfa china statement",
        "display_name": "mfa china statement",
        "category": "China Foreign Ministry Statements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china foreign ministry spokesperson",
        "display_name": "china foreign ministry spokesperson",
        "category": "China Foreign Ministry Statements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china diplomatic response",
        "display_name": "china diplomatic response",
        "category": "China Foreign Ministry Statements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "wang yi statement",
        "display_name": "wang yi statement",
        "category": "China Foreign Ministry Statements",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china beige book",
        "display_name": "china beige book",
        "category": "China Economic Strategic Intel",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china economic indicators",
        "display_name": "china economic indicators",
        "category": "China Economic Strategic Intel",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china energy infrastructure map",
        "display_name": "china energy infrastructure map",
        "category": "China Economic Strategic Intel",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china bio threats",
        "display_name": "china bio threats",
        "category": "China Economic Strategic Intel",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china dual use technology",
        "display_name": "china dual use technology",
        "category": "China Economic Strategic Intel",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china power csis",
        "display_name": "china power csis",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china brief jamestown",
        "display_name": "china brief jamestown",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "council on foreign relations china",
        "display_name": "council on foreign relations china",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "rand corporation china",
        "display_name": "rand corporation china",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "east asia forum china",
        "display_name": "east asia forum china",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china file",
        "display_name": "china file",
        "category": "China Think Tank Analysis",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "wassenaar arrangement",
        "display_name": "wassenaar arrangement",
        "category": "Export Control Nonproliferation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "sipri arms transfer",
        "display_name": "sipri arms transfer",
        "category": "Export Control Nonproliferation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china arms export",
        "display_name": "china arms export",
        "category": "Export Control Nonproliferation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "dual use export control",
        "display_name": "dual use export control",
        "category": "Export Control Nonproliferation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "nonproliferation china",
        "display_name": "nonproliferation china",
        "category": "Export Control Nonproliferation",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "japan ministry of defense",
        "display_name": "japan ministry of defense",
        "category": "Japan Defense Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "japan self defense force",
        "display_name": "japan self defense force",
        "category": "Japan Defense Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china japan tension",
        "display_name": "china japan tension",
        "category": "Japan Defense Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "east china sea patrol",
        "display_name": "east china sea patrol",
        "category": "Japan Defense Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "senkaku islands",
        "display_name": "senkaku islands",
        "category": "Japan Defense Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "scramble.nl",
        "display_name": "scramble.nl",
        "category": "Aviation Osint Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "military aircraft tracking",
        "display_name": "military aircraft tracking",
        "category": "Aviation Osint Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "adiz incursion",
        "display_name": "adiz incursion",
        "category": "Aviation Osint Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "fighter jet scramble",
        "display_name": "fighter jet scramble",
        "category": "Aviation Osint Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "airspace violation china",
        "display_name": "airspace violation china",
        "category": "Aviation Osint Tracking",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "manohar parrikar idsa",
        "display_name": "manohar parrikar idsa",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "vivekananda international foundation",
        "display_name": "vivekananda international foundation",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "claws india",
        "display_name": "claws india",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "cenjows",
        "display_name": "cenjows",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "centre for joint warfare studies",
        "display_name": "centre for joint warfare studies",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "usanas foundation",
        "display_name": "usanas foundation",
        "category": "Indian Think Tanks Defense",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "australian strategic policy institute",
        "display_name": "australian strategic policy institute",
        "category": "Australian Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "aspi china report",
        "display_name": "aspi china report",
        "category": "Australian Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "indo pacific security aspi",
        "display_name": "indo pacific security aspi",
        "category": "Australian Strategic Studies",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "weibo trending china",
        "display_name": "weibo trending china",
        "category": "Chinese Social Media Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "zaobao news",
        "display_name": "zaobao news",
        "category": "Chinese Social Media Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "china social media sentiment",
        "display_name": "china social media sentiment",
        "category": "Chinese Social Media Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "chinese public opinion monitoring",
        "display_name": "chinese public opinion monitoring",
        "category": "Chinese Social Media Monitoring",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS summit illness",
        "display_name": "BRICS summit illness",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS leader sick",
        "display_name": "BRICS leader sick",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS leader health",
        "display_name": "BRICS leader health",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS leader hospitalized",
        "display_name": "BRICS leader hospitalized",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS leader collapse",
        "display_name": "BRICS leader collapse",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS health emergency",
        "display_name": "BRICS health emergency",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS summit medical",
        "display_name": "BRICS summit medical",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "world leader sick BRICS",
        "display_name": "world leader sick BRICS",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS summit 2026",
        "display_name": "BRICS summit 2026",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "BRICS Brazil summit",
        "display_name": "BRICS Brazil summit",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "leader falls ill summit",
        "display_name": "leader falls ill summit",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "head of state hospitalized BRICS",
        "display_name": "head of state hospitalized BRICS",
        "category": "Brics Leader Health",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus",
        "display_name": "Powerus",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus drone",
        "display_name": "Powerus drone",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus UAV",
        "display_name": "Powerus UAV",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus defense",
        "display_name": "Powerus defense",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus Guardian-2",
        "display_name": "Powerus Guardian-2",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Guardian-2 interceptor",
        "display_name": "Guardian-2 interceptor",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Guardian-2 counter-drone",
        "display_name": "Guardian-2 counter-drone",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus Air Force",
        "display_name": "Powerus Air Force",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus counter-drone",
        "display_name": "Powerus counter-drone",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus USAF contract",
        "display_name": "Powerus USAF contract",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Brett Velicovich",
        "display_name": "Brett Velicovich",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Andrew Fox drone",
        "display_name": "Andrew Fox drone",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "PUSA drone Nasdaq",
        "display_name": "PUSA drone Nasdaq",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Aureus Greenway Powerus",
        "display_name": "Aureus Greenway Powerus",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Trump sons drone company",
        "display_name": "Trump sons drone company",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus Gulf markets",
        "display_name": "Powerus Gulf markets",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "Powerus Shahed interceptor",
        "display_name": "Powerus Shahed interceptor",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "truth_social",
        "handle": "autonomous drone defense US",
        "display_name": "autonomous drone defense US",
        "category": "Powerus Drone",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal floods",
        "display_name": "Nepal floods",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal flood disaster",
        "display_name": "Nepal flood disaster",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal flood alert",
        "display_name": "Nepal flood alert",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kathmandu floods",
        "display_name": "Kathmandu floods",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal monsoon floods",
        "display_name": "Nepal monsoon floods",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal heavy rainfall",
        "display_name": "Nepal heavy rainfall",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal mudslide",
        "display_name": "Nepal mudslide",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal landslide",
        "display_name": "Nepal landslide",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal flood relief",
        "display_name": "Nepal flood relief",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal disaster response",
        "display_name": "Nepal disaster response",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal emergency",
        "display_name": "Nepal emergency",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal natural disaster",
        "display_name": "Nepal natural disaster",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal flood update",
        "display_name": "Nepal flood update",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal weather alert",
        "display_name": "Nepal weather alert",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Nepal flood casualties",
        "display_name": "Nepal flood casualties",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "gas price india",
        "display_name": "gas price india",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "fuel price india",
        "display_name": "fuel price india",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "elections",
        "display_name": "elections",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "political media",
        "display_name": "political media",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "cockroach janata party",
        "display_name": "cockroach janata party",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "युद्ध",
        "display_name": "युद्ध",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "border tension",
        "display_name": "border tension",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "military movement",
        "display_name": "military movement",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "defence news",
        "display_name": "defence news",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "security alert",
        "display_name": "security alert",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "terror attack india",
        "display_name": "terror attack india",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "bomb blast",
        "display_name": "bomb blast",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "drone attack",
        "display_name": "drone attack",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "cross border firing",
        "display_name": "cross border firing",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "encounter news",
        "display_name": "encounter news",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "kashmir encounter",
        "display_name": "kashmir encounter",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "manipur violence",
        "display_name": "manipur violence",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "northeast violence",
        "display_name": "northeast violence",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "india pakistan tension",
        "display_name": "india pakistan tension",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "iran israel war",
        "display_name": "iran israel war",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "china border",
        "display_name": "china border",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "सीमा तनाव",
        "display_name": "सीमा तनाव",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Japanese Ministry of Defense",
        "display_name": "Japanese Ministry of Defense",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Scramble.nl",
        "display_name": "Scramble.nl",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Bakers Institute Energy Map",
        "display_name": "Bakers Institute Energy Map",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "IOSI Global",
        "display_name": "IOSI Global",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Australian Strategic Policy Institute",
        "display_name": "Australian Strategic Policy Institute",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ASPI",
        "display_name": "ASPI",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China File",
        "display_name": "China File",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Wassenaar Arrangement",
        "display_name": "Wassenaar Arrangement",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Stockholm International Peace Research Institute",
        "display_name": "Stockholm International Peace Research Institute",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "SIPRI",
        "display_name": "SIPRI",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Aerospace Studies Institute",
        "display_name": "China Aerospace Studies Institute",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CASI",
        "display_name": "CASI",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CCP Bio Threats Initiative",
        "display_name": "CCP Bio Threats Initiative",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "display_name": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "IDSA",
        "display_name": "IDSA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Vivekananda International Foundation",
        "display_name": "Vivekananda International Foundation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Indian Council for World Affairs",
        "display_name": "Indian Council for World Affairs",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ICWA",
        "display_name": "ICWA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Centre for Land Warfare Studies",
        "display_name": "Centre for Land Warfare Studies",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CLAWS",
        "display_name": "CLAWS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "USANAS Foundation",
        "display_name": "USANAS Foundation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Centre for Joint Warfare Studies",
        "display_name": "Centre for Joint Warfare Studies",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CENJOWS",
        "display_name": "CENJOWS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "display_name": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BSMRMU",
        "display_name": "BSMRMU",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Bangladesh Maritime Journal",
        "display_name": "Bangladesh Maritime Journal",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Journal of Engineering and Technology",
        "display_name": "Journal of Engineering and Technology",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "maritimejournal.id",
        "display_name": "maritimejournal.id",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "National Defence University Naval Academy",
        "display_name": "National Defence University Naval Academy",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "DergiPark",
        "display_name": "DergiPark",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Stimson Center",
        "display_name": "Stimson Center",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Geopolitical Monitor",
        "display_name": "Geopolitical Monitor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "US Naval Institute",
        "display_name": "US Naval Institute",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Seaways",
        "display_name": "Seaways",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "The Navigator",
        "display_name": "The Navigator",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan Navy War College",
        "display_name": "Pakistan Navy War College",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Sri Lanka Navy Journal",
        "display_name": "Sri Lanka Navy Journal",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Insight Magazine Maldives",
        "display_name": "Insight Magazine Maldives",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Journal of the Indian Ocean Region",
        "display_name": "Journal of the Indian Ocean Region",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Maritime Reports",
        "display_name": "China Maritime Reports",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Indonesian Maritime Journal",
        "display_name": "Indonesian Maritime Journal",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Jurnal Maritim Indonesia",
        "display_name": "Jurnal Maritim Indonesia",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "MIMA Bulletin",
        "display_name": "MIMA Bulletin",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Royal Thai Naval Academy",
        "display_name": "Royal Thai Naval Academy",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Journal of Naval Science and Engineering",
        "display_name": "Journal of Naval Science and Engineering",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "The Arab Gulf Journal",
        "display_name": "The Arab Gulf Journal",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Scientia Militaria",
        "display_name": "Scientia Militaria",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Western Indian Ocean Marine Science Association",
        "display_name": "Western Indian Ocean Marine Science Association",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "WIOMSA",
        "display_name": "WIOMSA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Intergovernmental Authority on Development",
        "display_name": "Intergovernmental Authority on Development",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "IGAD",
        "display_name": "IGAD",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Naval War College Review",
        "display_name": "Naval War College Review",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "US Naval Institute Proceedings",
        "display_name": "US Naval Institute Proceedings",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Journal of Marine Engineering and Technology",
        "display_name": "Journal of Marine Engineering and Technology",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "maritime security",
        "display_name": "maritime security",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "maritime affairs",
        "display_name": "maritime affairs",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "naval strategy",
        "display_name": "naval strategy",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "naval warfare",
        "display_name": "naval warfare",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "defense studies",
        "display_name": "defense studies",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "strategic affairs",
        "display_name": "strategic affairs",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "geopolitics",
        "display_name": "geopolitics",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "indo pacific",
        "display_name": "indo pacific",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "indian ocean region",
        "display_name": "indian ocean region",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "People's Liberation Army",
        "display_name": "People's Liberation Army",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA training",
        "display_name": "PLA training",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA exercise",
        "display_name": "PLA exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA drill",
        "display_name": "PLA drill",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA modernization",
        "display_name": "PLA modernization",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA combat readiness",
        "display_name": "PLA combat readiness",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA field forces",
        "display_name": "PLA field forces",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA leadership",
        "display_name": "PLA leadership",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA purge",
        "display_name": "PLA purge",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Western Theater Command",
        "display_name": "Western Theater Command",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA Western Theater Command",
        "display_name": "PLA Western Theater Command",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Eastern Theater Command",
        "display_name": "Eastern Theater Command",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Southern Theater Command",
        "display_name": "Southern Theater Command",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "76th Group Army",
        "display_name": "76th Group Army",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "77th Group Army",
        "display_name": "77th Group Army",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Group Army PLA",
        "display_name": "Group Army PLA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xinjiang Military District",
        "display_name": "Xinjiang Military District",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Tibet Military",
        "display_name": "Tibet Military",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xinjiang military exercise",
        "display_name": "Xinjiang military exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Tibet military exercise",
        "display_name": "Tibet military exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xinjiang infrastructure",
        "display_name": "Xinjiang infrastructure",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLAAF",
        "display_name": "PLAAF",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "People's Liberation Army Air Force",
        "display_name": "People's Liberation Army Air Force",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLAAF training",
        "display_name": "PLAAF training",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLAAF exercise",
        "display_name": "PLAAF exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese military aviation",
        "display_name": "Chinese military aviation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "J-20 fighter",
        "display_name": "J-20 fighter",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "J-16 fighter",
        "display_name": "J-16 fighter",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "H-6 bomber",
        "display_name": "H-6 bomber",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLARF",
        "display_name": "PLARF",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "People's Liberation Army Rocket Force",
        "display_name": "People's Liberation Army Rocket Force",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Rocket Force exercise",
        "display_name": "Rocket Force exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese missile launch",
        "display_name": "Chinese missile launch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese missile test",
        "display_name": "Chinese missile test",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "hypersonic missile China",
        "display_name": "hypersonic missile China",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "LAC standoff",
        "display_name": "LAC standoff",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Eastern Ladakh",
        "display_name": "Eastern Ladakh",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Arunachal border",
        "display_name": "Arunachal border",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "People's Liberation Army Navy",
        "display_name": "People's Liberation Army Navy",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese naval exercise",
        "display_name": "Chinese naval exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese warship",
        "display_name": "Chinese warship",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese aircraft carrier",
        "display_name": "Chinese aircraft carrier",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Type 055 destroyer",
        "display_name": "Type 055 destroyer",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Type 052D destroyer",
        "display_name": "Type 052D destroyer",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "South China Sea exercise",
        "display_name": "South China Sea exercise",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Indian Ocean deployment",
        "display_name": "Indian Ocean deployment",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese presence Indian Ocean",
        "display_name": "Chinese presence Indian Ocean",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China maritime infrastructure",
        "display_name": "China maritime infrastructure",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese naval base",
        "display_name": "Chinese naval base",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "String of Pearls",
        "display_name": "String of Pearls",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Maritime Silk Road",
        "display_name": "Maritime Silk Road",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "IOR security",
        "display_name": "IOR security",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gwadar Port",
        "display_name": "Gwadar Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Hambantota Port",
        "display_name": "Hambantota Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kyaukpyu Port",
        "display_name": "Kyaukpyu Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chittagong Port",
        "display_name": "Chittagong Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Bagamoyo Port",
        "display_name": "Bagamoyo Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Djibouti naval base",
        "display_name": "Djibouti naval base",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China border infrastructure",
        "display_name": "China border infrastructure",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China border village",
        "display_name": "China border village",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China military road",
        "display_name": "China military road",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Sichuan Tibet Railway",
        "display_name": "Sichuan Tibet Railway",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Brahmaputra dam",
        "display_name": "Brahmaputra dam",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Yarlung Tsangpo dam",
        "display_name": "Yarlung Tsangpo dam",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment Pakistan",
        "display_name": "Chinese investment Pakistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment Nepal",
        "display_name": "Chinese investment Nepal",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment Bangladesh",
        "display_name": "Chinese investment Bangladesh",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment ASEAN",
        "display_name": "Chinese investment ASEAN",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China energy security",
        "display_name": "China energy security",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China energy transition",
        "display_name": "China energy transition",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China oil imports",
        "display_name": "China oil imports",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China coal production",
        "display_name": "China coal production",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Zhang Youxia",
        "display_name": "Zhang Youxia",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CMC China",
        "display_name": "CMC China",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "解放军",
        "display_name": "解放军",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "中国军队",
        "display_name": "中国军队",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "西部战区",
        "display_name": "西部战区",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "东部战区",
        "display_name": "东部战区",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "南部战区",
        "display_name": "南部战区",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "新疆军区",
        "display_name": "新疆军区",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "中印边境",
        "display_name": "中印边境",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China",
        "display_name": "China",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese",
        "display_name": "Chinese",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Beijing",
        "display_name": "Beijing",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PRC",
        "display_name": "PRC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CCP",
        "display_name": "CCP",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xi Jinping",
        "display_name": "Xi Jinping",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese Embassy",
        "display_name": "Chinese Embassy",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese Ambassador",
        "display_name": "Chinese Ambassador",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Myanmar",
        "display_name": "Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Burma",
        "display_name": "Burma",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Naypyidaw",
        "display_name": "Naypyidaw",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Yangon",
        "display_name": "Yangon",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Mandalay",
        "display_name": "Mandalay",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Tatmadaw",
        "display_name": "Tatmadaw",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Junta",
        "display_name": "Junta",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "SAC",
        "display_name": "SAC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "State Administration Council",
        "display_name": "State Administration Council",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "NUG",
        "display_name": "NUG",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "National Unity Government",
        "display_name": "National Unity Government",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Belt and Road Initiative",
        "display_name": "Belt and Road Initiative",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRI",
        "display_name": "BRI",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar Economic Corridor",
        "display_name": "China Myanmar Economic Corridor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CMEC",
        "display_name": "CMEC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kyaukphyu",
        "display_name": "Kyaukphyu",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kyaukphyu Port",
        "display_name": "Kyaukphyu Port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kyaukphyu SEZ",
        "display_name": "Kyaukphyu SEZ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Special Economic Zone",
        "display_name": "Special Economic Zone",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar Railway",
        "display_name": "China Myanmar Railway",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Muse Mandalay Railway",
        "display_name": "Muse Mandalay Railway",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar Corridor",
        "display_name": "China Myanmar Corridor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "infrastructure project",
        "display_name": "infrastructure project",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "industrial park",
        "display_name": "industrial park",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "logistics corridor",
        "display_name": "logistics corridor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar Oil Pipeline",
        "display_name": "China Myanmar Oil Pipeline",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar Gas Pipeline",
        "display_name": "China Myanmar Gas Pipeline",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "oil pipeline",
        "display_name": "oil pipeline",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "gas pipeline",
        "display_name": "gas pipeline",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "LNG",
        "display_name": "LNG",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "hydropower",
        "display_name": "hydropower",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "power project",
        "display_name": "power project",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "electricity project",
        "display_name": "electricity project",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment",
        "display_name": "Chinese investment",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese company",
        "display_name": "Chinese company",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese business",
        "display_name": "Chinese business",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese contractor",
        "display_name": "Chinese contractor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "investment project",
        "display_name": "investment project",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "economic cooperation",
        "display_name": "economic cooperation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "bilateral cooperation",
        "display_name": "bilateral cooperation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "trade agreement",
        "display_name": "trade agreement",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "trade corridor",
        "display_name": "trade corridor",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "foreign investment",
        "display_name": "foreign investment",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "development project",
        "display_name": "development project",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese weapons",
        "display_name": "Chinese weapons",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese arms",
        "display_name": "Chinese arms",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "military cooperation",
        "display_name": "military cooperation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "military aid",
        "display_name": "military aid",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "fighter jet",
        "display_name": "fighter jet",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "UAV",
        "display_name": "UAV",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "drone",
        "display_name": "drone",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "surveillance",
        "display_name": "surveillance",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "radar",
        "display_name": "radar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA",
        "display_name": "PLA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "security cooperation",
        "display_name": "security cooperation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Yunnan",
        "display_name": "Yunnan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Ruili",
        "display_name": "Ruili",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Muse",
        "display_name": "Muse",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kokang",
        "display_name": "Kokang",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Wa State",
        "display_name": "Wa State",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Shan State",
        "display_name": "Shan State",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kachin",
        "display_name": "Kachin",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinshwehaw",
        "display_name": "Chinshwehaw",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "border trade",
        "display_name": "border trade",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "border crossing",
        "display_name": "border crossing",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "cross border trade",
        "display_name": "cross border trade",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "customs",
        "display_name": "customs",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "rare earth",
        "display_name": "rare earth",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "rare earth mining",
        "display_name": "rare earth mining",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "jade mining",
        "display_name": "jade mining",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "copper mine",
        "display_name": "copper mine",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "lithium",
        "display_name": "lithium",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "nickel",
        "display_name": "nickel",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "mineral extraction",
        "display_name": "mineral extraction",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Huawei",
        "display_name": "Huawei",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ZTE",
        "display_name": "ZTE",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Hikvision",
        "display_name": "Hikvision",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Dahua",
        "display_name": "Dahua",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "facial recognition",
        "display_name": "facial recognition",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "AI surveillance",
        "display_name": "AI surveillance",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CCTV",
        "display_name": "CCTV",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "telecom",
        "display_name": "telecom",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "5G",
        "display_name": "5G",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "diplomatic visit",
        "display_name": "diplomatic visit",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ambassador meeting",
        "display_name": "ambassador meeting",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "foreign minister",
        "display_name": "foreign minister",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "official delegation",
        "display_name": "official delegation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "strategic partnership",
        "display_name": "strategic partnership",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "peace talks",
        "display_name": "peace talks",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese influence",
        "display_name": "Chinese influence",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "influence operation",
        "display_name": "influence operation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "propaganda",
        "display_name": "propaganda",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "disinformation",
        "display_name": "disinformation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "misinformation",
        "display_name": "misinformation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "soft power",
        "display_name": "soft power",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "media influence",
        "display_name": "media influence",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "strategic influence",
        "display_name": "strategic influence",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Operation 1027",
        "display_name": "Operation 1027",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Three Brotherhood Alliance",
        "display_name": "Three Brotherhood Alliance",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "MNDAA",
        "display_name": "MNDAA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "TNLA",
        "display_name": "TNLA",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Arakan Army",
        "display_name": "Arakan Army",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ceasefire",
        "display_name": "ceasefire",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "armed clashes",
        "display_name": "armed clashes",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "insurgency",
        "display_name": "insurgency",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ChinaMyanmar",
        "display_name": "#ChinaMyanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Myanmar",
        "display_name": "#Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Burma",
        "display_name": "#Burma",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#BRI",
        "display_name": "#BRI",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#CMEC",
        "display_name": "#CMEC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Kyaukphyu",
        "display_name": "#Kyaukphyu",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar",
        "display_name": "China Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese investment Myanmar",
        "display_name": "Chinese investment Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese activities Myanmar",
        "display_name": "Chinese activities Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China in Myanmar",
        "display_name": "China in Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese project Myanmar",
        "display_name": "Chinese project Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese company Myanmar",
        "display_name": "Chinese company Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese military Myanmar",
        "display_name": "Chinese military Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar pipeline",
        "display_name": "China Myanmar pipeline",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese influence Myanmar",
        "display_name": "Chinese influence Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar border",
        "display_name": "China Myanmar border",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese embassy Myanmar",
        "display_name": "Chinese embassy Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China backs Myanmar junta",
        "display_name": "China backs Myanmar junta",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese support Myanmar",
        "display_name": "Chinese support Myanmar",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar trade",
        "display_name": "China Myanmar trade",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar relations",
        "display_name": "China Myanmar relations",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar cooperation",
        "display_name": "China Myanmar cooperation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar port",
        "display_name": "China Myanmar port",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar SEZ",
        "display_name": "China Myanmar SEZ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Myanmar conflict",
        "display_name": "China Myanmar conflict",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK",
        "display_name": "PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoK",
        "display_name": "PoK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan occupied Jammu and Kashmir",
        "display_name": "Pakistan occupied Jammu and Kashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan occupied Kashmir",
        "display_name": "Pakistan occupied Kashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Azad Kashmir",
        "display_name": "Azad Kashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "AJK",
        "display_name": "AJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gilgit Baltistan",
        "display_name": "Gilgit Baltistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gilgit-Baltistan",
        "display_name": "Gilgit-Baltistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "GB protest",
        "display_name": "GB protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Muzaffarabad",
        "display_name": "Muzaffarabad",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Mirpur",
        "display_name": "Mirpur",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Skardu",
        "display_name": "Skardu",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gilgit",
        "display_name": "Gilgit",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Rawalakot",
        "display_name": "Rawalakot",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kotli",
        "display_name": "Kotli",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Bhimber",
        "display_name": "Bhimber",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Neelum Valley",
        "display_name": "Neelum Valley",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Awami Action Committee",
        "display_name": "Awami Action Committee",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Jammu Kashmir Joint Awami Action Committee",
        "display_name": "Jammu Kashmir Joint Awami Action Committee",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "JAAC",
        "display_name": "JAAC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "GB Awami Action Committee",
        "display_name": "GB Awami Action Committee",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "wheat subsidy protest",
        "display_name": "wheat subsidy protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "flour crisis PoJK",
        "display_name": "flour crisis PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "electricity price protest",
        "display_name": "electricity price protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "tax protest PoJK",
        "display_name": "tax protest PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "shutter down strike",
        "display_name": "shutter down strike",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK shutdown",
        "display_name": "PoJK shutdown",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK unrest",
        "display_name": "PoJK unrest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK protest",
        "display_name": "PoJK protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "GB protest march",
        "display_name": "GB protest march",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kashmiri diaspora protest",
        "display_name": "Kashmiri diaspora protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "human rights violations PoJK",
        "display_name": "human rights violations PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "enforced disappearances PoJK",
        "display_name": "enforced disappearances PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK activists",
        "display_name": "PoJK activists",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK crackdown",
        "display_name": "PoJK crackdown",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK arrests",
        "display_name": "PoJK arrests",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK internet shutdown",
        "display_name": "PoJK internet shutdown",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK curfew",
        "display_name": "PoJK curfew",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK strike",
        "display_name": "PoJK strike",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan army PoJK",
        "display_name": "Pakistan army PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan forces Gilgit Baltistan",
        "display_name": "Pakistan forces Gilgit Baltistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ISI PoJK",
        "display_name": "ISI PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "self-rule PoJK",
        "display_name": "self-rule PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "anti Pakistan protest Kashmir",
        "display_name": "anti Pakistan protest Kashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Diamer Bhasha Dam protest",
        "display_name": "Diamer Bhasha Dam protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CPEC Gilgit Baltistan protest",
        "display_name": "CPEC Gilgit Baltistan protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "land rights Gilgit Baltistan",
        "display_name": "land rights Gilgit Baltistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK political prisoners",
        "display_name": "PoJK political prisoners",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK independence movement",
        "display_name": "PoJK independence movement",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK freedom movement",
        "display_name": "PoJK freedom movement",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Kashmir liberation front PoJK",
        "display_name": "Kashmir liberation front PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK elections",
        "display_name": "PoJK elections",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK assembly",
        "display_name": "PoJK assembly",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK police action",
        "display_name": "PoJK police action",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK tear gas",
        "display_name": "PoJK tear gas",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK baton charge",
        "display_name": "PoJK baton charge",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK sit-in",
        "display_name": "PoJK sit-in",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK hunger strike",
        "display_name": "PoJK hunger strike",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK long march",
        "display_name": "PoJK long march",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#PoJK",
        "display_name": "#PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#PoK",
        "display_name": "#PoK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#AzadKashmir",
        "display_name": "#AzadKashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#GilgitBaltistan",
        "display_name": "#GilgitBaltistan",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#FreeGB",
        "display_name": "#FreeGB",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#FreePoK",
        "display_name": "#FreePoK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#KashmirProtest",
        "display_name": "#KashmirProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#JAAC",
        "display_name": "#JAAC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#AwamiActionCommittee",
        "display_name": "#AwamiActionCommittee",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#PoKProtest",
        "display_name": "#PoKProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#GBProtest",
        "display_name": "#GBProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#StandWithGB",
        "display_name": "#StandWithGB",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#EndOccupation",
        "display_name": "#EndOccupation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#PakistanOccupiedKashmir",
        "display_name": "#PakistanOccupiedKashmir",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#KashmirLiberation",
        "display_name": "#KashmirLiberation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#GBRights",
        "display_name": "#GBRights",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#MuzaffarabadProtest",
        "display_name": "#MuzaffarabadProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#SkarduProtest",
        "display_name": "#SkarduProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#GilgitProtest",
        "display_name": "#GilgitProtest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#WeWantFreedom",
        "display_name": "#WeWantFreedom",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK protest news",
        "display_name": "PoJK protest news",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoK protest news",
        "display_name": "PoK protest news",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gilgit Baltistan protest",
        "display_name": "Gilgit Baltistan protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Azad Kashmir protest",
        "display_name": "Azad Kashmir protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Muzaffarabad protest",
        "display_name": "Muzaffarabad protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK shutter down strike",
        "display_name": "PoJK shutter down strike",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK electricity protest",
        "display_name": "PoJK electricity protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK wheat flour protest",
        "display_name": "PoJK wheat flour protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK crackdown on protesters",
        "display_name": "PoJK crackdown on protesters",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK internet shutdown protest",
        "display_name": "PoJK internet shutdown protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Pakistan army crackdown PoJK",
        "display_name": "Pakistan army crackdown PoJK",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Awami Action Committee protest",
        "display_name": "Awami Action Committee protest",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Gilgit Baltistan long march",
        "display_name": "Gilgit Baltistan long march",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK human rights violation",
        "display_name": "PoJK human rights violation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PoJK enforced disappearance",
        "display_name": "PoJK enforced disappearance",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "စစ်တပ်",
        "display_name": "စစ်တပ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "တပ်မတော်",
        "display_name": "တပ်မတော်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "အကြမ်းဖက်",
        "display_name": "အကြမ်းဖက်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "အကြမ်းဖက်မှု",
        "display_name": "အကြမ်းဖက်မှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "နိုင်ငံရေး",
        "display_name": "နိုင်ငံရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဆန္ဒပြ",
        "display_name": "ဆန္ဒပြ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဆူပူမှု",
        "display_name": "ဆူပူမှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဂြိုဟ်တု",
        "display_name": "ဂြိုဟ်တု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဂြိုဟ်တုပုံရိပ်",
        "display_name": "ဂြိုဟ်တုပုံရိပ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "display_name": "စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လေယာဉ်",
        "display_name": "လေယာဉ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လေကြောင်း",
        "display_name": "လေကြောင်း",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လေယာဉ်ပျံသန်းမှု",
        "display_name": "လေယာဉ်ပျံသန်းမှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လေကြောင်းတိုက်ခိုက်မှု",
        "display_name": "လေကြောင်းတိုက်ခိုက်မှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဒရုန်း",
        "display_name": "ဒရုန်း",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဟယ်လီကော်ပတာ",
        "display_name": "ဟယ်လီကော်ပတာ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "တိုက်လေယာဉ်",
        "display_name": "တိုက်လေယာဉ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဗုံးကြဲ",
        "display_name": "ဗုံးကြဲ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဒုံးကျည်",
        "display_name": "ဒုံးကျည်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လက်နက်",
        "display_name": "လက်နက်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "စစ်ဆင်ရေး",
        "display_name": "စစ်ဆင်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "တိုက်ပွဲ",
        "display_name": "တိုက်ပွဲ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "တပ်ဖွဲ့",
        "display_name": "တပ်ဖွဲ့",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "လုံခြုံရေး",
        "display_name": "လုံခြုံရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ဖမ်းဆီး",
        "display_name": "ဖမ်းဆီး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ပေါက်ကွဲမှု",
        "display_name": "ပေါက်ကွဲမှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "မီးရှို့",
        "display_name": "မီးရှို့",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "နယ်စပ်",
        "display_name": "နယ်စပ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "စစ်ရေး",
        "display_name": "စစ်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ကာကွယ်ရေး",
        "display_name": "ကာကွယ်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ထောက်လှမ်းရေး",
        "display_name": "ထောက်လှမ်းရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#စစ်တပ်",
        "display_name": "#စစ်တပ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#တပ်မတော်",
        "display_name": "#တပ်မတော်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#အကြမ်းဖက်",
        "display_name": "#အကြမ်းဖက်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#အကြမ်းဖက်မှု",
        "display_name": "#အကြမ်းဖက်မှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#နိုင်ငံရေး",
        "display_name": "#နိုင်ငံရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ဆန္ဒပြ",
        "display_name": "#ဆန္ဒပြ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ဆူပူမှု",
        "display_name": "#ဆူပူမှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#စစ်ရေး",
        "display_name": "#စစ်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#စစ်ဆင်ရေး",
        "display_name": "#စစ်ဆင်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#တိုက်ပွဲ",
        "display_name": "#တိုက်ပွဲ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#လေကြောင်းတိုက်ခိုက်မှု",
        "display_name": "#လေကြောင်းတိုက်ခိုက်မှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#လေယာဉ်",
        "display_name": "#လေယာဉ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ဂြိုဟ်တု",
        "display_name": "#ဂြိုဟ်တု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ဒရုန်း",
        "display_name": "#ဒရုန်း",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#လက်နက်",
        "display_name": "#လက်နက်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#လုံခြုံရေး",
        "display_name": "#လုံခြုံရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ဖမ်းဆီး",
        "display_name": "#ဖမ်းဆီး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ပေါက်ကွဲမှု",
        "display_name": "#ပေါက်ကွဲမှု",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#နယ်စပ်",
        "display_name": "#နယ်စပ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ကာကွယ်ရေး",
        "display_name": "#ကာကွယ်ရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ပြည်တွင်းစစ်",
        "display_name": "#ပြည်တွင်းစစ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ငြိမ်းချမ်းရေး",
        "display_name": "#ငြိမ်းချမ်းရေး",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#အရေးပေါ်",
        "display_name": "#အရေးပေါ်",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#သတင်း",
        "display_name": "#သတင်း",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#မြန်မာ",
        "display_name": "#မြန်မာ",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Long March 7A",
        "display_name": "Long March 7A",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Long March 7",
        "display_name": "Long March 7",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CZ-7A",
        "display_name": "CZ-7A",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Wenchang launch",
        "display_name": "Wenchang launch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Wenchang Space Launch Site",
        "display_name": "Wenchang Space Launch Site",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CASC",
        "display_name": "CASC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Aerospace Science and Technology Corporation",
        "display_name": "China Aerospace Science and Technology Corporation",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "ChinaSat 4B",
        "display_name": "ChinaSat 4B",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Zhongxing 4B",
        "display_name": "Zhongxing 4B",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "YF-100 engine",
        "display_name": "YF-100 engine",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "rocket explosion China",
        "display_name": "rocket explosion China",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "launch failure China",
        "display_name": "launch failure China",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chinese rocket anomaly",
        "display_name": "Chinese rocket anomaly",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "flight termination system",
        "display_name": "flight termination system",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China orbital launch failure",
        "display_name": "China orbital launch failure",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Chang'e 7",
        "display_name": "Chang'e 7",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "CALT",
        "display_name": "CALT",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China Academy of Launch Vehicle Technology",
        "display_name": "China Academy of Launch Vehicle Technology",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Jiuquan launch",
        "display_name": "Jiuquan launch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xichang launch",
        "display_name": "Xichang launch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Taiyuan launch",
        "display_name": "Taiyuan launch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "China National Radio launch report",
        "display_name": "China National Radio launch report",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Long March explosion",
        "display_name": "Long March explosion",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#LongMarch7A",
        "display_name": "#LongMarch7A",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#LongMarch",
        "display_name": "#LongMarch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ChinaSat4B",
        "display_name": "#ChinaSat4B",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Zhongxing4B",
        "display_name": "#Zhongxing4B",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Wenchang",
        "display_name": "#Wenchang",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#CASC",
        "display_name": "#CASC",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#RocketLaunch",
        "display_name": "#RocketLaunch",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#LaunchFailure",
        "display_name": "#LaunchFailure",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#SpaceNews",
        "display_name": "#SpaceNews",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#ChinaSpace",
        "display_name": "#ChinaSpace",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Chang'e7",
        "display_name": "#Chang'e7",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#YF100",
        "display_name": "#YF100",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS summit illness",
        "display_name": "BRICS summit illness",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS leader health",
        "display_name": "BRICS leader health",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS leader sick",
        "display_name": "BRICS leader sick",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS health emergency",
        "display_name": "BRICS health emergency",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS summit medical",
        "display_name": "BRICS summit medical",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS leader hospitalized",
        "display_name": "BRICS leader hospitalized",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "BRICS leader collapse",
        "display_name": "BRICS leader collapse",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "world leader sick BRICS",
        "display_name": "world leader sick BRICS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Xi Jinping health BRICS",
        "display_name": "Xi Jinping health BRICS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Putin health BRICS",
        "display_name": "Putin health BRICS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Modi health BRICS",
        "display_name": "Modi health BRICS",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#BRICSSummit",
        "display_name": "#BRICSSummit",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#BRICSHealth",
        "display_name": "#BRICSHealth",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus",
        "display_name": "Powerus",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus drone",
        "display_name": "Powerus drone",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus UAV",
        "display_name": "Powerus UAV",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus defense",
        "display_name": "Powerus defense",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus military",
        "display_name": "Powerus military",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus contract",
        "display_name": "Powerus contract",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus funding",
        "display_name": "Powerus funding",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus technology",
        "display_name": "Powerus technology",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "Powerus US drone maker",
        "display_name": "Powerus US drone maker",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#Powerus",
        "display_name": "#Powerus",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "#PowerusDrone",
        "display_name": "#PowerusDrone",
        "category": "Snapchat Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "manipur violence",
        "display_name": "manipur violence",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "northeast violence",
        "display_name": "northeast violence",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "india protest",
        "display_name": "india protest",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "election violence",
        "display_name": "election violence",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "political violence india",
        "display_name": "political violence india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "cockroach janta party",
        "display_name": "cockroach janta party",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "border tension",
        "display_name": "border tension",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "india pakistan tension",
        "display_name": "india pakistan tension",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "china border",
        "display_name": "china border",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "security alert india",
        "display_name": "security alert india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "terror attack india",
        "display_name": "terror attack india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "bomb blast india",
        "display_name": "bomb blast india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "drone attack india",
        "display_name": "drone attack india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "cross border firing",
        "display_name": "cross border firing",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "kashmir encounter",
        "display_name": "kashmir encounter",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military movement india",
        "display_name": "military movement india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "defence news india",
        "display_name": "defence news india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "disaster india",
        "display_name": "disaster india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "flood india",
        "display_name": "flood india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "landslide india",
        "display_name": "landslide india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "cyclone india",
        "display_name": "cyclone india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "earthquake india",
        "display_name": "earthquake india",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Japanese Ministry of Defense",
        "display_name": "Japanese Ministry of Defense",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Scramble.nl",
        "display_name": "Scramble.nl",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Bakers Institute Energy Map",
        "display_name": "Bakers Institute Energy Map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "IOSI Global",
        "display_name": "IOSI Global",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Australian Strategic Policy Institute",
        "display_name": "Australian Strategic Policy Institute",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ASPI",
        "display_name": "ASPI",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China File",
        "display_name": "China File",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Wassenaar Arrangement",
        "display_name": "Wassenaar Arrangement",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Stockholm International Peace Research Institute",
        "display_name": "Stockholm International Peace Research Institute",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "SIPRI",
        "display_name": "SIPRI",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China Aerospace Studies Institute",
        "display_name": "China Aerospace Studies Institute",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CASI",
        "display_name": "CASI",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CCP Bio Threats Initiative",
        "display_name": "CCP Bio Threats Initiative",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "display_name": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "IDSA",
        "display_name": "IDSA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Vivekananda International Foundation",
        "display_name": "Vivekananda International Foundation",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Indian Council for World Affairs",
        "display_name": "Indian Council for World Affairs",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ICWA",
        "display_name": "ICWA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Centre for Land Warfare Studies",
        "display_name": "Centre for Land Warfare Studies",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CLAWS",
        "display_name": "CLAWS",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "USANAS Foundation",
        "display_name": "USANAS Foundation",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Centre for Joint Warfare Studies",
        "display_name": "Centre for Joint Warfare Studies",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CENJOWS",
        "display_name": "CENJOWS",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "display_name": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "BSMRMU",
        "display_name": "BSMRMU",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Bangladesh Maritime Journal",
        "display_name": "Bangladesh Maritime Journal",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Journal of Engineering and Technology",
        "display_name": "Journal of Engineering and Technology",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "maritimejournal.id",
        "display_name": "maritimejournal.id",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "National Defence University Naval Academy",
        "display_name": "National Defence University Naval Academy",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "DergiPark",
        "display_name": "DergiPark",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Stimson Center",
        "display_name": "Stimson Center",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Geopolitical Monitor",
        "display_name": "Geopolitical Monitor",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "US Naval Institute",
        "display_name": "US Naval Institute",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Seaways",
        "display_name": "Seaways",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "The Navigator",
        "display_name": "The Navigator",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Pakistan Navy War College",
        "display_name": "Pakistan Navy War College",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Sri Lanka Navy Journal",
        "display_name": "Sri Lanka Navy Journal",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Insight Magazine Maldives",
        "display_name": "Insight Magazine Maldives",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Journal of the Indian Ocean Region",
        "display_name": "Journal of the Indian Ocean Region",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China Maritime Reports",
        "display_name": "China Maritime Reports",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Indonesian Maritime Journal",
        "display_name": "Indonesian Maritime Journal",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Jurnal Maritim Indonesia",
        "display_name": "Jurnal Maritim Indonesia",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "MIMA Bulletin",
        "display_name": "MIMA Bulletin",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Royal Thai Naval Academy",
        "display_name": "Royal Thai Naval Academy",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Journal of Naval Science and Engineering",
        "display_name": "Journal of Naval Science and Engineering",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "The Arab Gulf Journal",
        "display_name": "The Arab Gulf Journal",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Scientia Militaria",
        "display_name": "Scientia Militaria",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Western Indian Ocean Marine Science Association",
        "display_name": "Western Indian Ocean Marine Science Association",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "WIOMSA",
        "display_name": "WIOMSA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Intergovernmental Authority on Development",
        "display_name": "Intergovernmental Authority on Development",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "IGAD",
        "display_name": "IGAD",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Naval War College Review",
        "display_name": "Naval War College Review",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "US Naval Institute Proceedings",
        "display_name": "US Naval Institute Proceedings",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Journal of Marine Engineering and Technology",
        "display_name": "Journal of Marine Engineering and Technology",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "maritime security",
        "display_name": "maritime security",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "maritime affairs",
        "display_name": "maritime affairs",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "naval strategy",
        "display_name": "naval strategy",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "naval warfare",
        "display_name": "naval warfare",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "defense studies",
        "display_name": "defense studies",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "strategic affairs",
        "display_name": "strategic affairs",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "geopolitics",
        "display_name": "geopolitics",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "indo pacific",
        "display_name": "indo pacific",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "indian ocean region",
        "display_name": "indian ocean region",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "युद्ध",
        "display_name": "युद्ध",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "सीमा तनाव",
        "display_name": "सीमा तनाव",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "भारतीय सेना",
        "display_name": "भारतीय सेना",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "आतंकी हमला",
        "display_name": "आतंकी हमला",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "कश्मीर मुठभेड़",
        "display_name": "कश्मीर मुठभेड़",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "বাংলা খবর",
        "display_name": "বাংলা খবর",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "সীমান্ত উত্তেজনা",
        "display_name": "সীমান্ত উত্তেজনা",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "போர்",
        "display_name": "போர்",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "தீவிரவாத தாக்குதல்",
        "display_name": "தீவிரவாத தாக்குதல்",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ಯುದ್ಧ",
        "display_name": "ಯುದ್ಧ",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ಗಡಿ ಉದ್ವಿಗ್ನತೆ",
        "display_name": "ಗಡಿ ಉದ್ವಿಗ್ನತೆ",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "People's Liberation Army",
        "display_name": "People's Liberation Army",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA",
        "display_name": "PLA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA training",
        "display_name": "PLA training",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA exercise",
        "display_name": "PLA exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA drill",
        "display_name": "PLA drill",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA modernization",
        "display_name": "PLA modernization",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA combat readiness",
        "display_name": "PLA combat readiness",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA field forces",
        "display_name": "PLA field forces",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA leadership",
        "display_name": "PLA leadership",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA purge",
        "display_name": "PLA purge",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Western Theater Command",
        "display_name": "Western Theater Command",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "WTC",
        "display_name": "WTC",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA Western Theater Command",
        "display_name": "PLA Western Theater Command",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Eastern Theater Command",
        "display_name": "Eastern Theater Command",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Southern Theater Command",
        "display_name": "Southern Theater Command",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "76th Group Army",
        "display_name": "76th Group Army",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "77th Group Army",
        "display_name": "77th Group Army",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "76 GA",
        "display_name": "76 GA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "77 GA",
        "display_name": "77 GA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Group Army PLA",
        "display_name": "Group Army PLA",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Xinjiang Military District",
        "display_name": "Xinjiang Military District",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "XMD",
        "display_name": "XMD",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Tibet Military District",
        "display_name": "Tibet Military District",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "TMD",
        "display_name": "TMD",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Xinjiang military exercise",
        "display_name": "Xinjiang military exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Tibet military exercise",
        "display_name": "Tibet military exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Tibet infrastructure",
        "display_name": "Tibet infrastructure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Xinjiang infrastructure",
        "display_name": "Xinjiang infrastructure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLAAF",
        "display_name": "PLAAF",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "People's Liberation Army Air Force",
        "display_name": "People's Liberation Army Air Force",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLAAF training",
        "display_name": "PLAAF training",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLAAF exercise",
        "display_name": "PLAAF exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese military aviation",
        "display_name": "Chinese military aviation",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "J-20 fighter",
        "display_name": "J-20 fighter",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "J-16 fighter",
        "display_name": "J-16 fighter",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "H-6 bomber",
        "display_name": "H-6 bomber",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLARF",
        "display_name": "PLARF",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "People's Liberation Army Rocket Force",
        "display_name": "People's Liberation Army Rocket Force",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Rocket Force exercise",
        "display_name": "Rocket Force exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese missile launch",
        "display_name": "Chinese missile launch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese missile test",
        "display_name": "Chinese missile test",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "DF-17",
        "display_name": "DF-17",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "DF-21",
        "display_name": "DF-21",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "DF-26",
        "display_name": "DF-26",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "hypersonic missile China",
        "display_name": "hypersonic missile China",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "LAC standoff",
        "display_name": "LAC standoff",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Eastern Ladakh",
        "display_name": "Eastern Ladakh",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Arunachal border",
        "display_name": "Arunachal border",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Doklam",
        "display_name": "Doklam",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Depsang",
        "display_name": "Depsang",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Demchok",
        "display_name": "Demchok",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "India China military",
        "display_name": "India China military",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "India China border infrastructure",
        "display_name": "India China border infrastructure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLAN",
        "display_name": "PLAN",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "People's Liberation Army Navy",
        "display_name": "People's Liberation Army Navy",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese naval exercise",
        "display_name": "Chinese naval exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese warship",
        "display_name": "Chinese warship",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese aircraft carrier",
        "display_name": "Chinese aircraft carrier",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Type 055 destroyer",
        "display_name": "Type 055 destroyer",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Type 052D destroyer",
        "display_name": "Type 052D destroyer",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "South China Sea exercise",
        "display_name": "South China Sea exercise",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Indian Ocean deployment",
        "display_name": "Indian Ocean deployment",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese presence Indian Ocean",
        "display_name": "Chinese presence Indian Ocean",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China maritime infrastructure",
        "display_name": "China maritime infrastructure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese naval base",
        "display_name": "Chinese naval base",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "String of Pearls",
        "display_name": "String of Pearls",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Maritime Silk Road",
        "display_name": "Maritime Silk Road",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "IOR security",
        "display_name": "IOR security",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Gwadar Port",
        "display_name": "Gwadar Port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Hambantota Port",
        "display_name": "Hambantota Port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Kyaukpyu Port",
        "display_name": "Kyaukpyu Port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chittagong Port",
        "display_name": "Chittagong Port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Bagamoyo Port",
        "display_name": "Bagamoyo Port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Djibouti naval base",
        "display_name": "Djibouti naval base",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China port project",
        "display_name": "China port project",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China overseas port",
        "display_name": "China overseas port",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "BRI maritime project",
        "display_name": "BRI maritime project",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Belt and Road Initiative",
        "display_name": "Belt and Road Initiative",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China border infrastructure",
        "display_name": "China border infrastructure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China border village",
        "display_name": "China border village",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China military road",
        "display_name": "China military road",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China military airfield",
        "display_name": "China military airfield",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Tibet railway",
        "display_name": "Tibet railway",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Sichuan Tibet Railway",
        "display_name": "Sichuan Tibet Railway",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Brahmaputra dam",
        "display_name": "Brahmaputra dam",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Yarlung Tsangpo dam",
        "display_name": "Yarlung Tsangpo dam",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment Pakistan",
        "display_name": "Chinese investment Pakistan",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment Nepal",
        "display_name": "Chinese investment Nepal",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment Bangladesh",
        "display_name": "Chinese investment Bangladesh",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment ASEAN",
        "display_name": "Chinese investment ASEAN",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment Africa",
        "display_name": "Chinese investment Africa",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment Europe",
        "display_name": "Chinese investment Europe",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese investment South America",
        "display_name": "Chinese investment South America",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China infrastructure project",
        "display_name": "China infrastructure project",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China energy security",
        "display_name": "China energy security",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China energy transition",
        "display_name": "China energy transition",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China fossil fuel imports",
        "display_name": "China fossil fuel imports",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China renewable energy",
        "display_name": "China renewable energy",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China LNG imports",
        "display_name": "China LNG imports",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China oil imports",
        "display_name": "China oil imports",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China coal production",
        "display_name": "China coal production",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Zhang Youxia",
        "display_name": "Zhang Youxia",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Central Military Commission",
        "display_name": "Central Military Commission",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CMC China",
        "display_name": "CMC China",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese military corruption",
        "display_name": "Chinese military corruption",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "satellite imagery China",
        "display_name": "satellite imagery China",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military satellite imagery",
        "display_name": "military satellite imagery",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China military base",
        "display_name": "China military base",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China border map",
        "display_name": "China border map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "India China border map",
        "display_name": "India China border map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "PLA infographic",
        "display_name": "PLA infographic",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military infographic",
        "display_name": "military infographic",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "strategic map",
        "display_name": "strategic map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Indian Ocean map",
        "display_name": "Indian Ocean map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "South China Sea map",
        "display_name": "South China Sea map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Belt and Road map",
        "display_name": "Belt and Road map",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "解放军",
        "display_name": "解放军",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "中国军队",
        "display_name": "中国军队",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "西部战区",
        "display_name": "西部战区",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "东部战区",
        "display_name": "东部战区",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "南部战区",
        "display_name": "南部战区",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "新疆军区",
        "display_name": "新疆军区",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "西藏军区",
        "display_name": "西藏军区",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "空军演习",
        "display_name": "空军演习",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "火箭军",
        "display_name": "火箭军",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "军事演习",
        "display_name": "军事演习",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "边境建设",
        "display_name": "边境建设",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "中印边境",
        "display_name": "中印边境",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Long March 7A",
        "display_name": "Long March 7A",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Long March 7",
        "display_name": "Long March 7",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CZ-7A",
        "display_name": "CZ-7A",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Wenchang launch",
        "display_name": "Wenchang launch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Wenchang Space Launch Site",
        "display_name": "Wenchang Space Launch Site",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CASC",
        "display_name": "CASC",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China Aerospace Science and Technology Corporation",
        "display_name": "China Aerospace Science and Technology Corporation",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ChinaSat 4B",
        "display_name": "ChinaSat 4B",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Zhongxing 4B",
        "display_name": "Zhongxing 4B",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "YF-100 engine",
        "display_name": "YF-100 engine",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "rocket explosion China",
        "display_name": "rocket explosion China",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "launch failure China",
        "display_name": "launch failure China",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chinese rocket anomaly",
        "display_name": "Chinese rocket anomaly",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "flight termination system",
        "display_name": "flight termination system",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China orbital launch failure",
        "display_name": "China orbital launch failure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chang'e 7",
        "display_name": "Chang'e 7",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "CALT",
        "display_name": "CALT",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China Academy of Launch Vehicle Technology",
        "display_name": "China Academy of Launch Vehicle Technology",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Jiuquan launch",
        "display_name": "Jiuquan launch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Xichang launch",
        "display_name": "Xichang launch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Taiyuan launch",
        "display_name": "Taiyuan launch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "China National Radio launch report",
        "display_name": "China National Radio launch report",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Long March explosion",
        "display_name": "Long March explosion",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "LongMarch7A",
        "display_name": "LongMarch7A",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "LongMarch",
        "display_name": "LongMarch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ChinaSat4B",
        "display_name": "ChinaSat4B",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Zhongxing4B",
        "display_name": "Zhongxing4B",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Wenchang",
        "display_name": "Wenchang",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "RocketLaunch",
        "display_name": "RocketLaunch",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "LaunchFailure",
        "display_name": "LaunchFailure",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "SpaceNews",
        "display_name": "SpaceNews",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ChinaSpace",
        "display_name": "ChinaSpace",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "Chang'e7",
        "display_name": "Chang'e7",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "YF100",
        "display_name": "YF100",
        "category": "Pinterest Keywords",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#Arson",
        "display_name": "#Arson",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#ASPI",
        "display_name": "#ASPI",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "BakersInstituteEnergyMap",
        "display_name": "BakersInstituteEnergyMap",
        "category": "Mastodon Keywords",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "BangabandhuSheikhMujiburRahmanMaritimeUniversity",
        "display_name": "BangabandhuSheikhMujiburRahmanMaritimeUniversity",
        "category": "Mastodon Keywords",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "BeltAndRoadMaritime",
        "display_name": "BeltAndRoadMaritime",
        "category": "Mastodon Keywords",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#BSMRMU",
        "display_name": "#BSMRMU",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CASI",
        "display_name": "#CASI",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CENJOWS",
        "display_name": "#CENJOWS",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "CentralMilitaryCommission",
        "display_name": "CentralMilitaryCommission",
        "category": "Mastodon Keywords",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CLAWS",
        "display_name": "#CLAWS",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CNA",
        "display_name": "#CNA",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CSIS",
        "display_name": "#CSIS",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#DergiPark",
        "display_name": "#DergiPark",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#flood",
        "display_name": "#flood",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#ICWA",
        "display_name": "#ICWA",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#IDSA",
        "display_name": "#IDSA",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#IGAD",
        "display_name": "#IGAD",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#IISS",
        "display_name": "#IISS",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#IPCS",
        "display_name": "#IPCS",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#ORF",
        "display_name": "#ORF",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#PLAAF",
        "display_name": "#PLAAF",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#RUSI",
        "display_name": "#RUSI",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#Scramble.nl",
        "display_name": "#Scramble.nl",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#SIPRI",
        "display_name": "#SIPRI",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#Vandalism",
        "display_name": "#Vandalism",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#প্ৰতিবাদ",
        "display_name": "#প্ৰতিবাদ",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#বন্যা",
        "display_name": "#বন্যা",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#中印边境",
        "display_name": "#中印边境",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#中国军队",
        "display_name": "#中国军队",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#军事演习",
        "display_name": "#军事演习",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#新疆军区",
        "display_name": "#新疆军区",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#西藏军区",
        "display_name": "#西藏军区",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#西部战区",
        "display_name": "#西部战区",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#解放军",
        "display_name": "#解放军",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#hydropower",
        "display_name": "#hydropower",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#drone",
        "display_name": "#drone",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#surveillance",
        "display_name": "#surveillance",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#customs",
        "display_name": "#customs",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#lithium",
        "display_name": "#lithium",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#nickel",
        "display_name": "#nickel",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#telecom",
        "display_name": "#telecom",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#propaganda",
        "display_name": "#propaganda",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#disinformation",
        "display_name": "#disinformation",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#misinformation",
        "display_name": "#misinformation",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#ceasefire",
        "display_name": "#ceasefire",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#insurgency",
        "display_name": "#insurgency",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#Gilgit-Baltistan",
        "display_name": "#Gilgit-Baltistan",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CZ-7A",
        "display_name": "#CZ-7A",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#CALT",
        "display_name": "#CALT",
        "category": "Mastodon Hashtags",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Bangladesh Institute of International and Strategic Studies (BIISS)",
        "display_name": "Bangladesh Institute of International and Strategic Studies (BIISS)",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "China Beige Book",
        "display_name": "China Beige Book",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Japan Ministry of Defence",
        "display_name": "Japan Ministry of Defence",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Maldives National Defence Force",
        "display_name": "Maldives National Defence Force",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Maritime Malaysia",
        "display_name": "Maritime Malaysia",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Royal Thai Navy",
        "display_name": "Royal Thai Navy",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "What's on Weibo",
        "display_name": "What's on Weibo",
        "category": "LinkedIn Sources",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "leader hospitalized BRICS",
        "display_name": "leader hospitalized BRICS",
        "category": "LinkedIn Keywords",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "BRICS 2025 illness",
        "display_name": "BRICS 2025 illness",
        "category": "LinkedIn Keywords",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "BRICS 2026 illness",
        "display_name": "BRICS 2026 illness",
        "category": "LinkedIn Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Afghanistan",
        "display_name": "Afghanistan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Aircraft Carrier",
        "display_name": "Aircraft Carrier",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Anti-submarine warfare",
        "display_name": "Anti-submarine warfare",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Arambai Tengol",
        "display_name": "Arambai Tengol",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Aviation",
        "display_name": "Aviation",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Ballistic missile",
        "display_name": "Ballistic missile",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Baloch insurgency",
        "display_name": "Baloch insurgency",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Baloch Liberation",
        "display_name": "Baloch Liberation",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Baloch movement",
        "display_name": "Baloch movement",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Baloch nationalism",
        "display_name": "Baloch nationalism",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Baloch protests",
        "display_name": "Baloch protests",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Balochistan",
        "display_name": "Balochistan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Balochistan insurgency",
        "display_name": "Balochistan insurgency",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh",
        "display_name": "Bangladesh",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh Air Force",
        "display_name": "Bangladesh Air Force",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh Army",
        "display_name": "Bangladesh Army",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh defence",
        "display_name": "Bangladesh defence",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh elections",
        "display_name": "Bangladesh elections",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh military",
        "display_name": "Bangladesh military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh Navy",
        "display_name": "Bangladesh Navy",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh politics",
        "display_name": "Bangladesh politics",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Bangladesh security",
        "display_name": "Bangladesh security",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BLA",
        "display_name": "BLA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Blue Water Navy",
        "display_name": "Blue Water Navy",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BNA",
        "display_name": "BNA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BNP",
        "display_name": "BNP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Border security Bangladesh",
        "display_name": "Border security Bangladesh",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRA",
        "display_name": "BRA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRAS",
        "display_name": "BRAS",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BYC",
        "display_name": "BYC",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CALT",
        "display_name": "CALT",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CASC",
        "display_name": "CASC",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China Academy of Launch Vehicle Technology",
        "display_name": "China Academy of Launch Vehicle Technology",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China Aerospace",
        "display_name": "China Aerospace",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China Aerospace Science and Technology Corporation",
        "display_name": "China Aerospace Science and Technology Corporation",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China armed forces",
        "display_name": "China armed forces",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China defence",
        "display_name": "China defence",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China military",
        "display_name": "China military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China military modernization",
        "display_name": "China military modernization",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China Pakistan",
        "display_name": "China Pakistan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "China_irl",
        "display_name": "China_irl",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese aerospace",
        "display_name": "Chinese aerospace",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese Air Force",
        "display_name": "Chinese Air Force",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese destroyer",
        "display_name": "Chinese destroyer",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese frigate",
        "display_name": "Chinese frigate",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese launch",
        "display_name": "Chinese launch",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese military",
        "display_name": "Chinese military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese missile",
        "display_name": "Chinese missile",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese Navy",
        "display_name": "Chinese Navy",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese satellite",
        "display_name": "Chinese satellite",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese shipbuilding",
        "display_name": "Chinese shipbuilding",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Chinese submarine",
        "display_name": "Chinese submarine",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "COCOMI",
        "display_name": "COCOMI",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CombatFootage",
        "display_name": "CombatFootage",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Corvette",
        "display_name": "Corvette",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Counter Terrorism",
        "display_name": "Counter Terrorism",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CPEC",
        "display_name": "CPEC",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CredibleDefense",
        "display_name": "CredibleDefense",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CSIS China",
        "display_name": "CSIS China",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "DAESH",
        "display_name": "DAESH",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Defence technology",
        "display_name": "Defence technology",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Destroyer",
        "display_name": "Destroyer",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "DF-21D",
        "display_name": "DF-21D",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "East Africa",
        "display_name": "East Africa",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "East China Sea",
        "display_name": "East China Sea",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ETIM",
        "display_name": "ETIM",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Extremism",
        "display_name": "Extremism",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Freedom of Navigation",
        "display_name": "Freedom of Navigation",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Frigate",
        "display_name": "Frigate",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Geopolitics",
        "display_name": "Geopolitics",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Gwadar",
        "display_name": "Gwadar",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "H-20",
        "display_name": "H-20",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "HuA",
        "display_name": "HuA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "HuT",
        "display_name": "HuT",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Hypersonic missile",
        "display_name": "Hypersonic missile",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "IJG",
        "display_name": "IJG",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "IMU",
        "display_name": "IMU",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "India",
        "display_name": "India",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Indian Ocean",
        "display_name": "Indian Ocean",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "IndianDefense",
        "display_name": "IndianDefense",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Indonesia",
        "display_name": "Indonesia",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Insurgency Northeast India",
        "display_name": "Insurgency Northeast India",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Intelligence",
        "display_name": "Intelligence",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Iran",
        "display_name": "Iran",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ISIS-K",
        "display_name": "ISIS-K",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ISKP",
        "display_name": "ISKP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Islamic State Khorasan",
        "display_name": "Islamic State Khorasan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ISPR",
        "display_name": "ISPR",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "J-20",
        "display_name": "J-20",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "J-35",
        "display_name": "J-35",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Jaish ul Adl",
        "display_name": "Jaish ul Adl",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Jamaat e Islami",
        "display_name": "Jamaat e Islami",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "JuA",
        "display_name": "JuA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Jundullah",
        "display_name": "Jundullah",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "KatmanduFloods",
        "display_name": "KatmanduFloods",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "KCP",
        "display_name": "KCP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Kuki",
        "display_name": "Kuki",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "KYKL",
        "display_name": "KYKL",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "LeJ",
        "display_name": "LeJ",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "LessCredibleDefence",
        "display_name": "LessCredibleDefence",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Maharang Baloch",
        "display_name": "Maharang Baloch",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Malaysia",
        "display_name": "Malaysia",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Maldives",
        "display_name": "Maldives",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Manipur conflict",
        "display_name": "Manipur conflict",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Maritime Domain Awareness",
        "display_name": "Maritime Domain Awareness",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Maritime Security",
        "display_name": "Maritime Security",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Meitei",
        "display_name": "Meitei",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Militant groups",
        "display_name": "Militant groups",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Military",
        "display_name": "Military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Military satellite",
        "display_name": "Military satellite",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Missing persons Balochistan",
        "display_name": "Missing persons Balochistan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Myanmar conflict",
        "display_name": "Myanmar conflict",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naga",
        "display_name": "Naga",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naval",
        "display_name": "Naval",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naval Base",
        "display_name": "Naval Base",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naval Exercise",
        "display_name": "Naval Exercise",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naval Modernization",
        "display_name": "Naval Modernization",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Naval War College",
        "display_name": "Naval War College",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalDisasterNews",
        "display_name": "NepalDisasterNews",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalDisasterResponse",
        "display_name": "NepalDisasterResponse",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalEmergency",
        "display_name": "NepalEmergency",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalFloodAlert",
        "display_name": "NepalFloodAlert",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalFloodDisaster",
        "display_name": "NepalFloodDisaster",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalFloodRelief",
        "display_name": "NepalFloodRelief",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalFloods",
        "display_name": "NepalFloods",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalFloodUpdate",
        "display_name": "NepalFloodUpdate",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalLandslide",
        "display_name": "NepalLandslide",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalMonsoon",
        "display_name": "NepalMonsoon",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalMudslide",
        "display_name": "NepalMudslide",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalNaturalDisaster",
        "display_name": "NepalNaturalDisaster",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalRainfall",
        "display_name": "NepalRainfall",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NepalWeatherAlert",
        "display_name": "NepalWeatherAlert",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NonCredibleDefense",
        "display_name": "NonCredibleDefense",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "NSCN",
        "display_name": "NSCN",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Nuclear storage",
        "display_name": "Nuclear storage",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "OSINT",
        "display_name": "OSINT",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "PAF, Pakistan Air Force, jets, F-16, JF-17, pilots, aircraft, defense, aviation, airbases, radar",
        "display_name": "PAF, Pakistan Air Force, jets, F-16, JF-17, pilots, aircraft, defense, aviation, airbases, radar",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pak China",
        "display_name": "Pak China",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pak Navy",
        "display_name": "Pak Navy",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan Air Force",
        "display_name": "Pakistan Air Force",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan Army",
        "display_name": "Pakistan Army",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan defence",
        "display_name": "Pakistan defence",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan military",
        "display_name": "Pakistan military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan military exercise",
        "display_name": "Pakistan military exercise",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan missile",
        "display_name": "Pakistan missile",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan nuclear",
        "display_name": "Pakistan nuclear",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan security",
        "display_name": "Pakistan security",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "pakistan security situation,",
        "display_name": "pakistan security situation,",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Pakistan strategic forces",
        "display_name": "Pakistan strategic forces",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "PLA Navy",
        "display_name": "PLA Navy",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "PLA Rocket Force",
        "display_name": "PLA Rocket Force",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "PLAN",
        "display_name": "PLAN",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Port Development",
        "display_name": "Port Development",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "PREPAK",
        "display_name": "PREPAK",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "RAB",
        "display_name": "RAB",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Rocket Force",
        "display_name": "Rocket Force",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Satellite",
        "display_name": "Satellite",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Satellite imagery",
        "display_name": "Satellite imagery",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Satellite imagery China",
        "display_name": "Satellite imagery China",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Sea Lines of Communication",
        "display_name": "Sea Lines of Communication",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Shipbuilding",
        "display_name": "Shipbuilding",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "South China Sea",
        "display_name": "South China Sea",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Space",
        "display_name": "Space",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Space launch",
        "display_name": "Space launch",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "SpaceXLounge",
        "display_name": "SpaceXLounge",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Sri Lanka",
        "display_name": "Sri Lanka",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "SSP",
        "display_name": "SSP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Strategic Studies",
        "display_name": "Strategic Studies",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Submarine",
        "display_name": "Submarine",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Taiwan Strait",
        "display_name": "Taiwan Strait",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Tehreek-e-Taliban Pakistan",
        "display_name": "Tehreek-e-Taliban Pakistan",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Thailand",
        "display_name": "Thailand",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "TIP",
        "display_name": "TIP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "TTP",
        "display_name": "TTP",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Turkey",
        "display_name": "Turkey",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Type 052D",
        "display_name": "Type 052D",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Type 055",
        "display_name": "Type 055",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Type 075",
        "display_name": "Type 075",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Type 076",
        "display_name": "Type 076",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ULFA",
        "display_name": "ULFA",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "UNLF",
        "display_name": "UNLF",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "VIF",
        "display_name": "VIF",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "War",
        "display_name": "War",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "War College",
        "display_name": "War College",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "WarCollege",
        "display_name": "WarCollege",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Warship",
        "display_name": "Warship",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Y-20",
        "display_name": "Y-20",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRICS summit illness",
        "display_name": "BRICS summit illness",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRICS leader sick",
        "display_name": "BRICS leader sick",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRICS health emergency",
        "display_name": "BRICS health emergency",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRICS summit medical",
        "display_name": "BRICS summit medical",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "BRICS leader collapse",
        "display_name": "BRICS leader collapse",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "world leader sick BRICS",
        "display_name": "world leader sick BRICS",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Xi Jinping health BRICS",
        "display_name": "Xi Jinping health BRICS",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Putin health BRICS",
        "display_name": "Putin health BRICS",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Modi health BRICS",
        "display_name": "Modi health BRICS",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "#BRICSHealth",
        "display_name": "#BRICSHealth",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus",
        "display_name": "Powerus",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus UAV",
        "display_name": "Powerus UAV",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus defense",
        "display_name": "Powerus defense",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus military",
        "display_name": "Powerus military",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus contract",
        "display_name": "Powerus contract",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus funding",
        "display_name": "Powerus funding",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus technology",
        "display_name": "Powerus technology",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Powerus US drone maker",
        "display_name": "Powerus US drone maker",
        "category": "Reddit Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Malaysia Institute of Maritime Affairs",
        "display_name": "Malaysia Institute of Maritime Affairs",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Malaysia National Defence",
        "display_name": "Malaysia National Defence",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Official CLAWS",
        "display_name": "Official CLAWS",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "RAND Corporation",
        "display_name": "RAND Corporation",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Royal Thai Naval Academy",
        "display_name": "Royal Thai Naval Academy",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Scientia Militaria",
        "display_name": "Scientia Militaria",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Some PLA OSINT",
        "display_name": "Some PLA OSINT",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "US Naval Institute",
        "display_name": "US Naval Institute",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Zaobao SG",
        "display_name": "Zaobao SG",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "SpaceNews",
        "display_name": "SpaceNews",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Space.com (VideoFromSpace)",
        "display_name": "Space.com (VideoFromSpace)",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Interesting Engineering",
        "display_name": "Interesting Engineering",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Spaceflight Now",
        "display_name": "Spaceflight Now",
        "category": "YouTube Channels",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "#ChinaMyanmar",
        "display_name": "#ChinaMyanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "#PoJK",
        "display_name": "#PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "#pojkprotests",
        "display_name": "#pojkprotests",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "#PoK",
        "display_name": "#PoK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "5G",
        "display_name": "5G",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "AI surveillance",
        "display_name": "AI surveillance",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "AJK",
        "display_name": "AJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ambassador meeting",
        "display_name": "ambassador meeting",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "anti Pakistan protest Kashmir",
        "display_name": "anti Pakistan protest Kashmir",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Arakan Army",
        "display_name": "Arakan Army",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "armed clashes",
        "display_name": "armed clashes",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "army",
        "display_name": "army",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Arson",
        "display_name": "Arson",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ASPI",
        "display_name": "ASPI",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Assam flood",
        "display_name": "Assam flood",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Awami Action Committee",
        "display_name": "Awami Action Committee",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Awami Action Committee protest",
        "display_name": "Awami Action Committee protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Azad Kashmir",
        "display_name": "Azad Kashmir",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Azad Kashmir protest",
        "display_name": "Azad Kashmir protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "display_name": "Bangabandhu Sheikh Mujibur Rahman Maritime University",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Beijing",
        "display_name": "Beijing",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Belt and Road Initiative",
        "display_name": "Belt and Road Initiative",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Bhimber",
        "display_name": "Bhimber",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "bilateral cooperation",
        "display_name": "bilateral cooperation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "border crossing",
        "display_name": "border crossing",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "border trade",
        "display_name": "border trade",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Border village China",
        "display_name": "Border village China",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "BRI",
        "display_name": "BRI",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "BRI infrastructure",
        "display_name": "BRI infrastructure",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "BSMRMU",
        "display_name": "BSMRMU",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Burma",
        "display_name": "Burma",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "campaign protest",
        "display_name": "campaign protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Carnegie India",
        "display_name": "Carnegie India",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CASI",
        "display_name": "CASI",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CCP",
        "display_name": "CCP",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CCTV",
        "display_name": "CCTV",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ceasefire",
        "display_name": "ceasefire",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CENJOWS",
        "display_name": "CENJOWS",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Center for Naval Analyses",
        "display_name": "Center for Naval Analyses",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Center for Strategic and International Studies",
        "display_name": "Center for Strategic and International Studies",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Centre for Joint Warfare Studies",
        "display_name": "Centre for Joint Warfare Studies",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Centre for Land Warfare Studies",
        "display_name": "Centre for Land Warfare Studies",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China",
        "display_name": "China",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Aerospace Studies Institute",
        "display_name": "China Aerospace Studies Institute",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China backs Myanmar junta",
        "display_name": "China backs Myanmar junta",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China File",
        "display_name": "China File",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China in Myanmar",
        "display_name": "China in Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China infrastructure investment",
        "display_name": "China infrastructure investment",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China military logistics",
        "display_name": "China military logistics",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar",
        "display_name": "China Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar border",
        "display_name": "China Myanmar border",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar conflict",
        "display_name": "China Myanmar conflict",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar cooperation",
        "display_name": "China Myanmar cooperation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar Corridor",
        "display_name": "China Myanmar Corridor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar Economic Corridor",
        "display_name": "China Myanmar Economic Corridor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar Gas Pipeline",
        "display_name": "China Myanmar Gas Pipeline",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar Oil Pipeline",
        "display_name": "China Myanmar Oil Pipeline",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar pipeline",
        "display_name": "China Myanmar pipeline",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar port",
        "display_name": "China Myanmar port",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar Railway",
        "display_name": "China Myanmar Railway",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar relations",
        "display_name": "China Myanmar relations",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar SEZ",
        "display_name": "China Myanmar SEZ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China Myanmar trade",
        "display_name": "China Myanmar trade",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "China port infrastructure",
        "display_name": "China port infrastructure",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese",
        "display_name": "Chinese",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese activities Myanmar",
        "display_name": "Chinese activities Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese Ambassador",
        "display_name": "Chinese Ambassador",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese arms",
        "display_name": "Chinese arms",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese business",
        "display_name": "Chinese business",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese company",
        "display_name": "Chinese company",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese company Myanmar",
        "display_name": "Chinese company Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese contractor",
        "display_name": "Chinese contractor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese defence policy",
        "display_name": "Chinese defence policy",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese Embassy",
        "display_name": "Chinese Embassy",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese embassy Myanmar",
        "display_name": "Chinese embassy Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese influence",
        "display_name": "Chinese influence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese influence Myanmar",
        "display_name": "Chinese influence Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese investment",
        "display_name": "Chinese investment",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese investment Myanmar",
        "display_name": "Chinese investment Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese military modernization",
        "display_name": "Chinese military modernization",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese military Myanmar",
        "display_name": "Chinese military Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese missile program",
        "display_name": "Chinese missile program",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese naval expansion",
        "display_name": "Chinese naval expansion",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese naval presence",
        "display_name": "Chinese naval presence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese overseas infrastructure",
        "display_name": "Chinese overseas infrastructure",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese project Myanmar",
        "display_name": "Chinese project Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese strategic deterrence",
        "display_name": "Chinese strategic deterrence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese support Myanmar",
        "display_name": "Chinese support Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinese weapons",
        "display_name": "Chinese weapons",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Chinshwehaw",
        "display_name": "Chinshwehaw",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CLAWS",
        "display_name": "CLAWS",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CMEC",
        "display_name": "CMEC",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "communal violence",
        "display_name": "communal violence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "copper mine",
        "display_name": "copper mine",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CPEC Gilgit Baltistan protest",
        "display_name": "CPEC Gilgit Baltistan protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "cross border trade",
        "display_name": "cross border trade",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CSIS",
        "display_name": "CSIS",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "customs",
        "display_name": "customs",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "cyber fraud India",
        "display_name": "cyber fraud India",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Dahua",
        "display_name": "Dahua",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "defense studies",
        "display_name": "defense studies",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "DergiPark",
        "display_name": "DergiPark",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "development project",
        "display_name": "development project",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Diamer Bhasha Dam protest",
        "display_name": "Diamer Bhasha Dam protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "diplomatic visit",
        "display_name": "diplomatic visit",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "disaster India",
        "display_name": "disaster India",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "disinformation",
        "display_name": "disinformation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "drone",
        "display_name": "drone",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Eastern Theater Command",
        "display_name": "Eastern Theater Command",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "economic cooperation",
        "display_name": "economic cooperation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "election violence",
        "display_name": "election violence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "electricity price protest",
        "display_name": "electricity price protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "electricity project",
        "display_name": "electricity project",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "enforced disappearances PoJK",
        "display_name": "enforced disappearances PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "english news",
        "display_name": "english news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "extremist violence",
        "display_name": "extremist violence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "facial recognition",
        "display_name": "facial recognition",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "fighter jet",
        "display_name": "fighter jet",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Fighting between Taliban and anti-Taliban forces along Afghan-Pak and Afghan-Tajik borders",
        "display_name": "Fighting between Taliban and anti-Taliban forces along Afghan-Pak and Afghan-Tajik borders",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "flood",
        "display_name": "flood",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "flour crisis PoJK",
        "display_name": "flour crisis PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "foreign investment",
        "display_name": "foreign investment",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "foreign minister",
        "display_name": "foreign minister",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Foreign Policy",
        "display_name": "Foreign Policy",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "gas pipeline",
        "display_name": "gas pipeline",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "GB Awami Action Committee",
        "display_name": "GB Awami Action Committee",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "GB protest",
        "display_name": "GB protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "GB protest march",
        "display_name": "GB protest march",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Geopolitical Monitor",
        "display_name": "Geopolitical Monitor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Gilgit",
        "display_name": "Gilgit",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Gilgit Baltistan",
        "display_name": "Gilgit Baltistan",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Gilgit Baltistan long march",
        "display_name": "Gilgit Baltistan long march",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Gilgit Baltistan protest",
        "display_name": "Gilgit Baltistan protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Gilgit-Baltistan",
        "display_name": "Gilgit-Baltistan",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Hikvision",
        "display_name": "Hikvision",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Huawei",
        "display_name": "Huawei",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Hudson Institute",
        "display_name": "Hudson Institute",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "human rights violations PoJK",
        "display_name": "human rights violations PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "hydropower",
        "display_name": "hydropower",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Hypersonic weapons China",
        "display_name": "Hypersonic weapons China",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ICWA",
        "display_name": "ICWA",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "IDSA",
        "display_name": "IDSA",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "IISS",
        "display_name": "IISS",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India border security",
        "display_name": "India border security",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India China border",
        "display_name": "India China border",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India China relations",
        "display_name": "India China relations",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India defence",
        "display_name": "India defence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India defence manufacturing",
        "display_name": "India defence manufacturing",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India national security",
        "display_name": "India national security",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India Pakistan tensions",
        "display_name": "India Pakistan tensions",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "indian ocean region",
        "display_name": "indian ocean region",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Indian Ocean security",
        "display_name": "Indian Ocean security",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "indo pacific",
        "display_name": "indo pacific",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "industrial park",
        "display_name": "industrial park",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "influence operation",
        "display_name": "influence operation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "infrastructure project",
        "display_name": "infrastructure project",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Institute of Peace and Conflict Studies",
        "display_name": "Institute of Peace and Conflict Studies",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "insurgency",
        "display_name": "insurgency",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Intergovernmental Authority on Development",
        "display_name": "Intergovernmental Authority on Development",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "investment project",
        "display_name": "investment project",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "IPCS",
        "display_name": "IPCS",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ISI PoJK",
        "display_name": "ISI PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "israel iran war",
        "display_name": "israel iran war",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "JAAC",
        "display_name": "JAAC",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "jade mining",
        "display_name": "jade mining",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Jamestown Foundation",
        "display_name": "Jamestown Foundation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Jammu Kashmir Joint Awami Action Committee",
        "display_name": "Jammu Kashmir Joint Awami Action Committee",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Junta",
        "display_name": "Junta",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kachin",
        "display_name": "Kachin",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kashmir encounter",
        "display_name": "Kashmir encounter",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kashmir liberation front PoJK",
        "display_name": "Kashmir liberation front PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kashmiri diaspora protest",
        "display_name": "Kashmiri diaspora protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kokang",
        "display_name": "Kokang",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kotli",
        "display_name": "Kotli",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kyaukphyu",
        "display_name": "Kyaukphyu",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kyaukphyu Port",
        "display_name": "Kyaukphyu Port",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Kyaukphyu SEZ",
        "display_name": "Kyaukphyu SEZ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "land rights Gilgit Baltistan",
        "display_name": "land rights Gilgit Baltistan",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "latest news",
        "display_name": "latest news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "lithium",
        "display_name": "lithium",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "LNG",
        "display_name": "LNG",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "logistics corridor",
        "display_name": "logistics corridor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Lowy Institute",
        "display_name": "Lowy Institute",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Mandalay",
        "display_name": "Mandalay",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "maritime affairs",
        "display_name": "maritime affairs",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "maritime security",
        "display_name": "maritime security",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "media influence",
        "display_name": "media influence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "military aid",
        "display_name": "military aid",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "military cooperation",
        "display_name": "military cooperation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "mineral extraction",
        "display_name": "mineral extraction",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Mirpur",
        "display_name": "Mirpur",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "misinformation",
        "display_name": "misinformation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "MNDAA",
        "display_name": "MNDAA",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Muse",
        "display_name": "Muse",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Muse Mandalay Railway",
        "display_name": "Muse Mandalay Railway",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Muzaffarabad",
        "display_name": "Muzaffarabad",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Muzaffarabad protest",
        "display_name": "Muzaffarabad protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Myanmar",
        "display_name": "Myanmar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "National Defence University Naval Academy",
        "display_name": "National Defence University Naval Academy",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "National Unity Government",
        "display_name": "National Unity Government",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "naval strategy",
        "display_name": "naval strategy",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "naval warfare",
        "display_name": "naval warfare",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Naypyidaw",
        "display_name": "Naypyidaw",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Neelum Valley",
        "display_name": "Neelum Valley",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "NEET protest",
        "display_name": "NEET protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Nepal flood death",
        "display_name": "Nepal flood death",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Nepal Floods",
        "display_name": "Nepal Floods",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "news today",
        "display_name": "news today",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "news updates",
        "display_name": "news updates",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "nickel",
        "display_name": "nickel",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "NUG",
        "display_name": "NUG",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "official delegation",
        "display_name": "official delegation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "oil pipeline",
        "display_name": "oil pipeline",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Operation 1027",
        "display_name": "Operation 1027",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ORF",
        "display_name": "ORF",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan army crackdown PoJK",
        "display_name": "Pakistan army crackdown PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan army PoJK",
        "display_name": "Pakistan army PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan forces Gilgit Baltistan",
        "display_name": "Pakistan forces Gilgit Baltistan",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan occupied Jammu and Kashmir",
        "display_name": "Pakistan occupied Jammu and Kashmir",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan occupied Kashmir",
        "display_name": "Pakistan occupied Kashmir",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Payment fraud",
        "display_name": "Payment fraud",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "peace talks",
        "display_name": "peace talks",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PLA",
        "display_name": "PLA",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PLA reform",
        "display_name": "PLA reform",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK",
        "display_name": "PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK activists",
        "display_name": "PoJK activists",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK arrests",
        "display_name": "PoJK arrests",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK assembly",
        "display_name": "PoJK assembly",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK baton charge",
        "display_name": "PoJK baton charge",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK crackdown",
        "display_name": "PoJK crackdown",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK crackdown on protesters",
        "display_name": "PoJK crackdown on protesters",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK curfew",
        "display_name": "PoJK curfew",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK elections",
        "display_name": "PoJK elections",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK electricity protest",
        "display_name": "PoJK electricity protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK enforced disappearance",
        "display_name": "PoJK enforced disappearance",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK freedom movement",
        "display_name": "PoJK freedom movement",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK human rights violation",
        "display_name": "PoJK human rights violation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK hunger strike",
        "display_name": "PoJK hunger strike",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK independence movement",
        "display_name": "PoJK independence movement",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK internet shutdown",
        "display_name": "PoJK internet shutdown",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK internet shutdown protest",
        "display_name": "PoJK internet shutdown protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK long march",
        "display_name": "PoJK long march",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK police action",
        "display_name": "PoJK police action",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK political prisoners",
        "display_name": "PoJK political prisoners",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK protest",
        "display_name": "PoJK protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK protest news",
        "display_name": "PoJK protest news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK shutdown",
        "display_name": "PoJK shutdown",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK shutter down strike",
        "display_name": "PoJK shutter down strike",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK sit-in",
        "display_name": "PoJK sit-in",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK strike",
        "display_name": "PoJK strike",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK tear gas",
        "display_name": "PoJK tear gas",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK unrest",
        "display_name": "PoJK unrest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoJK wheat flour protest",
        "display_name": "PoJK wheat flour protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoK",
        "display_name": "PoK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PoK protest news",
        "display_name": "PoK protest news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "power project",
        "display_name": "power project",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "PRC",
        "display_name": "PRC",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "propaganda",
        "display_name": "propaganda",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "protest",
        "display_name": "protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "radar",
        "display_name": "radar",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "rare earth",
        "display_name": "rare earth",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "rare earth mining",
        "display_name": "rare earth mining",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Rawalakot",
        "display_name": "Rawalakot",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "riot India",
        "display_name": "riot India",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Royal United Services Institute",
        "display_name": "Royal United Services Institute",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Ruili",
        "display_name": "Ruili",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "RUSI",
        "display_name": "RUSI",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "SAC",
        "display_name": "SAC",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Scramble.nl",
        "display_name": "Scramble.nl",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "seaspark",
        "display_name": "seaspark",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "security cooperation",
        "display_name": "security cooperation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "self-rule PoJK",
        "display_name": "self-rule PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Shan State",
        "display_name": "Shan State",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "shutter down strike",
        "display_name": "shutter down strike",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Skardu",
        "display_name": "Skardu",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "sky dragon",
        "display_name": "sky dragon",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "soft power",
        "display_name": "soft power",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "South China Sea security",
        "display_name": "South China Sea security",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Southern Theater Command",
        "display_name": "Southern Theater Command",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Special Economic Zone",
        "display_name": "Special Economic Zone",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "State Administration Council",
        "display_name": "State Administration Council",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Stimson Center",
        "display_name": "Stimson Center",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Stockholm International Peace Research Institute",
        "display_name": "Stockholm International Peace Research Institute",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "strategic affairs",
        "display_name": "strategic affairs",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "strategic influence",
        "display_name": "strategic influence",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "strategic partnership",
        "display_name": "strategic partnership",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Strikes in Afghanistan during next 2 days",
        "display_name": "Strikes in Afghanistan during next 2 days",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "surveillance",
        "display_name": "surveillance",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Tatmadaw",
        "display_name": "Tatmadaw",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "tax protest PoJK",
        "display_name": "tax protest PoJK",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "telecom",
        "display_name": "telecom",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Three Brotherhood Alliance",
        "display_name": "Three Brotherhood Alliance",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Tibet infrastructure",
        "display_name": "Tibet infrastructure",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "times news",
        "display_name": "times news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "TNLA",
        "display_name": "TNLA",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "toi",
        "display_name": "toi",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "toi news",
        "display_name": "toi news",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "trade agreement",
        "display_name": "trade agreement",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "trade corridor",
        "display_name": "trade corridor",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "UAV",
        "display_name": "UAV",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "UCAV",
        "display_name": "UCAV",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "USANAS Foundation",
        "display_name": "USANAS Foundation",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Vandalism",
        "display_name": "Vandalism",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "vladimir putin",
        "display_name": "vladimir putin",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Wa State",
        "display_name": "Wa State",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Wassenaar Arrangement",
        "display_name": "Wassenaar Arrangement",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "wheat subsidy protest",
        "display_name": "wheat subsidy protest",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Xi Jinping",
        "display_name": "Xi Jinping",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Xinjiang infrastructure",
        "display_name": "Xinjiang infrastructure",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Yangon",
        "display_name": "Yangon",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Yunnan",
        "display_name": "Yunnan",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ZTE",
        "display_name": "ZTE",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "উত্তর পূর্ব ভারত",
        "display_name": "উত্তর পূর্ব ভারত",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "উত্তর-পূর্ব সংঘর্ষ",
        "display_name": "উত্তর-পূর্ব সংঘর্ষ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ত্রিপুরা হিংসা",
        "display_name": "ত্রিপুরা হিংসা",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "নাগালেণ্ড সুৰক্ষা",
        "display_name": "নাগালেণ্ড সুৰক্ষা",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "প্ৰতিবাদ",
        "display_name": "প্ৰতিবাদ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "বন্যা",
        "display_name": "বন্যা",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ကာကွယ်ရေး",
        "display_name": "ကာကွယ်ရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဂြိုဟ်တု",
        "display_name": "ဂြိုဟ်တု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဂြိုဟ်တုပုံရိပ်",
        "display_name": "ဂြိုဟ်တုပုံရိပ်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "စစ်ဆင်ရေး",
        "display_name": "စစ်ဆင်ရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "စစ်တပ်",
        "display_name": "စစ်တပ်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "စစ်ရေး",
        "display_name": "စစ်ရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "display_name": "စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဆန္ဒပြ",
        "display_name": "ဆန္ဒပြ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဆူပူမှု",
        "display_name": "ဆူပူမှု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "တပ်ဖွဲ့",
        "display_name": "တပ်ဖွဲ့",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "တပ်မတော်",
        "display_name": "တပ်မတော်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "တိုက်ပွဲ",
        "display_name": "တိုက်ပွဲ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "တိုက်လေယာဉ်",
        "display_name": "တိုက်လေယာဉ်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ထောက်လှမ်းရေး",
        "display_name": "ထောက်လှမ်းရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဒရုန်း",
        "display_name": "ဒရုန်း",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဒုံးကျည်",
        "display_name": "ဒုံးကျည်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "နယ်စပ်",
        "display_name": "နယ်စပ်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "နိုင်ငံရေး",
        "display_name": "နိုင်ငံရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ပေါက်ကွဲမှု",
        "display_name": "ပေါက်ကွဲမှု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဖမ်းဆီး",
        "display_name": "ဖမ်းဆီး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဗုံးကြဲ",
        "display_name": "ဗုံးကြဲ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "မီးရှို့",
        "display_name": "မီးရှို့",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လက်နက်",
        "display_name": "လက်နက်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လုံခြုံရေး",
        "display_name": "လုံခြုံရေး",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လေကြောင်း",
        "display_name": "လေကြောင်း",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လေကြောင်းတိုက်ခိုက်မှု",
        "display_name": "လေကြောင်းတိုက်ခိုက်မှု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လေယာဉ်",
        "display_name": "လေယာဉ်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "လေယာဉ်ပျံသန်းမှု",
        "display_name": "လေယာဉ်ပျံသန်းမှု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ဟယ်လီကော်ပတာ",
        "display_name": "ဟယ်လီကော်ပတာ",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "အကြမ်းဖက်",
        "display_name": "အကြမ်းဖက်",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "အကြမ်းဖက်မှု",
        "display_name": "အကြမ်းဖက်မှု",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "中印边境",
        "display_name": "中印边境",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "中国军队",
        "display_name": "中国军队",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "军事演习",
        "display_name": "军事演习",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "新疆军区",
        "display_name": "新疆军区",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "西藏军区",
        "display_name": "西藏军区",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "西部战区",
        "display_name": "西部战区",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "解放军",
        "display_name": "解放军",
        "category": "YouTube Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bamar People's Liberation Army - BPLA",
        "display_name": "Bamar People's Liberation Army - BPLA",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ပြည်သူ့လွတ်မြောက်ရေးတပ်မတော် - People's Liberation Army - PLA",
        "display_name": "ပြည်သူ့လွတ်မြောက်ရေးတပ်မတော် - People's Liberation Army - PLA",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ABSDF",
        "display_name": "ABSDF",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Dr. Shafiqur Rahman",
        "display_name": "Dr. Shafiqur Rahman",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "InFormative Bangla",
        "display_name": "InFormative Bangla",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "The Bangladesh Today",
        "display_name": "The Bangladesh Today",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Military News",
        "display_name": "Bangladesh Military News",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Arakan National Council-ANC",
        "display_name": "Arakan National Council-ANC",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Progressive Karenni People Force - PKPF",
        "display_name": "Progressive Karenni People Force - PKPF",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Zokhan Khual",
        "display_name": "Zokhan Khual",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Kachin National Organization",
        "display_name": "Kachin National Organization",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ချင်းပြည်သူ့အစည်းအရုံး/ချင်းပြည်သူ့တပ်မတော်",
        "display_name": "ချင်းပြည်သူ့အစည်းအရုံး/ချင်းပြည်သူ့တပ်မတော်",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Spring Revolution News",
        "display_name": "Spring Revolution News",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Pakistan Narrative",
        "display_name": "Pakistan Narrative",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Scramble Magazine",
        "display_name": "Scramble Magazine",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Malaysia Maritime",
        "display_name": "Malaysia Maritime",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Lianhe Zaobao 联合早报",
        "display_name": "Lianhe Zaobao 联合早报",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "display_name": "Manohar Parrikar Institute for Defence Studies and Analyses",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "UNODC - United Nations Office on Drugs and Crime",
        "display_name": "UNODC - United Nations Office on Drugs and Crime",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Science & Technology Journal - Mizoram University",
        "display_name": "Science & Technology Journal - Mizoram University",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Western Indian Ocean Marine Science Association (WIOMSA)",
        "display_name": "Western Indian Ocean Marine Science Association (WIOMSA)",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Taylor & Francis",
        "display_name": "Taylor & Francis",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Institute of Peace & Security Studies - BIPSS",
        "display_name": "Bangladesh Institute of Peace & Security Studies - BIPSS",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chatham House",
        "display_name": "Chatham House",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Tillotoma Foundation",
        "display_name": "Tillotoma Foundation",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Nationalist Party-BNP",
        "display_name": "Bangladesh Nationalist Party-BNP",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Kanglei Miyamgi Thawai",
        "display_name": "Kanglei Miyamgi Thawai",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Arambai Tenggol",
        "display_name": "Arambai Tenggol",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Cocomi MEDIA",
        "display_name": "Cocomi MEDIA",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Meitei Leepun",
        "display_name": "Meitei Leepun",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "防衛省（Japan Ministry of Defense）",
        "display_name": "防衛省（Japan Ministry of Defense）",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Rice University's Baker Institute for Public Policy",
        "display_name": "Rice University's Baker Institute for Public Policy",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ChinaFile",
        "display_name": "ChinaFile",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Air University",
        "display_name": "Air University",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Usanas Foundation",
        "display_name": "Usanas Foundation",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Institute of International and Strategic Studies",
        "display_name": "Bangladesh Institute of International and Strategic Studies",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "IMPRI Impact and Policy Research Institute",
        "display_name": "IMPRI Impact and Policy Research Institute",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Institute of Strategic Studies Islamabad",
        "display_name": "Institute of Strategic Studies Islamabad",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Centre for Policy Dialogue (CPD)",
        "display_name": "Centre for Policy Dialogue (CPD)",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRAC Institute of Governance and Development - BIGD",
        "display_name": "BRAC Institute of Governance and Development - BIGD",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Enterprise Institute (BEI)",
        "display_name": "Bangladesh Enterprise Institute (BEI)",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Jamaat-e-Islami",
        "display_name": "Bangladesh Jamaat-e-Islami",
        "category": "Facebook Profiles",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal floods",
        "display_name": "Nepal floods",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood disaster",
        "display_name": "Nepal flood disaster",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood alert",
        "display_name": "Nepal flood alert",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood relief",
        "display_name": "Nepal flood relief",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood casualties",
        "display_name": "Nepal flood casualties",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood rescue",
        "display_name": "Nepal flood rescue",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal flood update",
        "display_name": "Nepal flood update",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal landslide",
        "display_name": "Nepal landslide",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal mudslide",
        "display_name": "Nepal mudslide",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal monsoon floods",
        "display_name": "Nepal monsoon floods",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal heavy rainfall",
        "display_name": "Nepal heavy rainfall",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal disaster response",
        "display_name": "Nepal disaster response",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal emergency",
        "display_name": "Nepal emergency",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal natural disaster",
        "display_name": "Nepal natural disaster",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Nepal weather alert",
        "display_name": "Nepal weather alert",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "#scramblemagazine",
        "display_name": "#scramblemagazine",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "76 GA",
        "display_name": "76 GA",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "76th Group Army",
        "display_name": "76th Group Army",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "77 GA",
        "display_name": "77 GA",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "77th Group Army",
        "display_name": "77th Group Army",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "aggression protest india",
        "display_name": "aggression protest india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Arunachal border",
        "display_name": "Arunachal border",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "assam flood",
        "display_name": "assam flood",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "assam violence",
        "display_name": "assam violence",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Australian Strategic Policy Institute (ASPI)",
        "display_name": "Australian Strategic Policy Institute (ASPI)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bagamoyo Port",
        "display_name": "Bagamoyo Port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bakers Institute Energy Map",
        "display_name": "Bakers Institute Energy Map",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangabandhu Sheikh Mujibur Rahman Maritime University (BSMRMU)",
        "display_name": "Bangabandhu Sheikh Mujibur Rahman Maritime University (BSMRMU)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Bangladesh Maritime Journal",
        "display_name": "Bangladesh Maritime Journal",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "banned app india",
        "display_name": "banned app india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Belt and Road maritime",
        "display_name": "Belt and Road maritime",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "border tension india",
        "display_name": "border tension india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Brahmaputra dam",
        "display_name": "Brahmaputra dam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRI maritime project",
        "display_name": "BRI maritime project",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "CCP Bio Threats Initiative",
        "display_name": "CCP Bio Threats Initiative",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Central Military Commission",
        "display_name": "Central Military Commission",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Centre for Joint Operation Warfare Studies (CENJOWS)",
        "display_name": "Centre for Joint Operation Warfare Studies (CENJOWS)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Centre for Land Warfare Studies (CLAWS)",
        "display_name": "Centre for Land Warfare Studies (CLAWS)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China Aerospace Studies Institute (CASI)",
        "display_name": "China Aerospace Studies Institute (CASI)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China border infrastructure",
        "display_name": "China border infrastructure",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "china border ladakh arunachal",
        "display_name": "china border ladakh arunachal",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China border village",
        "display_name": "China border village",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China coal production",
        "display_name": "China coal production",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China energy security",
        "display_name": "China energy security",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China energy transition",
        "display_name": "China energy transition",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China fossil fuel imports",
        "display_name": "China fossil fuel imports",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China infrastructure project",
        "display_name": "China infrastructure project",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China LNG imports",
        "display_name": "China LNG imports",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China maritime infrastructure",
        "display_name": "China maritime infrastructure",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China Maritime Reports (Shipbuilding of China)",
        "display_name": "China Maritime Reports (Shipbuilding of China)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China military airfield",
        "display_name": "China military airfield",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China military road",
        "display_name": "China military road",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China oil imports",
        "display_name": "China oil imports",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China overseas port",
        "display_name": "China overseas port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China port project",
        "display_name": "China port project",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China renewable energy",
        "display_name": "China renewable energy",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ChinaFile ChinaFileConversation Asia society",
        "display_name": "ChinaFile ChinaFileConversation Asia society",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese aircraft carrier",
        "display_name": "Chinese aircraft carrier",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese fighter jet exercise",
        "display_name": "Chinese fighter jet exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment Africa",
        "display_name": "Chinese investment Africa",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment ASEAN",
        "display_name": "Chinese investment ASEAN",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment Bangladesh",
        "display_name": "Chinese investment Bangladesh",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment Europe",
        "display_name": "Chinese investment Europe",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment Nepal",
        "display_name": "Chinese investment Nepal",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment Pakistan",
        "display_name": "Chinese investment Pakistan",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese investment South America",
        "display_name": "Chinese investment South America",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese military aviation",
        "display_name": "Chinese military aviation",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese military corruption",
        "display_name": "Chinese military corruption",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese missile launch",
        "display_name": "Chinese missile launch",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese missile test",
        "display_name": "Chinese missile test",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese naval base",
        "display_name": "Chinese naval base",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese naval exercise",
        "display_name": "Chinese naval exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese presence Indian Ocean",
        "display_name": "Chinese presence Indian Ocean",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese warship",
        "display_name": "Chinese warship",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chittagong Port",
        "display_name": "Chittagong Port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "CMC China",
        "display_name": "CMC China",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "cockroach janata party",
        "display_name": "cockroach janata party",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "cross border firing",
        "display_name": "cross border firing",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "cyber fraud india",
        "display_name": "cyber fraud india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "cyclone india",
        "display_name": "cyclone india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "defence news india",
        "display_name": "defence news india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Demchok",
        "display_name": "Demchok",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Depsang",
        "display_name": "Depsang",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "DF-17",
        "display_name": "DF-17",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "DF-21",
        "display_name": "DF-21",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "DF-26",
        "display_name": "DF-26",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Djibouti naval base",
        "display_name": "Djibouti naval base",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Doklam",
        "display_name": "Doklam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "drone attack india",
        "display_name": "drone attack india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Eastern Ladakh",
        "display_name": "Eastern Ladakh",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "elections india",
        "display_name": "elections india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "flood northeast india",
        "display_name": "flood northeast india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Gwadar Port",
        "display_name": "Gwadar Port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "H-6 bomber",
        "display_name": "H-6 bomber",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Hambantota Port",
        "display_name": "Hambantota Port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "hate speech india",
        "display_name": "hate speech india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "hypersonic missile China",
        "display_name": "hypersonic missile China",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "India China border infrastructure",
        "display_name": "India China border infrastructure",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "India China military",
        "display_name": "India China military",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "india pakistan tension",
        "display_name": "india pakistan tension",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Indian Council for World Affairs",
        "display_name": "Indian Council for World Affairs",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Indian Ocean deployment",
        "display_name": "Indian Ocean deployment",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Indian Ocean Region",
        "display_name": "Indian Ocean Region",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Intergovernmental Authority on Development (IGAD)",
        "display_name": "Intergovernmental Authority on Development (IGAD)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "IOR security",
        "display_name": "IOR security",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "IOSI Global",
        "display_name": "IOSI Global",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "iran israel war",
        "display_name": "iran israel war",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "J-16 fighter",
        "display_name": "J-16 fighter",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "J-20 fighter",
        "display_name": "J-20 fighter",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Japanese Ministry of Defense",
        "display_name": "Japanese Ministry of Defense",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Journal of Engineering and Technology",
        "display_name": "Journal of Engineering and Technology",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "kashmir encounter",
        "display_name": "kashmir encounter",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Kyaukpyu Port",
        "display_name": "Kyaukpyu Port",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "LAC standoff",
        "display_name": "LAC standoff",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "landslide northeast",
        "display_name": "landslide northeast",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "loan app scam",
        "display_name": "loan app scam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "malicious URL scam",
        "display_name": "malicious URL scam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "manipur flood",
        "display_name": "manipur flood",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "manipur violence",
        "display_name": "manipur violence",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Maritime journal indonesia",
        "display_name": "Maritime journal indonesia",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Maritime Silk Road",
        "display_name": "Maritime Silk Road",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "military movement india",
        "display_name": "military movement india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "National Defence University Naval Academy (Türkiye)",
        "display_name": "National Defence University Naval Academy (Türkiye)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "NEET",
        "display_name": "NEET",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "northeast violence",
        "display_name": "northeast violence",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Pakistan Navy War College Publications",
        "display_name": "Pakistan Navy War College Publications",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "payment fraud india",
        "display_name": "payment fraud india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "People's Liberation Army",
        "display_name": "People's Liberation Army",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "People's Liberation Army Air Force",
        "display_name": "People's Liberation Army Air Force",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "People's Liberation Army Navy",
        "display_name": "People's Liberation Army Navy",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "People's Liberation Army Rocket Force",
        "display_name": "People's Liberation Army Rocket Force",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "phishing scam india",
        "display_name": "phishing scam india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA combat readiness",
        "display_name": "PLA combat readiness",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA drill",
        "display_name": "PLA drill",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA exercise",
        "display_name": "PLA exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA field force",
        "display_name": "PLA field force",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA leadership",
        "display_name": "PLA leadership",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA military exercise",
        "display_name": "PLA military exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA modernization",
        "display_name": "PLA modernization",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA purge",
        "display_name": "PLA purge",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA training",
        "display_name": "PLA training",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLA Western Theater Command",
        "display_name": "PLA Western Theater Command",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLAAF",
        "display_name": "PLAAF",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLAAF exercise",
        "display_name": "PLAAF exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLAAF training",
        "display_name": "PLAAF training",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLAN Navy",
        "display_name": "PLAN Navy",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PLARF",
        "display_name": "PLARF",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "political clash india",
        "display_name": "political clash india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "public anger india",
        "display_name": "public anger india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Rocket Force exercise",
        "display_name": "Rocket Force exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "security alert india",
        "display_name": "security alert india",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Sichuan Tibet Railway",
        "display_name": "Sichuan Tibet Railway",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "South China Sea exercise",
        "display_name": "South China Sea exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Stimson Center Geopolitical Monitor",
        "display_name": "Stimson Center Geopolitical Monitor",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Stockholm International Peace Research Institute (SIPRI)",
        "display_name": "Stockholm International Peace Research Institute (SIPRI)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "String of Pearls",
        "display_name": "String of Pearls",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "The Arab Gulf Journal",
        "display_name": "The Arab Gulf Journal",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Tibet Military District",
        "display_name": "Tibet Military District",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Tibet military exercise",
        "display_name": "Tibet military exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Tibet railway",
        "display_name": "Tibet railway",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "TMD PLA",
        "display_name": "TMD PLA",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Type 052D destroyer",
        "display_name": "Type 052D destroyer",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Type 055 destroyer",
        "display_name": "Type 055 destroyer",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "UPI scam",
        "display_name": "UPI scam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Vivekananda International Foundation",
        "display_name": "Vivekananda International Foundation",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Wassenaar Arrangements",
        "display_name": "Wassenaar Arrangements",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Western Theater Command",
        "display_name": "Western Theater Command",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "WTC PLA",
        "display_name": "WTC PLA",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Xinjiang Military District",
        "display_name": "Xinjiang Military District",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Xinjiang military exercise",
        "display_name": "Xinjiang military exercise",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "XMD PLA",
        "display_name": "XMD PLA",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Yarlung Tsangpo dam",
        "display_name": "Yarlung Tsangpo dam",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Zhang Youxia",
        "display_name": "Zhang Youxia",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "आतंकी हमला भारत",
        "display_name": "आतंकी हमला भारत",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "कश्मीर मुठभेड़",
        "display_name": "कश्मीर मुठभेड़",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "युद्ध भारत",
        "display_name": "युद्ध भारत",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "सीमा तनाव",
        "display_name": "सीमा तनाव",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "सेना खबर",
        "display_name": "सेना खबर",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "অসম সংঘাত",
        "display_name": "অসম সংঘাত",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "উত্তৰ-পূব হিংসা",
        "display_name": "উত্তৰ-পূব হিংসা",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "জঙ্গি হামলা",
        "display_name": "জঙ্গি হামলা",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "প্রতিরক্ষা খবর",
        "display_name": "প্রতিরক্ষা খবর",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "বাংলা খবর",
        "display_name": "বাংলা খবর",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "বোমা বিস্ফোরণ",
        "display_name": "বোমা বিস্ফোরণ",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "মণিপুর হিংসা",
        "display_name": "মণিপুর হিংসা",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "মণিপုৰ সংঘাত",
        "display_name": "মণিপုৰ সংঘাত",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "সীমান্ত উত্তেজনা",
        "display_name": "সীমান্ত উত্তেজনা",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "PAK occupied Kashmir",
        "display_name": "PAK occupied Kashmir",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Long March 7A",
        "display_name": "Long March 7A",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Long March 7",
        "display_name": "Long March 7",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "CZ-7A",
        "display_name": "CZ-7A",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Wenchang launch",
        "display_name": "Wenchang launch",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Wenchang Space Launch Site",
        "display_name": "Wenchang Space Launch Site",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "CASC (China Aerospace Science and Technology Corporation)",
        "display_name": "CASC (China Aerospace Science and Technology Corporation)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ChinaSat 4B",
        "display_name": "ChinaSat 4B",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Zhongxing 4B",
        "display_name": "Zhongxing 4B",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "YF-100 engine",
        "display_name": "YF-100 engine",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "rocket explosion China",
        "display_name": "rocket explosion China",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "launch failure China",
        "display_name": "launch failure China",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chinese rocket anomaly",
        "display_name": "Chinese rocket anomaly",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "flight termination system",
        "display_name": "flight termination system",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China orbital launch failure",
        "display_name": "China orbital launch failure",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Chang'e 7",
        "display_name": "Chang'e 7",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China Academy of Launch Vehicle Technology (CALT)",
        "display_name": "China Academy of Launch Vehicle Technology (CALT)",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Jiuquan launch",
        "display_name": "Jiuquan launch",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Xichang launch",
        "display_name": "Xichang launch",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Taiyuan launch",
        "display_name": "Taiyuan launch",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China National Radio launch report",
        "display_name": "China National Radio launch report",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Long March explosion",
        "display_name": "Long March explosion",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Pak-China MoU on AI",
        "display_name": "Pak-China MoU on AI",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "NCAI Pakistan",
        "display_name": "NCAI Pakistan",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS leader health",
        "display_name": "BRICS leader health",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS leader illness",
        "display_name": "BRICS leader illness",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS summit leader sick",
        "display_name": "BRICS summit leader sick",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "world leader falls ill BRICS",
        "display_name": "world leader falls ill BRICS",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS leader hospitalized",
        "display_name": "BRICS leader hospitalized",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS leader health scare",
        "display_name": "BRICS leader health scare",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "BRICS leader missing summit",
        "display_name": "BRICS leader missing summit",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Xi Jinping health",
        "display_name": "Xi Jinping health",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Putin health rumors",
        "display_name": "Putin health rumors",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Modi health",
        "display_name": "Modi health",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Lula health",
        "display_name": "Lula health",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus drone",
        "display_name": "Powerus drone",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus drone maker",
        "display_name": "Powerus drone maker",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Pentagon contract",
        "display_name": "Powerus Pentagon contract",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Trump sons",
        "display_name": "Powerus Trump sons",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Andrew Fox",
        "display_name": "Powerus Andrew Fox",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Brett Velicovich",
        "display_name": "Powerus Brett Velicovich",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Aureus Greenway merger",
        "display_name": "Powerus Aureus Greenway merger",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus interceptor drone",
        "display_name": "Powerus interceptor drone",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Powerus Ukraine drone technology",
        "display_name": "Powerus Ukraine drone technology",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Global Defense Insight",
        "display_name": "Global Defense Insight",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Prince Mohammad bin Salman",
        "display_name": "Prince Mohammad bin Salman",
        "category": "Facebook Keywords",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "all_source_news",
        "display_name": "all_source_news",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@aspi_org",
        "display_name": "aspi_org",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "aspi_org",
        "display_name": "aspi_org",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "bakerinstitute",
        "display_name": "bakerinstitute",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "bipss.org.bd",
        "display_name": "bipss.org.bd",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "bji.official",
        "display_name": "bji.official",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "bnpbd",
        "display_name": "bnpbd",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "cenjowsindia",
        "display_name": "cenjowsindia",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "chathamhouse_org",
        "display_name": "chathamhouse_org",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@chinafile",
        "display_name": "chinafile",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "chinafile",
        "display_name": "chinafile",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@chopsyturvey",
        "display_name": "chopsyturvey",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "chopsyturvey",
        "display_name": "chopsyturvey",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "cpd_bangladesh",
        "display_name": "cpd_bangladesh",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "defense_talks",
        "display_name": "defense_talks",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@HASHTAG_JALALABAD",
        "display_name": "HASHTAG_JALALABAD",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "HASHTAG_JALALABAD",
        "display_name": "HASHTAG_JALALABAD",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "impriindia",
        "display_name": "impriindia",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "indopacificreport",
        "display_name": "indopacificreport",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@joseph.wen___",
        "display_name": "joseph.wen___",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "joseph.wen___",
        "display_name": "joseph.wen___",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@JosephWen___",
        "display_name": "JosephWen___",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "JosephWen___",
        "display_name": "JosephWen___",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@joseph wen_",
        "display_name": "joseph wen_",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "mfa_china",
        "display_name": "mfa_china",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "mima_malaysia",
        "display_name": "mima_malaysia",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@mndf_official",
        "display_name": "mndf_official",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "mndf_official",
        "display_name": "mndf_official",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "modefencemv",
        "display_name": "modefencemv",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "modjapan_en",
        "display_name": "modjapan_en",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@mpidsa_india",
        "display_name": "mpidsa_india",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "mpidsa_india",
        "display_name": "mpidsa_india",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "navalinstitute",
        "display_name": "navalinstitute",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@ncp_bd",
        "display_name": "ncp_bd",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "ncp_bd",
        "display_name": "ncp_bd",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "pakinsrilanka",
        "display_name": "pakinsrilanka",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "pakistan_navy",
        "display_name": "pakistan_navy",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@pakistanairforce.paf",
        "display_name": "pakistanairforce.paf",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "pakistanairforce.paf",
        "display_name": "pakistanairforce.paf",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "randcorporation",
        "display_name": "randcorporation",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "robbanassafina",
        "display_name": "robbanassafina",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "scramble_nl",
        "display_name": "scramble_nl",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@sipriorg",
        "display_name": "sipriorg",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "sipriorg",
        "display_name": "sipriorg",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "srilankanavy",
        "display_name": "srilankanavy",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@tillotomafoundation",
        "display_name": "tillotomafoundation",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "tillotomafoundation",
        "display_name": "tillotomafoundation",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "unodc_hq",
        "display_name": "unodc_hq",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "usanasfoundation",
        "display_name": "usanasfoundation",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "vermilion_china",
        "display_name": "vermilion_china",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "wiomsa",
        "display_name": "wiomsa",
        "category": "Instagram Profiles",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#76GA",
        "display_name": "#76GA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#76thGroupArmy",
        "display_name": "#76thGroupArmy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#77GA",
        "display_name": "#77GA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#77thGroupArmy",
        "display_name": "#77thGroupArmy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AggressionProtestIndia",
        "display_name": "#AggressionProtestIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AircraftCarrier",
        "display_name": "#AircraftCarrier",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AntiDrone",
        "display_name": "#AntiDrone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ArunachalBorder",
        "display_name": "#ArunachalBorder",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AssamFlood",
        "display_name": "#AssamFlood",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AssamViolence",
        "display_name": "#AssamViolence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AustralianStrategicPolicyInstitute",
        "display_name": "#AustralianStrategicPolicyInstitute",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BagamoyoPort",
        "display_name": "#BagamoyoPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BakersInstituteEnergyMap",
        "display_name": "#BakersInstituteEnergyMap",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BangabandhuSheikhMujiburRahmanMaritimeUniversity",
        "display_name": "#BangabandhuSheikhMujiburRahmanMaritimeUniversity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BangladeshMaritimeJournal",
        "display_name": "#BangladeshMaritimeJournal",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BannedAppIndia",
        "display_name": "#BannedAppIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BeltAndRoadMaritime",
        "display_name": "#BeltAndRoadMaritime",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BorderConflict",
        "display_name": "#BorderConflict",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BorderIntrusion",
        "display_name": "#BorderIntrusion",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#bordertensionindia",
        "display_name": "#bordertensionindia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BorderTensions",
        "display_name": "#BorderTensions",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BrahmaputraDam",
        "display_name": "#BrahmaputraDam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BRIMaritimeProject",
        "display_name": "#BRIMaritimeProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#cbse",
        "display_name": "#cbse",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CCPBioThreatsInitiative",
        "display_name": "#CCPBioThreatsInitiative",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CeasefireViolation",
        "display_name": "#CeasefireViolation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CentralMilitaryCommission",
        "display_name": "#CentralMilitaryCommission",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CentreForJointOperationWarfareStudies",
        "display_name": "#CentreForJointOperationWarfareStudies",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CentreForLandWarfareStudies",
        "display_name": "#CentreForLandWarfareStudies",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaAerospaceStudiesInstitute",
        "display_name": "#ChinaAerospaceStudiesInstitute",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaBorderInfrastructure",
        "display_name": "#ChinaBorderInfrastructure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaBorderLadakhArunachal",
        "display_name": "#ChinaBorderLadakhArunachal",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaBorderVillage",
        "display_name": "#ChinaBorderVillage",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaCoalProduction",
        "display_name": "#ChinaCoalProduction",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaEnergySecurity",
        "display_name": "#ChinaEnergySecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaEnergyTransition",
        "display_name": "#ChinaEnergyTransition",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaFileChinaFileConversationAsiaSociety",
        "display_name": "#ChinaFileChinaFileConversationAsiaSociety",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaFossilFuelImports",
        "display_name": "#ChinaFossilFuelImports",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaInfrastructureProject",
        "display_name": "#ChinaInfrastructureProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaLNGImports",
        "display_name": "#ChinaLNGImports",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMaritimeInfrastructure",
        "display_name": "#ChinaMaritimeInfrastructure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMaritimeReports",
        "display_name": "#ChinaMaritimeReports",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMilitaryAirfield",
        "display_name": "#ChinaMilitaryAirfield",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMilitaryRoad",
        "display_name": "#ChinaMilitaryRoad",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Chinamyanmar",
        "display_name": "#Chinamyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaOilImports",
        "display_name": "#ChinaOilImports",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaOverseasPort",
        "display_name": "#ChinaOverseasPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaPortProject",
        "display_name": "#ChinaPortProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaRenewableEnergy",
        "display_name": "#ChinaRenewableEnergy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseAircraftCarrier",
        "display_name": "#ChineseAircraftCarrier",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseFighterJetExercise",
        "display_name": "#ChineseFighterJetExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentAfrica",
        "display_name": "#ChineseInvestmentAfrica",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentASEAN",
        "display_name": "#ChineseInvestmentASEAN",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentBangladesh",
        "display_name": "#ChineseInvestmentBangladesh",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentEurope",
        "display_name": "#ChineseInvestmentEurope",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentNepal",
        "display_name": "#ChineseInvestmentNepal",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentPakistan",
        "display_name": "#ChineseInvestmentPakistan",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentSouthAmerica",
        "display_name": "#ChineseInvestmentSouthAmerica",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#chinesejet",
        "display_name": "#chinesejet",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseMilitaryAviation",
        "display_name": "#ChineseMilitaryAviation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseMilitaryCorruption",
        "display_name": "#ChineseMilitaryCorruption",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseMissileLaunch",
        "display_name": "#ChineseMissileLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseMissileTest",
        "display_name": "#ChineseMissileTest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseNavalBase",
        "display_name": "#ChineseNavalBase",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseNavalExercise",
        "display_name": "#ChineseNavalExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinesePresenceIndianOcean",
        "display_name": "#ChinesePresenceIndianOcean",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseWarship",
        "display_name": "#ChineseWarship",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChittagongPort",
        "display_name": "#ChittagongPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#cjp",
        "display_name": "#cjp",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CoastalSecurity",
        "display_name": "#CoastalSecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CockroachJanataParty",
        "display_name": "#CockroachJanataParty",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#corruption",
        "display_name": "#corruption",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CounterTerrorOperation",
        "display_name": "#CounterTerrorOperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CriticalInfrastructure",
        "display_name": "#CriticalInfrastructure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CrossBorder",
        "display_name": "#CrossBorder",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CrossBorderFiring",
        "display_name": "#CrossBorderFiring",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#cyberfraud",
        "display_name": "#cyberfraud",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CyberFraudIndia",
        "display_name": "#CyberFraudIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CycloneIndia",
        "display_name": "#CycloneIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#defence",
        "display_name": "#defence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DefenceNewsIndia",
        "display_name": "#DefenceNewsIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DefenseInnovation",
        "display_name": "#DefenseInnovation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DefenseUpdate",
        "display_name": "#DefenseUpdate",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Demchok",
        "display_name": "#Demchok",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Depsang",
        "display_name": "#Depsang",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DF17",
        "display_name": "#DF17",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DF21",
        "display_name": "#DF21",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DF26",
        "display_name": "#DF26",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#disaster",
        "display_name": "#disaster",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DjiboutiNavalBase",
        "display_name": "#DjiboutiNavalBase",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Doklam",
        "display_name": "#Doklam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#droneattackindia",
        "display_name": "#droneattackindia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DroneStrike",
        "display_name": "#DroneStrike",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#EasternLadakh",
        "display_name": "#EasternLadakh",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#election",
        "display_name": "#election",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ElectionsIndia",
        "display_name": "#ElectionsIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FloodNortheastIndia",
        "display_name": "#FloodNortheastIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#fraud",
        "display_name": "#fraud",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FreedomOfNavigation",
        "display_name": "#FreedomOfNavigation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GwadarPort",
        "display_name": "#GwadarPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#H6Bomber",
        "display_name": "#H6Bomber",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#HambantotaPort",
        "display_name": "#HambantotaPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#HateSpeechIndia",
        "display_name": "#HateSpeechIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#HypersonicMissileChina",
        "display_name": "#HypersonicMissileChina",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Indiachinaborder",
        "display_name": "#Indiachinaborder",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndiaChinaBorderInfrastructure",
        "display_name": "#IndiaChinaBorderInfrastructure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndiaChinaMilitary",
        "display_name": "#IndiaChinaMilitary",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndianCouncilForWorldAffairs",
        "display_name": "#IndianCouncilForWorldAffairs",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#indianirforce",
        "display_name": "#indianirforce",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#indiannavy",
        "display_name": "#indiannavy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndianOceanDeployment",
        "display_name": "#IndianOceanDeployment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndianOceanRegion",
        "display_name": "#IndianOceanRegion",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndiaPakistanTension",
        "display_name": "#IndiaPakistanTension",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#indiapaktension",
        "display_name": "#indiapaktension",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndoPacific",
        "display_name": "#IndoPacific",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IntelligenceAlert",
        "display_name": "#IntelligenceAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IntergovernmentalAuthorityOnDevelopment",
        "display_name": "#IntergovernmentalAuthorityOnDevelopment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IORSecurity",
        "display_name": "#IORSecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IOSIGlobal",
        "display_name": "#IOSIGlobal",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IranIsraelWar",
        "display_name": "#IranIsraelWar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#J16Fighter",
        "display_name": "#J16Fighter",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#J20Fighter",
        "display_name": "#J20Fighter",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JapaneseMinistryOfDefense",
        "display_name": "#JapaneseMinistryOfDefense",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JournalOfEngineeringAndTechnology",
        "display_name": "#JournalOfEngineeringAndTechnology",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KashmirEncounter",
        "display_name": "#KashmirEncounter",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KyaukpyuPort",
        "display_name": "#KyaukpyuPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LACStandoff",
        "display_name": "#LACStandoff",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LandslideNortheast",
        "display_name": "#LandslideNortheast",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LoanAppScam",
        "display_name": "#LoanAppScam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MaliciousURLScam",
        "display_name": "#MaliciousURLScam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ManipurFlood",
        "display_name": "#ManipurFlood",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ManipurViolence",
        "display_name": "#ManipurViolence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ManoharParrikarInstituteForDefenceStudiesAndAnalyses",
        "display_name": "#ManoharParrikarInstituteForDefenceStudiesAndAnalyses",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MaritimeJournalIndonesia",
        "display_name": "#MaritimeJournalIndonesia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MaritimeSilkRoad",
        "display_name": "#MaritimeSilkRoad",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MaritimeSurveillance",
        "display_name": "#MaritimeSurveillance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MaritimeTrade",
        "display_name": "#MaritimeTrade",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MCChina",
        "display_name": "#MCChina",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryAlert",
        "display_name": "#MilitaryAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryDeployment",
        "display_name": "#MilitaryDeployment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryMovementIndia",
        "display_name": "#MilitaryMovementIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryOperation",
        "display_name": "#MilitaryOperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NationalAlert",
        "display_name": "#NationalAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NationalCrisis",
        "display_name": "#NationalCrisis",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NationalDefenceUniversityNavalAcademy",
        "display_name": "#NationalDefenceUniversityNavalAcademy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NavalModernization",
        "display_name": "#NavalModernization",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#neet",
        "display_name": "#neet",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NortheastViolence",
        "display_name": "#NortheastViolence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#nsg",
        "display_name": "#nsg",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NuclearSecurity",
        "display_name": "#NuclearSecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanNavyWarCollegePublications",
        "display_name": "#PakistanNavyWarCollegePublications",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakOccupiedKashmir",
        "display_name": "#PakOccupiedKashmir",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PaymentFraudIndia",
        "display_name": "#PaymentFraudIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PeoplesLiberationArmy",
        "display_name": "#PeoplesLiberationArmy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PeoplesLiberationArmyAirForce",
        "display_name": "#PeoplesLiberationArmyAirForce",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PeoplesLiberationArmyNavy",
        "display_name": "#PeoplesLiberationArmyNavy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PeoplesLiberationArmyRocketForce",
        "display_name": "#PeoplesLiberationArmyRocketForce",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PhishingScamIndia",
        "display_name": "#PhishingScamIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#plaaf",
        "display_name": "#plaaf",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAAFExercise",
        "display_name": "#PLAAFExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAAFTraining",
        "display_name": "#PLAAFTraining",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLACombatReadiness",
        "display_name": "#PLACombatReadiness",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLADrill",
        "display_name": "#PLADrill",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAExercise",
        "display_name": "#PLAExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAFieldForce",
        "display_name": "#PLAFieldForce",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLALeadership",
        "display_name": "#PLALeadership",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAMilitaryExercise",
        "display_name": "#PLAMilitaryExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAModernization",
        "display_name": "#PLAModernization",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLANNavy",
        "display_name": "#PLANNavy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAPurge",
        "display_name": "#PLAPurge",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLARF",
        "display_name": "#PLARF",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLATraining",
        "display_name": "#PLATraining",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLAWesternTheaterCommand",
        "display_name": "#PLAWesternTheaterCommand",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Pojk",
        "display_name": "#Pojk",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKProtest",
        "display_name": "#PoJKProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Pok",
        "display_name": "#Pok",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoliticalClashIndia",
        "display_name": "#PoliticalClashIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#politics",
        "display_name": "#politics",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PortSecurity",
        "display_name": "#PortSecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#protest",
        "display_name": "#protest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PublicAngerIndia",
        "display_name": "#PublicAngerIndia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#rawalkot",
        "display_name": "#rawalkot",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#RocketForceExercise",
        "display_name": "#RocketForceExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ScrambleMagazine",
        "display_name": "#ScrambleMagazine",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SecurityAlert",
        "display_name": "#SecurityAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#securityalertindia",
        "display_name": "#securityalertindia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SichuanTibetRailway",
        "display_name": "#SichuanTibetRailway",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SouthChinaSea",
        "display_name": "#SouthChinaSea",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SouthChinaSeaExercise",
        "display_name": "#SouthChinaSeaExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StimsonCenterGeopoliticalMonitor",
        "display_name": "#StimsonCenterGeopoliticalMonitor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StockholmInternationalPeaceResearchInstitute",
        "display_name": "#StockholmInternationalPeaceResearchInstitute",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StrategicSecurity",
        "display_name": "#StrategicSecurity",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StringOfPearls",
        "display_name": "#StringOfPearls",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Submarine",
        "display_name": "#Submarine",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TheArabGulfJournal",
        "display_name": "#TheArabGulfJournal",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TibetMilitaryDistrict",
        "display_name": "#TibetMilitaryDistrict",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TibetMilitaryExercise",
        "display_name": "#TibetMilitaryExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TibetRailway",
        "display_name": "#TibetRailway",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TMDPLA",
        "display_name": "#TMDPLA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Type052DDestroyer",
        "display_name": "#Type052DDestroyer",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Type055Destroyer",
        "display_name": "#Type055Destroyer",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ucav",
        "display_name": "#ucav",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#UnmannedSystems",
        "display_name": "#UnmannedSystems",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#UPIScam",
        "display_name": "#UPIScam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#USANASFoundation",
        "display_name": "#USANASFoundation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#violence",
        "display_name": "#violence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#VivekanandaInternationalFoundation",
        "display_name": "#VivekanandaInternationalFoundation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WarPreparedness",
        "display_name": "#WarPreparedness",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WassenaarArrangements",
        "display_name": "#WassenaarArrangements",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WesternTheaterCommand",
        "display_name": "#WesternTheaterCommand",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WTCPLA",
        "display_name": "#WTCPLA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XinjiangMilitaryDistrict",
        "display_name": "#XinjiangMilitaryDistrict",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XinjiangMilitaryExercise",
        "display_name": "#XinjiangMilitaryExercise",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XMDPLA",
        "display_name": "#XMDPLA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#YarlungTsangpoDam",
        "display_name": "#YarlungTsangpoDam",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ZhangYouxia",
        "display_name": "#ZhangYouxia",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#आतंकीहमलाभारत",
        "display_name": "#आतंकीहमलाभारत",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#कश्मीरमुठभेड़",
        "display_name": "#कश्मीरमुठभेड़",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#सीमातनाव",
        "display_name": "#सीमातनाव",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#सेनाखबर",
        "display_name": "#सेनाखबर",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#অসমসংঘাত",
        "display_name": "#অসমসংঘাত",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#উত্তৰপূবহিংসা",
        "display_name": "#উত্তৰপূবহিংসা",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#জঙ্গিহামলা",
        "display_name": "#জঙ্গিহামলা",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#প্রতিরক্ষাখবর",
        "display_name": "#প্রতিরক্ষাখবর",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#বাংলাখবর",
        "display_name": "#বাংলাখবর",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#বোমাবিস্ফোরণ",
        "display_name": "#বোমাবিস্ফোরণ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#মণিপুরহিংসা",
        "display_name": "#মণিপুরহিংসা",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#মণিপুৰসংঘাত",
        "display_name": "#মণিপুৰসংঘাত",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#যুদ্ধভারত",
        "display_name": "#যুদ্ধভারত",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#সীমান্তউত্তেজনা",
        "display_name": "#সীমান্তউত্তেজনা",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#China",
        "display_name": "#China",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Chinese",
        "display_name": "#Chinese",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Beijing",
        "display_name": "#Beijing",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PRC",
        "display_name": "#PRC",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CCP",
        "display_name": "#CCP",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XiJinping",
        "display_name": "#XiJinping",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseEmbassy",
        "display_name": "#ChineseEmbassy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseAmbassador",
        "display_name": "#ChineseAmbassador",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Myanmar",
        "display_name": "#Myanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Burma",
        "display_name": "#Burma",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Naypyidaw",
        "display_name": "#Naypyidaw",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Yangon",
        "display_name": "#Yangon",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Mandalay",
        "display_name": "#Mandalay",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Tatmadaw",
        "display_name": "#Tatmadaw",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Junta",
        "display_name": "#Junta",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SAC",
        "display_name": "#SAC",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StateAdministrationCouncil",
        "display_name": "#StateAdministrationCouncil",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NUG",
        "display_name": "#NUG",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NationalUnityGovernment",
        "display_name": "#NationalUnityGovernment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BeltAndRoadInitiative",
        "display_name": "#BeltAndRoadInitiative",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BRI",
        "display_name": "#BRI",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarEconomicCorridor",
        "display_name": "#ChinaMyanmarEconomicCorridor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CMEC",
        "display_name": "#CMEC",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Kyaukphyu",
        "display_name": "#Kyaukphyu",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KyaukphyuPort",
        "display_name": "#KyaukphyuPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KyaukphyuSEZ",
        "display_name": "#KyaukphyuSEZ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SpecialEconomicZone",
        "display_name": "#SpecialEconomicZone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarRailway",
        "display_name": "#ChinaMyanmarRailway",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MuseMandalayRailway",
        "display_name": "#MuseMandalayRailway",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarCorridor",
        "display_name": "#ChinaMyanmarCorridor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#InfrastructureProject",
        "display_name": "#InfrastructureProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#IndustrialPark",
        "display_name": "#IndustrialPark",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LogisticsCorridor",
        "display_name": "#LogisticsCorridor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarOilPipeline",
        "display_name": "#ChinaMyanmarOilPipeline",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarGasPipeline",
        "display_name": "#ChinaMyanmarGasPipeline",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#OilPipeline",
        "display_name": "#OilPipeline",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GasPipeline",
        "display_name": "#GasPipeline",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LNG",
        "display_name": "#LNG",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Hydropower",
        "display_name": "#Hydropower",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PowerProject",
        "display_name": "#PowerProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ElectricityProject",
        "display_name": "#ElectricityProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestment",
        "display_name": "#ChineseInvestment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseCompany",
        "display_name": "#ChineseCompany",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseBusiness",
        "display_name": "#ChineseBusiness",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseContractor",
        "display_name": "#ChineseContractor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#InvestmentProject",
        "display_name": "#InvestmentProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#EconomicCooperation",
        "display_name": "#EconomicCooperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BilateralCooperation",
        "display_name": "#BilateralCooperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TradeAgreement",
        "display_name": "#TradeAgreement",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TradeCorridor",
        "display_name": "#TradeCorridor",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ForeignInvestment",
        "display_name": "#ForeignInvestment",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DevelopmentProject",
        "display_name": "#DevelopmentProject",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseWeapons",
        "display_name": "#ChineseWeapons",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseArms",
        "display_name": "#ChineseArms",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryCooperation",
        "display_name": "#MilitaryCooperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MilitaryAid",
        "display_name": "#MilitaryAid",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FighterJet",
        "display_name": "#FighterJet",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#UAV",
        "display_name": "#UAV",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Drone",
        "display_name": "#Drone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Surveillance",
        "display_name": "#Surveillance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Radar",
        "display_name": "#Radar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PLA",
        "display_name": "#PLA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SecurityCooperation",
        "display_name": "#SecurityCooperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Yunnan",
        "display_name": "#Yunnan",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Ruili",
        "display_name": "#Ruili",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Muse",
        "display_name": "#Muse",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Kokang",
        "display_name": "#Kokang",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WaState",
        "display_name": "#WaState",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ShanState",
        "display_name": "#ShanState",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Kachin",
        "display_name": "#Kachin",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Chinshwehaw",
        "display_name": "#Chinshwehaw",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BorderTrade",
        "display_name": "#BorderTrade",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BorderCrossing",
        "display_name": "#BorderCrossing",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CrossBorderTrade",
        "display_name": "#CrossBorderTrade",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Customs",
        "display_name": "#Customs",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#RareEarth",
        "display_name": "#RareEarth",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#RareEarthMining",
        "display_name": "#RareEarthMining",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JadeMining",
        "display_name": "#JadeMining",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CopperMine",
        "display_name": "#CopperMine",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Lithium",
        "display_name": "#Lithium",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Nickel",
        "display_name": "#Nickel",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MineralExtraction",
        "display_name": "#MineralExtraction",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Huawei",
        "display_name": "#Huawei",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ZTE",
        "display_name": "#ZTE",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Hikvision",
        "display_name": "#Hikvision",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Dahua",
        "display_name": "#Dahua",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FacialRecognition",
        "display_name": "#FacialRecognition",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AISurveillance",
        "display_name": "#AISurveillance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CCTV",
        "display_name": "#CCTV",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Telecom",
        "display_name": "#Telecom",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#5G",
        "display_name": "#5G",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DiplomaticVisit",
        "display_name": "#DiplomaticVisit",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AmbassadorMeeting",
        "display_name": "#AmbassadorMeeting",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ForeignMinister",
        "display_name": "#ForeignMinister",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#OfficialDelegation",
        "display_name": "#OfficialDelegation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StrategicPartnership",
        "display_name": "#StrategicPartnership",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PeaceTalks",
        "display_name": "#PeaceTalks",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInfluence",
        "display_name": "#ChineseInfluence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#InfluenceOperation",
        "display_name": "#InfluenceOperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Propaganda",
        "display_name": "#Propaganda",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Disinformation",
        "display_name": "#Disinformation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Misinformation",
        "display_name": "#Misinformation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SoftPower",
        "display_name": "#SoftPower",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MediaInfluence",
        "display_name": "#MediaInfluence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StrategicInfluence",
        "display_name": "#StrategicInfluence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Operation1027",
        "display_name": "#Operation1027",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ThreeBrotherhoodAlliance",
        "display_name": "#ThreeBrotherhoodAlliance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MNDAA",
        "display_name": "#MNDAA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TNLA",
        "display_name": "#TNLA",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ArakanArmy",
        "display_name": "#ArakanArmy",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Ceasefire",
        "display_name": "#Ceasefire",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ArmedClashes",
        "display_name": "#ArmedClashes",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Insurgency",
        "display_name": "#Insurgency",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInvestmentMyanmar",
        "display_name": "#ChineseInvestmentMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseActivitiesMyanmar",
        "display_name": "#ChineseActivitiesMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaInMyanmar",
        "display_name": "#ChinaInMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseProjectMyanmar",
        "display_name": "#ChineseProjectMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseCompanyMyanmar",
        "display_name": "#ChineseCompanyMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseMilitaryMyanmar",
        "display_name": "#ChineseMilitaryMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarPipeline",
        "display_name": "#ChinaMyanmarPipeline",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseInfluenceMyanmar",
        "display_name": "#ChineseInfluenceMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarBorder",
        "display_name": "#ChinaMyanmarBorder",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseEmbassyMyanmar",
        "display_name": "#ChineseEmbassyMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaBacksMyanmarJunta",
        "display_name": "#ChinaBacksMyanmarJunta",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseSupportMyanmar",
        "display_name": "#ChineseSupportMyanmar",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarTrade",
        "display_name": "#ChinaMyanmarTrade",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarRelations",
        "display_name": "#ChinaMyanmarRelations",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarCooperation",
        "display_name": "#ChinaMyanmarCooperation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarPort",
        "display_name": "#ChinaMyanmarPort",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarSEZ",
        "display_name": "#ChinaMyanmarSEZ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaMyanmarConflict",
        "display_name": "#ChinaMyanmarConflict",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanOccupiedJammuAndKashmir",
        "display_name": "#PakistanOccupiedJammuAndKashmir",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanOccupiedKashmir",
        "display_name": "#PakistanOccupiedKashmir",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AzadKashmir",
        "display_name": "#AzadKashmir",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AJK",
        "display_name": "#AJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GilgitBaltistan",
        "display_name": "#GilgitBaltistan",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GBProtest",
        "display_name": "#GBProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Muzaffarabad",
        "display_name": "#Muzaffarabad",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Mirpur",
        "display_name": "#Mirpur",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Skardu",
        "display_name": "#Skardu",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Gilgit",
        "display_name": "#Gilgit",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Rawalakot",
        "display_name": "#Rawalakot",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Kotli",
        "display_name": "#Kotli",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Bhimber",
        "display_name": "#Bhimber",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NeelumValley",
        "display_name": "#NeelumValley",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AwamiActionCommittee",
        "display_name": "#AwamiActionCommittee",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JammuKashmirJointAwamiActionCommittee",
        "display_name": "#JammuKashmirJointAwamiActionCommittee",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JAAC",
        "display_name": "#JAAC",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GBAwamiActionCommittee",
        "display_name": "#GBAwamiActionCommittee",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WheatSubsidyProtest",
        "display_name": "#WheatSubsidyProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FlourCrisisPoJK",
        "display_name": "#FlourCrisisPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ElectricityPriceProtest",
        "display_name": "#ElectricityPriceProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TaxProtestPoJK",
        "display_name": "#TaxProtestPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ShutterDownStrike",
        "display_name": "#ShutterDownStrike",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKShutdown",
        "display_name": "#PoJKShutdown",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKUnrest",
        "display_name": "#PoJKUnrest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKShutterDownStrike",
        "display_name": "#PoJKShutterDownStrike",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KashmiriDiasporaProtest",
        "display_name": "#KashmiriDiasporaProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#HumanRightsViolationsPoJK",
        "display_name": "#HumanRightsViolationsPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#EnforcedDisappearancesPoJK",
        "display_name": "#EnforcedDisappearancesPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKActivists",
        "display_name": "#PoJKActivists",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKCrackdown",
        "display_name": "#PoJKCrackdown",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKArrests",
        "display_name": "#PoJKArrests",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKInternetShutdown",
        "display_name": "#PoJKInternetShutdown",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKCurfew",
        "display_name": "#PoJKCurfew",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKStrike",
        "display_name": "#PoJKStrike",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanArmyPoJK",
        "display_name": "#PakistanArmyPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanForcesGilgitBaltistan",
        "display_name": "#PakistanForcesGilgitBaltistan",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ISIPoJK",
        "display_name": "#ISIPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SelfRulePoJK",
        "display_name": "#SelfRulePoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AntiPakistanProtestKashmir",
        "display_name": "#AntiPakistanProtestKashmir",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DiamerBhashaDamProtest",
        "display_name": "#DiamerBhashaDamProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CPECGilgitBaltistanProtest",
        "display_name": "#CPECGilgitBaltistanProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LandRightsGilgitBaltistan",
        "display_name": "#LandRightsGilgitBaltistan",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKPoliticalPrisoners",
        "display_name": "#PoJKPoliticalPrisoners",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKIndependenceMovement",
        "display_name": "#PoJKIndependenceMovement",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKFreedomMovement",
        "display_name": "#PoJKFreedomMovement",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KashmirLiberationFrontPoJK",
        "display_name": "#KashmirLiberationFrontPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKElections",
        "display_name": "#PoJKElections",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKAssembly",
        "display_name": "#PoJKAssembly",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKPoliceAction",
        "display_name": "#PoJKPoliceAction",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKTearGas",
        "display_name": "#PoJKTearGas",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKBatonCharge",
        "display_name": "#PoJKBatonCharge",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKSitIn",
        "display_name": "#PoJKSitIn",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKHungerStrike",
        "display_name": "#PoJKHungerStrike",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKLongMarch",
        "display_name": "#PoJKLongMarch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FreeGB",
        "display_name": "#FreeGB",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FreePoK",
        "display_name": "#FreePoK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KashmirProtest",
        "display_name": "#KashmirProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoKProtest",
        "display_name": "#PoKProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#StandWithGB",
        "display_name": "#StandWithGB",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#EndOccupation",
        "display_name": "#EndOccupation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KashmirLiberation",
        "display_name": "#KashmirLiberation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GBRights",
        "display_name": "#GBRights",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#MuzaffarabadProtest",
        "display_name": "#MuzaffarabadProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SkarduProtest",
        "display_name": "#SkarduProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GilgitProtest",
        "display_name": "#GilgitProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WeWantFreedom",
        "display_name": "#WeWantFreedom",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKProtestNews",
        "display_name": "#PoJKProtestNews",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoKProtestNews",
        "display_name": "#PoKProtestNews",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GilgitBaltistanProtest",
        "display_name": "#GilgitBaltistanProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AzadKashmirProtest",
        "display_name": "#AzadKashmirProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKElectricityProtest",
        "display_name": "#PoJKElectricityProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKWheatFlourProtest",
        "display_name": "#PoJKWheatFlourProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKCrackdownOnProtesters",
        "display_name": "#PoJKCrackdownOnProtesters",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKInternetShutdownProtest",
        "display_name": "#PoJKInternetShutdownProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PakistanArmyCrackdownPoJK",
        "display_name": "#PakistanArmyCrackdownPoJK",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AwamiActionCommitteeProtest",
        "display_name": "#AwamiActionCommitteeProtest",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#GilgitBaltistanLongMarch",
        "display_name": "#GilgitBaltistanLongMarch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKHumanRightsViolation",
        "display_name": "#PoJKHumanRightsViolation",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PoJKEnforcedDisappearance",
        "display_name": "#PoJKEnforcedDisappearance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#စစ်တပ်",
        "display_name": "#စစ်တပ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#တပ်မတော်",
        "display_name": "#တပ်မတော်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#အကြမ်းဖက်",
        "display_name": "#အကြမ်းဖက်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#အကြမ်းဖက်မှု",
        "display_name": "#အကြမ်းဖက်မှု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#နိုင်ငံရေး",
        "display_name": "#နိုင်ငံရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဆန္ဒပြ",
        "display_name": "#ဆန္ဒပြ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဆူပူမှု",
        "display_name": "#ဆူပူမှု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဂြိုဟ်တု",
        "display_name": "#ဂြိုဟ်တု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဂြိုဟ်တုပုံရိပ်",
        "display_name": "#ဂြိုဟ်တုပုံရိပ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "display_name": "#စောင့်ကြည့်ရေးဂြိုဟ်တု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လေယာဉ်",
        "display_name": "#လေယာဉ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လေကြောင်း",
        "display_name": "#လေကြောင်း",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လေယာဉ်ပျံသန်းမှု",
        "display_name": "#လေယာဉ်ပျံသန်းမှု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လေကြောင်းတိုက်ခိုက်မှု",
        "display_name": "#လေကြောင်းတိုက်ခိုက်မှု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဒရုန်း",
        "display_name": "#ဒရုန်း",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဟယ်လီကော်ပတာ",
        "display_name": "#ဟယ်လီကော်ပတာ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#တိုက်လေယာဉ်",
        "display_name": "#တိုက်လေယာဉ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဗုံးကြဲ",
        "display_name": "#ဗုံးကြဲ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဒုံးကျည်",
        "display_name": "#ဒုံးကျည်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လက်နက်",
        "display_name": "#လက်နက်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#စစ်ဆင်ရေး",
        "display_name": "#စစ်ဆင်ရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#တိုက်ပွဲ",
        "display_name": "#တိုက်ပွဲ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#တပ်ဖွဲ့",
        "display_name": "#တပ်ဖွဲ့",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#လုံခြုံရေး",
        "display_name": "#လုံခြုံရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ဖမ်းဆီး",
        "display_name": "#ဖမ်းဆီး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ပေါက်ကွဲမှု",
        "display_name": "#ပေါက်ကွဲမှု",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#မီးရှို့",
        "display_name": "#မီးရှို့",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#နယ်စပ်",
        "display_name": "#နယ်စပ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#စစ်ရေး",
        "display_name": "#စစ်ရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ကာကွယ်ရေး",
        "display_name": "#ကာကွယ်ရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ထောက်လှမ်းရေး",
        "display_name": "#ထောက်လှမ်းရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ပြည်တွင်းစစ်",
        "display_name": "#ပြည်တွင်းစစ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ငြိမ်းချမ်းရေး",
        "display_name": "#ငြိမ်းချမ်းရေး",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#အရေးပေါ်",
        "display_name": "#အရေးပေါ်",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#သတင်း",
        "display_name": "#သတင်း",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#မြန်မာ",
        "display_name": "#မြန်မာ",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LongMarch7A",
        "display_name": "#LongMarch7A",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LongMarch7",
        "display_name": "#LongMarch7",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CZ7A",
        "display_name": "#CZ7A",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WenchangLaunch",
        "display_name": "#WenchangLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WenchangSpaceLaunchSite",
        "display_name": "#WenchangSpaceLaunchSite",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#CASC",
        "display_name": "#CASC",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaSat4B",
        "display_name": "#ChinaSat4B",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Zhongxing4B",
        "display_name": "#Zhongxing4B",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#YF100Engine",
        "display_name": "#YF100Engine",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#RocketExplosionChina",
        "display_name": "#RocketExplosionChina",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LaunchFailureChina",
        "display_name": "#LaunchFailureChina",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChineseRocketAnomaly",
        "display_name": "#ChineseRocketAnomaly",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#FlightTerminationSystem",
        "display_name": "#FlightTerminationSystem",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaOrbitalLaunchFailure",
        "display_name": "#ChinaOrbitalLaunchFailure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Chang'e7",
        "display_name": "#Chang'e7",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaAcademyOfLaunchVehicleTechnology",
        "display_name": "#ChinaAcademyOfLaunchVehicleTechnology",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#JiuquanLaunch",
        "display_name": "#JiuquanLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XichangLaunch",
        "display_name": "#XichangLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TaiyuanLaunch",
        "display_name": "#TaiyuanLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaNationalRadioLaunchReport",
        "display_name": "#ChinaNationalRadioLaunchReport",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LongMarchExplosion",
        "display_name": "#LongMarchExplosion",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LongMarch",
        "display_name": "#LongMarch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Wenchang",
        "display_name": "#Wenchang",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#RocketLaunch",
        "display_name": "#RocketLaunch",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LaunchFailure",
        "display_name": "#LaunchFailure",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#SpaceNews",
        "display_name": "#SpaceNews",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#ChinaSpace",
        "display_name": "#ChinaSpace",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#YF100",
        "display_name": "#YF100",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalFloods",
        "display_name": "#NepalFloods",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalFloodDisaster",
        "display_name": "#NepalFloodDisaster",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalFloodAlert",
        "display_name": "#NepalFloodAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#KatmanduFloods",
        "display_name": "#KatmanduFloods",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalMonsoon",
        "display_name": "#NepalMonsoon",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalRainfall",
        "display_name": "#NepalRainfall",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalMudslide",
        "display_name": "#NepalMudslide",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalLandslide",
        "display_name": "#NepalLandslide",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalFloodRelief",
        "display_name": "#NepalFloodRelief",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalDisasterResponse",
        "display_name": "#NepalDisasterResponse",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalEmergency",
        "display_name": "#NepalEmergency",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalDisasterNews",
        "display_name": "#NepalDisasterNews",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalNaturalDisaster",
        "display_name": "#NepalNaturalDisaster",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalFloodUpdate",
        "display_name": "#NepalFloodUpdate",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#NepalWeatherAlert",
        "display_name": "#NepalWeatherAlert",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BRICSLeaderHealth",
        "display_name": "#BRICSLeaderHealth",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BRICSSummitHealth",
        "display_name": "#BRICSSummitHealth",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#WorldLeaderIllness",
        "display_name": "#WorldLeaderIllness",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LulaHealth",
        "display_name": "#LulaHealth",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#LulaHospitalized",
        "display_name": "#LulaHospitalized",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XiJinpingHealth",
        "display_name": "#XiJinpingHealth",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#XiJinpingAbsence",
        "display_name": "#XiJinpingAbsence",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PutinHealthRumors",
        "display_name": "#PutinHealthRumors",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PutinIllness",
        "display_name": "#PutinIllness",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#BRICSSummit",
        "display_name": "#BRICSSummit",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#Powerus",
        "display_name": "#Powerus",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PowerusDrone",
        "display_name": "#PowerusDrone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#TrumpSonsDrone",
        "display_name": "#TrumpSonsDrone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#EricTrumpDrone",
        "display_name": "#EricTrumpDrone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DonaldTrumpJrDrone",
        "display_name": "#DonaldTrumpJrDrone",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PowerusPentagon",
        "display_name": "#PowerusPentagon",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#AureusGreenway",
        "display_name": "#AureusGreenway",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#PowerusIPO",
        "display_name": "#PowerusIPO",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#USDroneManufacturer",
        "display_name": "#USDroneManufacturer",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "#DroneDominance",
        "display_name": "#DroneDominance",
        "category": "Instagram Hashtags",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@AJ_FI (Andrew Jones)",
        "display_name": "@AJ_FI (Andrew Jones)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@NASASpaceflight",
        "display_name": "@NASASpaceflight",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@planet4589 (Jonathan McDowell)",
        "display_name": "@planet4589 (Jonathan McDowell)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@Space_Station",
        "display_name": "@Space_Station",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@SpaceflightNow",
        "display_name": "@SpaceflightNow",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "@thesheetztweetz (Michael Sheetz)",
        "display_name": "@thesheetztweetz (Michael Sheetz)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "afghanistan",
        "display_name": "afghanistan",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "China-in-Space (china-in-space.com)",
        "display_name": "China-in-Space (china-in-space.com)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Interesting Engineering (space section)",
        "display_name": "Interesting Engineering (space section)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "khyber",
        "display_name": "khyber",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "pakhtunkhwa",
        "display_name": "pakhtunkhwa",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Pakistan",
        "display_name": "Pakistan",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "pakTTP",
        "display_name": "pakTTP",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "pashto",
        "display_name": "pashto",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "pashtun",
        "display_name": "pashtun",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "SatNews",
        "display_name": "SatNews",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "SpaceNews (spacenews.com)",
        "display_name": "SpaceNews (spacenews.com)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "taliban",
        "display_name": "taliban",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@AltNewsIn",
        "display_name": "AltNewsIn",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "AltNewsIn",
        "display_name": "AltNewsIn",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "bjiofficial",
        "display_name": "bjiofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@bnonews",
        "display_name": "bnonews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "bnonews",
        "display_name": "bnonews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "breaking911",
        "display_name": "breaking911",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "cgtn_europe",
        "display_name": "cgtn_europe",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "CockroachJanataParty2K26",
        "display_name": "CockroachJanataParty2K26",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@LoksattaLive",
        "display_name": "LoksattaLive",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "LoksattaLive",
        "display_name": "LoksattaLive",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "Middle_East_Spectator",
        "display_name": "Middle_East_Spectator",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "MilitarySummary",
        "display_name": "MilitarySummary",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "news18dotcom",
        "display_name": "news18dotcom",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "rtnews",
        "display_name": "rtnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "TheInsider",
        "display_name": "TheInsider",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "ukraine_watch",
        "display_name": "ukraine_watch",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@USGS_Earthquakes",
        "display_name": "USGS_Earthquakes",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "USGS_Earthquakes",
        "display_name": "USGS_Earthquakes",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "voachinese",
        "display_name": "voachinese",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "war_monitor",
        "display_name": "war_monitor",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@aaaaaaa",
        "display_name": "aaaaaaa",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@aajtakofficial",
        "display_name": "aajtakofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@BBC_Chinese_news",
        "display_name": "BBC_Chinese_news",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@BengalSultanate",
        "display_name": "BengalSultanate",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@Conflict_Watch_Bangla",
        "display_name": "Conflict_Watch_Bangla",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@cybdetective",
        "display_name": "cybdetective",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@dailyosint",
        "display_name": "dailyosint",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "dailyosint",
        "display_name": "dailyosint",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@disclosetv",
        "display_name": "disclosetv",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@insiderpaper",
        "display_name": "insiderpaper",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@intelslava",
        "display_name": "intelslava",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@justiceforcbse",
        "display_name": "justiceforcbse",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@liveuamap",
        "display_name": "liveuamap",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@malwarebazaar",
        "display_name": "malwarebazaar",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "malwarebazaar",
        "display_name": "malwarebazaar",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@OSINTdefender",
        "display_name": "OSINTdefender",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@polimernewsofficial",
        "display_name": "polimernewsofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "polimernewsofficial",
        "display_name": "polimernewsofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@ransomwarelive",
        "display_name": "ransomwarelive",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@RONBupdates",
        "display_name": "RONBupdates",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "RONBupdates",
        "display_name": "RONBupdates",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@rybar",
        "display_name": "rybar",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@scmpnews",
        "display_name": "scmpnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@vxunderground",
        "display_name": "vxunderground",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@xhnews",
        "display_name": "xhnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@vermilion_china",
        "display_name": "vermilion_china",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@someplacein",
        "display_name": "someplacein",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@caseyresearch",
        "display_name": "caseyresearch",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Minaitech",
        "display_name": "Minaitech",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Rational534259",
        "display_name": "Rational534259",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@OGPR_February",
        "display_name": "OGPR_February",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@srilanka_navy",
        "display_name": "srilanka_navy",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Mukammed587",
        "display_name": "Mukammed587",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@bokeamnat",
        "display_name": "bokeamnat",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CathayCatch",
        "display_name": "CathayCatch",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MilSourceA",
        "display_name": "MilSourceA",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Consecutive",
        "display_name": "Consecutive",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaBriefUT",
        "display_name": "ChinaBriefUT",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CSISFreeman",
        "display_name": "CSISFreeman",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@east_asia_forum",
        "display_name": "east_asia_forum",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ROArmyOfficial",
        "display_name": "ROArmyOfficial",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BangladeshAFD",
        "display_name": "BangladeshAFD",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MilitaryDNA_BD",
        "display_name": "MilitaryDNA_BD",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DefenceBD",
        "display_name": "DefenceBD",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MINDEF_Official",
        "display_name": "MINDEF_Official",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MarineInsight",
        "display_name": "MarineInsight",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@minyar_baloch",
        "display_name": "minyar_baloch",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@hyrbyair_marri",
        "display_name": "hyrbyair_marri",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@bsosenglish",
        "display_name": "bsosenglish",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AfghanAnalyst2",
        "display_name": "AfghanAnalyst2",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@TOLONewsEnglish",
        "display_name": "TOLONewsEnglish",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HurriyatPk",
        "display_name": "HurriyatPk",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@dispatcherOnX",
        "display_name": "dispatcherOnX",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IEAUrduOfficial",
        "display_name": "IEAUrduOfficial",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@anuradissanayake",
        "display_name": "anuradissanayake",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SriLankaTweet",
        "display_name": "SriLankaTweet",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Dr_HariniN",
        "display_name": "Dr_HariniN",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Relukseyar4",
        "display_name": "Relukseyar4",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RW_SRILANKA",
        "display_name": "RW_SRILANKA",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ManoGanesan",
        "display_name": "ManoGanesan",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CBSriLanka",
        "display_name": "CBSriLanka",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MFA_SriLanka",
        "display_name": "MFA_SriLanka",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ConsulateSL",
        "display_name": "ConsulateSL",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@airforce4k",
        "display_name": "airforce4k",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SLCoastGuard",
        "display_name": "SLCoastGuard",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MNLalza",
        "display_name": "MNLalza",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Irusamhm",
        "display_name": "Irusamhm",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MohamedNasheed",
        "display_name": "MohamedNasheed",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@pra_ymn",
        "display_name": "pra_ymn",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MoosaZameer",
        "display_name": "MoosaZameer",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ThoriqIbrahim",
        "display_name": "ThoriqIbrahim",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@alimees3e",
        "display_name": "alimees3e",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PakInMaldives",
        "display_name": "PakInMaldives",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaEmbass_MV",
        "display_name": "ChinaEmbass_MV",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MEAPakistan",
        "display_name": "MEAPakistan",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MDVinChina",
        "display_name": "MDVinChina",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MDVinAnkara",
        "display_name": "MDVinAnkara",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MDVinUSA",
        "display_name": "MDVinUSA",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@governmentmv",
        "display_name": "governmentmv",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MDPSecretariat",
        "display_name": "MDPSecretariat",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@pnc_secretariat",
        "display_name": "pnc_secretariat",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@reuters",
        "display_name": "reuters",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@OsintTV",
        "display_name": "OsintTV",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@TOLOnews",
        "display_name": "TOLOnews",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SCMPNews",
        "display_name": "SCMPNews",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CNSpaceflight",
        "display_name": "CNSpaceflight",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IndiaTodayGlobal",
        "display_name": "IndiaTodayGlobal",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PakInSriLanka",
        "display_name": "PakInSriLanka",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SriLanka",
        "display_name": "SriLanka",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Dr_Harsh",
        "display_name": "Dr_Harsh",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaEmb",
        "display_name": "ChinaEmb",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PMOIndia",
        "display_name": "PMOIndia",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DefenceMinIndia",
        "display_name": "DefenceMinIndia",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@spokespersonMoD",
        "display_name": "spokespersonMoD",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@indiannavy",
        "display_name": "indiannavy",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MEAIndia",
        "display_name": "MEAIndia",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@huaxianzi",
        "display_name": "huaxianzi",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Vision_china",
        "display_name": "Vision_china",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RupprechtDeino",
        "display_name": "RupprechtDeino",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@rao_ba",
        "display_name": "rao_ba",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@OGPI_PB",
        "display_name": "OGPI_PB",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@GII_Official",
        "display_name": "GII_Official",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@rajfortytwo",
        "display_name": "rajfortytwo",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CathayPak",
        "display_name": "CathayPak",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MilSource",
        "display_name": "MilSource",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@neilthomas",
        "display_name": "neilthomas",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RGAnurag",
        "display_name": "RGAnurag",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MINDEF_ZA",
        "display_name": "MINDEF_ZA",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@minyar_osint",
        "display_name": "minyar_osint",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@hryb_watch",
        "display_name": "hryb_watch",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BalochInsight",
        "display_name": "BalochInsight",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HumInt_Watch",
        "display_name": "HumInt_Watch",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IEAUfficial",
        "display_name": "IEAUfficial",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@sajithpremadasa",
        "display_name": "sajithpremadasa",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SLCG_Official",
        "display_name": "SLCG_Official",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MohamedMuizzu",
        "display_name": "MohamedMuizzu",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@pra_yush",
        "display_name": "pra_yush",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Thorliq_mv",
        "display_name": "Thorliq_mv",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PNC_Secretariat",
        "display_name": "PNC_Secretariat",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Osint613",
        "display_name": "Osint613",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@osmasstro",
        "display_name": "osmasstro",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@greatestfailure_ever",
        "display_name": "greatestfailure_ever",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HinDoc",
        "display_name": "HinDoc",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@WanderlustView",
        "display_name": "WanderlustView",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@fireflychild024",
        "display_name": "fireflychild024",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@user9145398166819",
        "display_name": "user9145398166819",
        "category": "Twitter / X",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "ADGPI - INDIAN ARMY",
        "display_name": "ADGPI - INDIAN ARMY",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Indian Navy",
        "display_name": "Indian Navy",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Ministry of Defence, Government of India",
        "display_name": "Ministry of Defence, Government of India",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "NITI Aayog, Government of India",
        "display_name": "NITI Aayog, Government of India",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "INTERPOL",
        "display_name": "INTERPOL",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "South China Morning Post",
        "display_name": "South China Morning Post",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Global Times",
        "display_name": "Global Times",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "BBC News",
        "display_name": "BBC News",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "CNN",
        "display_name": "CNN",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Zee News",
        "display_name": "Zee News",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Aaj Tak",
        "display_name": "Aaj Tak",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "News18 India",
        "display_name": "News18 India",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "TV9 Bangla",
        "display_name": "TV9 Bangla",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "ARY News",
        "display_name": "ARY News",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "UGC India",
        "display_name": "UGC India",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Space.com",
        "display_name": "Space.com",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Bundesregierung",
        "display_name": "Bundesregierung",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Press Information Bureau (PIB India)",
        "display_name": "Press Information Bureau (PIB India)",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Reuters News",
        "display_name": "Reuters News",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "NDTV",
        "display_name": "NDTV",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Republic TV",
        "display_name": "Republic TV",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Al Jazeera English",
        "display_name": "Al Jazeera English",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "Deccan Herald",
        "display_name": "Deccan Herald",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "The Times of India",
        "display_name": "The Times of India",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "whatsapp",
        "handle": "The Hindu",
        "display_name": "The Hindu",
        "category": "WhatsApp",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u4fa6\u5bdf\u536b\u661f",
        "display_name": "\u4fa6\u5bdf\u536b\u661f",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u5175\u5668",
        "display_name": "\u5175\u5668",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u4e8b",
        "display_name": "\u519b\u4e8b",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u4e8b\u7269\u6d41",
        "display_name": "\u519b\u4e8b\u7269\u6d41",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u4e8b\u79d1\u6280",
        "display_name": "\u519b\u4e8b\u79d1\u6280",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u4eba\u88c5\u5907",
        "display_name": "\u519b\u4eba\u88c5\u5907",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u5907\u7ade\u8d5b",
        "display_name": "\u519b\u5907\u7ade\u8d5b",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u5de5\u4f01\u4e1a",
        "display_name": "\u519b\u5de5\u4f01\u4e1a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u60c5\u89c2\u5bdf",
        "display_name": "\u519b\u60c5\u89c2\u5bdf",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u6c11\u878d\u5408",
        "display_name": "\u519b\u6c11\u878d\u5408",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u6f14",
        "display_name": "\u519b\u6f14",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u8854\u5236",
        "display_name": "\u519b\u8854\u5236",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u519b\u8d39\u9884\u7b97",
        "display_name": "\u519b\u8d39\u9884\u7b97",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u53cd\u5bfc\u7cfb\u7edf",
        "display_name": "\u53cd\u5bfc\u7cfb\u7edf",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u540e\u52e4\u4fdd\u969c",
        "display_name": "\u540e\u52e4\u4fdd\u969c",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u54e8\u6240\u9632\u52a1",
        "display_name": "\u54e8\u6240\u9632\u52a1",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u56fd\u9632",
        "display_name": "\u56fd\u9632",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u5730\u7f18\u653f\u6cbb",
        "display_name": "\u5730\u7f18\u653f\u6cbb",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u5bfc\u5f39",
        "display_name": "\u5bfc\u5f39",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u5de1\u822a\u5bfc\u5f39",
        "display_name": "\u5de1\u822a\u5bfc\u5f39",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u5f39\u9053\u5bfc\u5f39",
        "display_name": "\u5f39\u9053\u5bfc\u5f39",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u60c5\u62a5",
        "display_name": "\u60c5\u62a5",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u533a\u53f8\u4ee4\u90e8",
        "display_name": "\u6218\u533a\u53f8\u4ee4\u90e8",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u672f",
        "display_name": "\u6218\u672f",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u672f\u8f70\u70b8\u673a",
        "display_name": "\u6218\u672f\u8f70\u70b8\u673a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u7565",
        "display_name": "\u6218\u7565",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u7565\u5a01\u6151",
        "display_name": "\u6218\u7565\u5a01\u6151",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6218\u7565\u8f70\u70b8\u673a",
        "display_name": "\u6218\u7565\u8f70\u70b8\u673a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u65e0\u4eba\u673a",
        "display_name": "\u65e0\u4eba\u673a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6838\u6f5c\u8247",
        "display_name": "\u6838\u6f5c\u8247",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6b66\u5668\u88c5\u5907",
        "display_name": "\u6b66\u5668\u88c5\u5907",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6b66\u88c5\u529b\u91cf",
        "display_name": "\u6b66\u88c5\u529b\u91cf",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6d77\u519b",
        "display_name": "\u6d77\u519b",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u6d77\u8b66\u90e8\u961f",
        "display_name": "\u6d77\u8b66\u90e8\u961f",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u706b\u70ae\u7cfb\u7edf",
        "display_name": "\u706b\u70ae\u7cfb\u7edf",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u7279\u79cd\u90e8\u961f",
        "display_name": "\u7279\u79cd\u90e8\u961f",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u73b0\u4ee3\u6218\u4e89",
        "display_name": "\u73b0\u4ee3\u6218\u4e89",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u7535\u5b50\u6218",
        "display_name": "\u7535\u5b50\u6218",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u7a7a\u519b",
        "display_name": "\u7a7a\u519b",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u7a7a\u95f4\u6218",
        "display_name": "\u7a7a\u95f4\u6218",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u7f51\u7edc\u5b89\u5168",
        "display_name": "\u7f51\u7edc\u5b89\u5168",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u8054\u5408\u4f5c\u6218",
        "display_name": "\u8054\u5408\u4f5c\u6218",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u822a\u6bcd",
        "display_name": "\u822a\u6bcd",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u8230\u8f7d\u673a",
        "display_name": "\u8230\u8f7d\u673a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u88c5\u7532\u8f66",
        "display_name": "\u88c5\u7532\u8f66",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u8fb9\u754c\u9632\u52a1",
        "display_name": "\u8fb9\u754c\u9632\u52a1",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u9632\u7a7a\u5bfc\u5f39",
        "display_name": "\u9632\u7a7a\u5bfc\u5f39",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u9646\u519b",
        "display_name": "\u9646\u519b",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u9690\u5f62\u6218\u673a",
        "display_name": "\u9690\u5f62\u6218\u673a",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "baidu",
        "handle": "\u96f7\u8fbe",
        "display_name": "\u96f7\u8fbe",
        "category": "Baidu",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "ADGPI - Indian Army",
        "display_name": "ADGPI - Indian Army",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "China Military Bugle",
        "display_name": "China Military Bugle",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Denzil Naidoo",
        "display_name": "Denzil Naidoo",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "DNA India",
        "display_name": "DNA India",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "India Today",
        "display_name": "India Today",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Indian Navy",
        "display_name": "Indian Navy",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Online News ",
        "display_name": "Online News ",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Sri Lanka Navy",
        "display_name": "Sri Lanka Navy",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "The Economic Times",
        "display_name": "The Economic Times",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "The National Defense",
        "display_name": "The National Defense",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "The Times of India",
        "display_name": "The Times of India",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "TIMES NOW",
        "display_name": "TIMES NOW",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Trekking In Nepal",
        "display_name": "Trekking In Nepal",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Yash Raut",
        "display_name": "Yash Raut",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "facebook",
        "handle": "Zee News English",
        "display_name": "Zee News English",
        "category": "Facebook",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@all_source_news",
        "display_name": "all_source_news",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@bakerinstitute",
        "display_name": "bakerinstitute",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@bipss.org.bd",
        "display_name": "bipss.org.bd",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@bji.official",
        "display_name": "bji.official",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@bnpbd",
        "display_name": "bnpbd",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@cenjowsindia",
        "display_name": "cenjowsindia",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@chathamhouse_org",
        "display_name": "chathamhouse_org",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@cpd_bangladesh",
        "display_name": "cpd_bangladesh",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@defencedirecteducation",
        "display_name": "defencedirecteducation",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@defense_talks",
        "display_name": "defense_talks",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@economictimes",
        "display_name": "economictimes",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@impriindia",
        "display_name": "impriindia",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@indiadotcom",
        "display_name": "indiadotcom",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@indopacificreport",
        "display_name": "indopacificreport",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@mfa_china",
        "display_name": "mfa_china",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@mima_malaysia",
        "display_name": "mima_malaysia",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@modefencemv",
        "display_name": "modefencemv",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@modjapan_en",
        "display_name": "modjapan_en",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@navalinstitute",
        "display_name": "navalinstitute",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@unodc_hq",
        "display_name": "unodc_hq",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@pakinsrilanka",
        "display_name": "pakinsrilanka",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@pakistan_navy",
        "display_name": "pakistan_navy",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@randcorporation",
        "display_name": "randcorporation",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@robbanassafina",
        "display_name": "robbanassafina",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@usanasfoundation",
        "display_name": "usanasfoundation",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@scramble_nl",
        "display_name": "scramble_nl",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@srilankanavy",
        "display_name": "srilankanavy",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@technical_astra_88",
        "display_name": "technical_astra_88",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@wiomsa",
        "display_name": "wiomsa",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "instagram",
        "handle": "@zaobaosg",
        "display_name": "zaobaosg",
        "category": "Instagram",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "U.S. Naval Institute",
        "display_name": "U.S. Naval Institute",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Armando Johan Obdola",
        "display_name": "Armando Johan Obdola",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Australian Strategic Policy Institute",
        "display_name": "Australian Strategic Policy Institute",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Capt.(Dr.) S G Naravane",
        "display_name": "Capt.(Dr.) S G Naravane",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Center for Strategic and International Studies (CSIS)",
        "display_name": "Center for Strategic and International Studies (CSIS)",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Centre for Joint Warfare Studies (CENJOWS)",
        "display_name": "Centre for Joint Warfare Studies (CENJOWS)",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Centre For Land Warfare Studies (CLAWS)",
        "display_name": "Centre For Land Warfare Studies (CLAWS)",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "China Beige Book (CBB)",
        "display_name": "China Beige Book (CBB)",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Confidential Careers",
        "display_name": "Confidential Careers",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "East Asia Forum",
        "display_name": "East Asia Forum",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "IGAD",
        "display_name": "IGAD",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Indo-Pacific Insight",
        "display_name": "Indo-Pacific Insight",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "International Institute for Strategic Studies",
        "display_name": "International Institute for Strategic Studies",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Maritime Institute of Malaysia",
        "display_name": "Maritime Institute of Malaysia",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "MP-IDSA",
        "display_name": "MP-IDSA",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Observer Research Foundation",
        "display_name": "Observer Research Foundation",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Opportunities for Youth",
        "display_name": "Opportunities for Youth",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Pakistan Armed Forces",
        "display_name": "Pakistan Armed Forces",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "RAND",
        "display_name": "RAND",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "RMS International",
        "display_name": "RMS International",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "SIPRI",
        "display_name": "SIPRI",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "Sri Lanka Navy",
        "display_name": "Sri Lanka Navy",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "The Cyber Security Hub",
        "display_name": "The Cyber Security Hub",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "VIF India",
        "display_name": "VIF India",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "linkedin",
        "handle": "WIOMSA - Western Indian Ocean Marine Science Association",
        "display_name": "WIOMSA - Western Indian Ocean Marine Science Association",
        "category": "Linkedin",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#airforce",
        "display_name": "#airforce",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#army",
        "display_name": "#army",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#artillery",
        "display_name": "#artillery",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#aviation",
        "display_name": "#aviation",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#ballistics",
        "display_name": "#ballistics",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#bordersecurity",
        "display_name": "#bordersecurity",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#bundeswehr",
        "display_name": "#bundeswehr",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#centcom",
        "display_name": "#centcom",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#conflict",
        "display_name": "#conflict",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#counterterrorism",
        "display_name": "#counterterrorism",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#cyberdefense",
        "display_name": "#cyberdefense",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#cybersecurity",
        "display_name": "#cybersecurity",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#defense",
        "display_name": "#defense",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#drones",
        "display_name": "#drones",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#electronicwarfare",
        "display_name": "#electronicwarfare",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#eucom",
        "display_name": "#eucom",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#foreignpolicy",
        "display_name": "#foreignpolicy",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#geoint",
        "display_name": "#geoint",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#geopolitics",
        "display_name": "#geopolitics",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#humint",
        "display_name": "#humint",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#hypersonic",
        "display_name": "#hypersonic",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#indopacom",
        "display_name": "#indopacom",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#infantry",
        "display_name": "#infantry",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#intelligence",
        "display_name": "#intelligence",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#isaf",
        "display_name": "#isaf",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#logistics",
        "display_name": "#logistics",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#marines",
        "display_name": "#marines",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#maritime",
        "display_name": "#maritime",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#military",
        "display_name": "#military",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#missiles",
        "display_name": "#missiles",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#nato",
        "display_name": "#nato",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#navy",
        "display_name": "#navy",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#osint",
        "display_name": "#osint",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#pentagon",
        "display_name": "#pentagon",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#pla",
        "display_name": "#pla",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#radar",
        "display_name": "#radar",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#reconnaissance",
        "display_name": "#reconnaissance",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#sanctions",
        "display_name": "#sanctions",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#satellites",
        "display_name": "#satellites",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#sigint",
        "display_name": "#sigint",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#socom",
        "display_name": "#socom",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#spaceforce",
        "display_name": "#spaceforce",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#spaceops",
        "display_name": "#spaceops",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#stealth",
        "display_name": "#stealth",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#stratcom",
        "display_name": "#stratcom",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#submarines",
        "display_name": "#submarines",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#tactical",
        "display_name": "#tactical",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#unpost",
        "display_name": "#unpost",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#war",
        "display_name": "#war",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "mastodon",
        "handle": "#wargaming",
        "display_name": "#wargaming",
        "category": "Mastodon",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "air_defense",
        "display_name": "air_defense",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "aircraft_carriers",
        "display_name": "aircraft_carriers",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "amphibious_assault",
        "display_name": "amphibious_assault",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "armored_vehicles",
        "display_name": "armored_vehicles",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "artillery_systems",
        "display_name": "artillery_systems",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "ballistic_missiles",
        "display_name": "ballistic_missiles",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "border_defenses",
        "display_name": "border_defenses",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "camouflage",
        "display_name": "camouflage",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "coast_guard",
        "display_name": "coast_guard",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "combat_gear",
        "display_name": "combat_gear",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "combat_medical",
        "display_name": "combat_medical",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "combat_uniforms",
        "display_name": "combat_uniforms",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "command_bunkers",
        "display_name": "command_bunkers",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "cyber_warfare",
        "display_name": "cyber_warfare",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "defense_strategy",
        "display_name": "defense_strategy",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "defense_technology",
        "display_name": "defense_technology",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "drone_technology",
        "display_name": "drone_technology",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "electronic_warfare",
        "display_name": "electronic_warfare",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "fighter_jets",
        "display_name": "fighter_jets",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "firearms_design",
        "display_name": "firearms_design",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "fortifications",
        "display_name": "fortifications",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "infantry_weapons",
        "display_name": "infantry_weapons",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "intelligence_analysis",
        "display_name": "intelligence_analysis",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "logistical_support",
        "display_name": "logistical_support",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military_aviation",
        "display_name": "military_aviation",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military_drones",
        "display_name": "military_drones",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military_history",
        "display_name": "military_history",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military_logistics",
        "display_name": "military_logistics",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "military_tactics",
        "display_name": "military_tactics",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "missile_defense",
        "display_name": "missile_defense",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "naval_tactics",
        "display_name": "naval_tactics",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "naval_warships",
        "display_name": "naval_warships",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "nuclear_submarines",
        "display_name": "nuclear_submarines",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "osint_maps",
        "display_name": "osint_maps",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "radar_systems",
        "display_name": "radar_systems",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "reconnaissance_aircraft",
        "display_name": "reconnaissance_aircraft",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "satellite_imagery",
        "display_name": "satellite_imagery",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "siege_tactics",
        "display_name": "siege_tactics",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "signal_intelligence",
        "display_name": "signal_intelligence",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "space_defense",
        "display_name": "space_defense",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "special_forces",
        "display_name": "special_forces",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "stealth_aircraft",
        "display_name": "stealth_aircraft",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "strategic_assets",
        "display_name": "strategic_assets",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "submarine_tech",
        "display_name": "submarine_tech",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "tactical_communications",
        "display_name": "tactical_communications",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "tactical_gear",
        "display_name": "tactical_gear",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "tank_designs",
        "display_name": "tank_designs",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "trench_warfare",
        "display_name": "trench_warfare",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "warfare_history",
        "display_name": "warfare_history",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "pinterest",
        "handle": "warship_models",
        "display_name": "warship_models",
        "category": "Pinterest",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "7700bot",
        "display_name": "7700bot",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "adulting4kids",
        "display_name": "adulting4kids",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "armyreco",
        "display_name": "armyreco",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "ATF1511",
        "display_name": "ATF1511",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CarmenMjkg",
        "display_name": "CarmenMjkg",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "CupEcstatic2721",
        "display_name": "CupEcstatic2721",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "dinkomaricic",
        "display_name": "dinkomaricic",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "falconupkid",
        "display_name": "falconupkid",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "georgebounacos",
        "display_name": "georgebounacos",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "GroundbreakingBoss14",
        "display_name": "GroundbreakingBoss14",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "IndiaTodayGlobal",
        "display_name": "IndiaTodayGlobal",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "jobhuntify",
        "display_name": "jobhuntify",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "John3262005",
        "display_name": "John3262005",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Key-Needleworker-702",
        "display_name": "Key-Needleworker-702",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Mamuthone125",
        "display_name": "Mamuthone125",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "MarketFlux",
        "display_name": "MarketFlux",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Miao_Yin8964",
        "display_name": "Miao_Yin8964",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Reddenbawker",
        "display_name": "Reddenbawker",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Rude-Result7362",
        "display_name": "Rude-Result7362",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "reddit",
        "handle": "Slow-Property5895",
        "display_name": "Slow-Property5895",
        "category": "Reddit",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "air_defense",
        "display_name": "air_defense",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "airforce_daily",
        "display_name": "airforce_daily",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "arms_control",
        "display_name": "arms_control",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "army_updates",
        "display_name": "army_updates",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "aviation_report",
        "display_name": "aviation_report",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "battle_log",
        "display_name": "battle_log",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "border_watch",
        "display_name": "border_watch",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "breaking_war",
        "display_name": "breaking_war",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "combat_reports",
        "display_name": "combat_reports",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "command_center",
        "display_name": "command_center",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "counter_intel",
        "display_name": "counter_intel",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "crisis_watch",
        "display_name": "crisis_watch",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "cyber_defense",
        "display_name": "cyber_defense",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "defense_news",
        "display_name": "defense_news",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "defense_tech",
        "display_name": "defense_tech",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "drone_watch",
        "display_name": "drone_watch",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "emergency_alert",
        "display_name": "emergency_alert",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "force_posture",
        "display_name": "force_posture",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "foreign_affairs",
        "display_name": "foreign_affairs",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "frontline_news",
        "display_name": "frontline_news",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "geoint_digest",
        "display_name": "geoint_digest",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "geopolitics",
        "display_name": "geopolitics",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "global_conflict",
        "display_name": "global_conflict",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "global_security",
        "display_name": "global_security",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "homeland_security",
        "display_name": "homeland_security",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "intel_report",
        "display_name": "intel_report",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "intelligence_brief",
        "display_name": "intelligence_brief",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "maritime_sec",
        "display_name": "maritime_sec",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "military_brief",
        "display_name": "military_brief",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "missile_tracker",
        "display_name": "missile_tracker",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "NATO_updates",
        "display_name": "NATO_updates",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "naval_news",
        "display_name": "naval_news",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "navy_brief",
        "display_name": "navy_brief",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "nuclear_watch",
        "display_name": "nuclear_watch",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "osint_updates",
        "display_name": "osint_updates",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "PLA_monitor",
        "display_name": "PLA_monitor",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "radar_intel",
        "display_name": "radar_intel",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "reconnaissance",
        "display_name": "reconnaissance",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "security_bulletin",
        "display_name": "security_bulletin",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "space_force",
        "display_name": "space_force",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "specops_news",
        "display_name": "specops_news",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "stealth_tech",
        "display_name": "stealth_tech",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "strategic_defense",
        "display_name": "strategic_defense",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "strategic_news",
        "display_name": "strategic_news",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "tactical_intel",
        "display_name": "tactical_intel",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "troop_movements",
        "display_name": "troop_movements",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "war_analysis",
        "display_name": "war_analysis",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "war_dispatch",
        "display_name": "war_dispatch",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "warfare_journal",
        "display_name": "warfare_journal",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "snapchat",
        "handle": "weapon_systems",
        "display_name": "weapon_systems",
        "category": "Snapchat",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "aaaaaaa",
        "display_name": "aaaaaaa",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "aajtakofficial",
        "display_name": "aajtakofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "BBC_Chinese_news",
        "display_name": "BBC_Chinese_news",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "BengalSultanate",
        "display_name": "BengalSultanate",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@bjiofficial",
        "display_name": "@bjiofficial",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@breaking911",
        "display_name": "@breaking911",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@cgtn_europe",
        "display_name": "@cgtn_europe",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@CockroachJanataParty2K26",
        "display_name": "@CockroachJanataParty2K26",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "Conflict_Watch_Bangla",
        "display_name": "Conflict_Watch_Bangla",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "cybdetective",
        "display_name": "cybdetective",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "disclosetv",
        "display_name": "disclosetv",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "insiderpaper",
        "display_name": "insiderpaper",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "intelslava",
        "display_name": "intelslava",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "justiceforcbse",
        "display_name": "justiceforcbse",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@ukraine_watch",
        "display_name": "@ukraine_watch",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "liveuamap",
        "display_name": "liveuamap",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@Middle_East_Spectator",
        "display_name": "@Middle_East_Spectator",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@MilitarySummary",
        "display_name": "@MilitarySummary",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@myanmarnownews",
        "display_name": "@myanmarnownews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@news18dotcom",
        "display_name": "@news18dotcom",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "OSINTdefender",
        "display_name": "OSINTdefender",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "ransomwarelive",
        "display_name": "ransomwarelive",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@rtnews",
        "display_name": "@rtnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "rybar",
        "display_name": "rybar",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "scmpnews",
        "display_name": "scmpnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@TheInsider",
        "display_name": "@TheInsider",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@voachinese",
        "display_name": "@voachinese",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "vxunderground",
        "display_name": "vxunderground",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "@war_monitor",
        "display_name": "@war_monitor",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "telegram",
        "handle": "xhnews",
        "display_name": "xhnews",
        "category": "Telegram",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#airdefense",
        "display_name": "#airdefense",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#airforce",
        "display_name": "#airforce",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#armoredvehicle",
        "display_name": "#armoredvehicle",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#army",
        "display_name": "#army",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#artillery",
        "display_name": "#artillery",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#breakingnews",
        "display_name": "#breakingnews",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#bundeswehr",
        "display_name": "#bundeswehr",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#combat",
        "display_name": "#combat",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#commandos",
        "display_name": "#commandos",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#counterterrorism",
        "display_name": "#counterterrorism",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#cyber",
        "display_name": "#cyber",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#defense",
        "display_name": "#defense",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#defenseindustry",
        "display_name": "#defenseindustry",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#drones",
        "display_name": "#drones",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#fighterjet",
        "display_name": "#fighterjet",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#frontline",
        "display_name": "#frontline",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#geopolitics",
        "display_name": "#geopolitics",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#geopolitique",
        "display_name": "#geopolitique",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#history",
        "display_name": "#history",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#indianarmy",
        "display_name": "#indianarmy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#infantry",
        "display_name": "#infantry",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#intel",
        "display_name": "#intel",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#marines",
        "display_name": "#marines",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#military",
        "display_name": "#military",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#missile",
        "display_name": "#missile",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#nationalsecurity",
        "display_name": "#nationalsecurity",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#nato",
        "display_name": "#nato",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#navy",
        "display_name": "#navy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#osint",
        "display_name": "#osint",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#pentagon",
        "display_name": "#pentagon",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#pla",
        "display_name": "#pla",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#radar",
        "display_name": "#radar",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#royalnavy",
        "display_name": "#royalnavy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#security",
        "display_name": "#security",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#securityforces",
        "display_name": "#securityforces",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#spaceforce",
        "display_name": "#spaceforce",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#specialforces",
        "display_name": "#specialforces",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#stealth",
        "display_name": "#stealth",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#strategy",
        "display_name": "#strategy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#submarines",
        "display_name": "#submarines",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#tactical",
        "display_name": "#tactical",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#tank",
        "display_name": "#tank",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#usairforce",
        "display_name": "#usairforce",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#usarmy",
        "display_name": "#usarmy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#usnavy",
        "display_name": "#usnavy",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#veterans",
        "display_name": "#veterans",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#warfare",
        "display_name": "#warfare",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#wargames",
        "display_name": "#wargames",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#warship",
        "display_name": "#warship",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "tiktok",
        "handle": "#weapons",
        "display_name": "#weapons",
        "category": "TikTok",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#aircraftcarrier",
        "display_name": "#aircraftcarrier",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#airdefense",
        "display_name": "#airdefense",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#airforce",
        "display_name": "#airforce",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#armedforces",
        "display_name": "#armedforces",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#armsdeal",
        "display_name": "#armsdeal",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#army",
        "display_name": "#army",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#ballisticmissile",
        "display_name": "#ballisticmissile",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#bordersecurity",
        "display_name": "#bordersecurity",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#centcom",
        "display_name": "#centcom",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#china",
        "display_name": "#china",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#counterterrorism",
        "display_name": "#counterterrorism",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#cybersecurity",
        "display_name": "#cybersecurity",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#defense",
        "display_name": "#defense",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#defensespending",
        "display_name": "#defensespending",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#drones",
        "display_name": "#drones",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#foreignpolicy",
        "display_name": "#foreignpolicy",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#foreignrelations",
        "display_name": "#foreignrelations",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#geopolitics",
        "display_name": "#geopolitics",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#globalsecurity",
        "display_name": "#globalsecurity",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#homelandsecurity",
        "display_name": "#homelandsecurity",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#indo_pacific",
        "display_name": "#indo_pacific",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#indopacom",
        "display_name": "#indopacom",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#intelligence",
        "display_name": "#intelligence",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#israel",
        "display_name": "#israel",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#marines",
        "display_name": "#marines",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#maritimedefense",
        "display_name": "#maritimedefense",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#middleeast",
        "display_name": "#middleeast",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#military",
        "display_name": "#military",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#militarytech",
        "display_name": "#militarytech",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#missiledefense",
        "display_name": "#missiledefense",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#nationalsecurity",
        "display_name": "#nationalsecurity",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#nato",
        "display_name": "#nato",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#navy",
        "display_name": "#navy",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#osint",
        "display_name": "#osint",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#pentagon",
        "display_name": "#pentagon",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#russia",
        "display_name": "#russia",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#sanctions",
        "display_name": "#sanctions",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#secdef",
        "display_name": "#secdef",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#socom",
        "display_name": "#socom",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#spaceforce",
        "display_name": "#spaceforce",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#spaceops",
        "display_name": "#spaceops",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#specops",
        "display_name": "#specops",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#stealthjet",
        "display_name": "#stealthjet",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#stratcom",
        "display_name": "#stratcom",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#submarines",
        "display_name": "#submarines",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#tactical",
        "display_name": "#tactical",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#taiwan",
        "display_name": "#taiwan",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#ukraine",
        "display_name": "#ukraine",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#veterans",
        "display_name": "#veterans",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "truthsocial",
        "handle": "#warfare",
        "display_name": "#warfare",
        "category": "Truth Social",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@_24_kg",
        "display_name": "_24_kg",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@adgpi",
        "display_name": "@adgpi",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AEI",
        "display_name": "@AEI",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AFP",
        "display_name": "@AFP",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AirForceTimes",
        "display_name": "@AirForceTimes",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AlJazeera",
        "display_name": "@AlJazeera",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@alpha_defense",
        "display_name": "@alpha_defense",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@anshu217",
        "display_name": "anshu217",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AP",
        "display_name": "@AP",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ArmyTimes",
        "display_name": "@ArmyTimes",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AtlanticCouncil",
        "display_name": "@AtlanticCouncil",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@AviationWeek",
        "display_name": "@AviationWeek",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Bangladesh_BN",
        "display_name": "Bangladesh_BN",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BBCWorld",
        "display_name": "BBCWorld",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@bdbnp78",
        "display_name": "bdbnp78",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BJI_Official",
        "display_name": "BJI_Official",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@bnaenglish",
        "display_name": "bnaenglish",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BreakingDefense",
        "display_name": "@BreakingDefense",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@brief_pk",
        "display_name": "brief_pk",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@BritishArmy",
        "display_name": "@BritishArmy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CalibreObscura",
        "display_name": "@CalibreObscura",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CarnegieEndow",
        "display_name": "@CarnegieEndow",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CFR_China",
        "display_name": "CFR_China",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChathamHouse",
        "display_name": "@ChathamHouse",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaEmbassy_MV",
        "display_name": "ChinaEmbassy_MV",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaEmbSL",
        "display_name": "ChinaEmbSL",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaFactCheck",
        "display_name": "@ChinaFactCheck",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CHINAPLAS",
        "display_name": "CHINAPLAS",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@ChinaPowerCSIS",
        "display_name": "@ChinaPowerCSIS",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@cnasdc",
        "display_name": "@cnasdc",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CNNBreaking",
        "display_name": "@CNNBreaking",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@COUPSURE",
        "display_name": "@COUPSURE",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CSIS",
        "display_name": "@CSIS",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@CSISChina",
        "display_name": "@CSISChina",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DefenseNews",
        "display_name": "@DefenseNews",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DefenseOne",
        "display_name": "@DefenseOne",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DefsecaBD",
        "display_name": "DefsecaBD",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@detresfa_",
        "display_name": "detresfa_",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@DODinIndoPac",
        "display_name": "@DODinIndoPac",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Geopolistics",
        "display_name": "@Geopolistics",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@GovSriLanka",
        "display_name": "GovSriLanka",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HMVijithaHerath",
        "display_name": "HMVijithaHerath",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HudsonInstitute",
        "display_name": "@HudsonInstitute",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@HurriyatPa",
        "display_name": "HurriyatPa",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IAF_MCC",
        "display_name": "@IAF_MCC",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@idrworg",
        "display_name": "@idrworg",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IISS_org",
        "display_name": "@IISS_org",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IndianNavy",
        "display_name": "@IndianNavy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IndoPac_Info",
        "display_name": "@IndoPac_Info",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@IsraelRadar_com",
        "display_name": "@IsraelRadar_com",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@JanesINTEL",
        "display_name": "@JanesINTEL",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@UKDefJournal",
        "display_name": "@UKDefJournal",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@KofmanMichael",
        "display_name": "@KofmanMichael",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Kyangs_Thang",
        "display_name": "Kyangs_Thang",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@KyivIndependent",
        "display_name": "@KyivIndependent",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Liveuamap",
        "display_name": "@Liveuamap",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MaithripalaS",
        "display_name": "MaithripalaS",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MarineTimes",
        "display_name": "@MarineTimes",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MdNAlam43921990",
        "display_name": "MdNAlam43921990",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MFA_China",
        "display_name": "MFA_China",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@MoFAmv",
        "display_name": "MoFAmv",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@NavalInstitute",
        "display_name": "@NavalInstitute",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@NavyTimes",
        "display_name": "@NavyTimes",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@neilthomas123",
        "display_name": "neilthomas123",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Nrg8000",
        "display_name": "@Nrg8000",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@nuwangzi",
        "display_name": "nuwangzi",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@OfficialCLAWSIN",
        "display_name": "OfficialCLAWSIN",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Osinttechnical",
        "display_name": "@Osinttechnical",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@osmastro",
        "display_name": "osmastro",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@pakafghanmatter",
        "display_name": "pakafghanmatter",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PakinSriLanka",
        "display_name": "PakinSriLanka",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PakistanNavy",
        "display_name": "PakistanNavy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PentagonPresSec",
        "display_name": "@PentagonPresSec",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@PMDNewsGov",
        "display_name": "PMDNewsGov",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RajapaksaNamal",
        "display_name": "RajapaksaNamal",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@rajfortyseven",
        "display_name": "rajfortyseven",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RALee85",
        "display_name": "@RALee85",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RANDCorporation",
        "display_name": "RANDCorporation",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Rational314159",
        "display_name": "Rational314159",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Reuters",
        "display_name": "@Reuters",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RobLeeOSINT",
        "display_name": "@RobLeeOSINT",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RoyalAirForce",
        "display_name": "@RoyalAirForce",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RoyalNavy",
        "display_name": "@RoyalNavy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@RUSI_org",
        "display_name": "@RUSI_org",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@USAirForce",
        "display_name": "@USAirForce",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@USArmy",
        "display_name": "@USArmy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@scramble_nl",
        "display_name": "scramble_nl",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SIPRIorg",
        "display_name": "@SIPRIorg",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@USMC",
        "display_name": "@USMC",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@USNavy",
        "display_name": "@USNavy",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@SpaceForceDoD",
        "display_name": "@SpaceForceDoD",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@spglobal",
        "display_name": "@spglobal",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Sri_Lanka_Army",
        "display_name": "Sri_Lanka_Army",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@Strategic_Front",
        "display_name": "@Strategic_Front",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@strategic_h",
        "display_name": "@strategic_h",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@sugar_wsnbn",
        "display_name": "sugar_wsnbn",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@TargetedInt",
        "display_name": "TargetedInt",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@TheStudyofWar",
        "display_name": "TheStudyofWar",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@War_analyst_",
        "display_name": "War_analyst_",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "twitter",
        "handle": "@WarMapper",
        "display_name": "@WarMapper",
        "category": "Twitter",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "U.S. Naval Institute",
        "display_name": "U.S. Naval Institute",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Al Jazeera English",
        "display_name": "Al Jazeera English",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "ANI News",
        "display_name": "ANI News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Center for Strategic & International Studies",
        "display_name": "Center for Strategic & International Studies",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CGTN",
        "display_name": "CGTN",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CNA",
        "display_name": "CNA",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CNBC-TV18",
        "display_name": "CNBC-TV18",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "CNN-News18",
        "display_name": "CNN-News18",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "DVB TV News",
        "display_name": "DVB TV News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Firstpost",
        "display_name": "Firstpost",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Hindustan Times",
        "display_name": "Hindustan Times",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Inconnect News",
        "display_name": "Inconnect News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India News",
        "display_name": "India News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India Today",
        "display_name": "India Today",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "India Today Global",
        "display_name": "India Today Global",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Indian Defence Updates",
        "display_name": "Indian Defence Updates",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Myanmar Now News",
        "display_name": "Myanmar Now News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "NASASpaceflight",
        "display_name": "NASASpaceflight",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "NDTV",
        "display_name": "NDTV",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "NewsX Live",
        "display_name": "NewsX Live",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Pakistan Navy",
        "display_name": "Pakistan Navy",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Republic World",
        "display_name": "Republic World",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Revolut Burma News Channel (Official)",
        "display_name": "Revolut Burma News Channel (Official)",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "The Irrawaddy News",
        "display_name": "The Irrawaddy News",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Times Now",
        "display_name": "Times Now",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Times Of India",
        "display_name": "Times Of India",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "VideoFromSpace",
        "display_name": "VideoFromSpace",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "WION",
        "display_name": "WION",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "zaobaosg",
        "display_name": "zaobaosg",
        "category": "Youtube",
        "is_custom": False
    },
    {
        "platform": "youtube",
        "handle": "Zee News",
        "display_name": "Zee News",
        "category": "Youtube",
        "is_custom": False
    },
]

def _get_db_conn():
    config = dict(DB_CONFIG)
    config["connect_timeout"] = 1
    return psycopg2.connect(**config)

def _load_json():
    with _file_lock:
        if not os.path.exists(_HANDLES_FILE):
            _save_json(DEFAULT_SEED_HANDLES)
            return list(DEFAULT_SEED_HANDLES)
        try:
            with open(_HANDLES_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return list(DEFAULT_SEED_HANDLES)

def _save_json(data):
    with _file_lock:
        with open(_HANDLES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

def _init_schema():
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(_SCHEMA_SQL)
        for h in DEFAULT_SEED_HANDLES:
            cur.execute(
                """
                INSERT INTO sm_platform_handles (platform, handle, display_name, category, is_custom)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (platform, handle) DO NOTHING;
                """,
                (h["platform"], h["handle"], h["display_name"], h["category"], h["is_custom"])
            )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("Postgres handle schema init failed/skipped, using local fallback: %s", e)

# Run schema init in background thread to avoid blocking module import
threading.Thread(target=_init_schema, daemon=True).start()

def list_handles(platform: str = None) -> list[dict]:
    """List all handles directly from sm_platform_handles in DB, with fallback if DB is unreachable."""
    items = []
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, platform, handle, display_name, category, is_custom FROM sm_platform_handles ORDER BY platform ASC, lower(handle) ASC;")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        items = [dict(zip(cols, r)) for r in rows]
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning("DB list_handles failed, using local JSON fallback: %s", e)
        items = _load_json()

    if not items:
        items = [dict(s) for s in DEFAULT_SEED_HANDLES]

    # Always sort alphabetically: platform first, then handle (case-insensitive)
    def _sort_key(h):
        handle_clean = h.get("handle", "").lstrip("@").lstrip("u/").lstrip("/")
        return (h.get("platform", "").lower(), handle_clean.lower())

    items = sorted(items, key=_sort_key)

    if platform and platform.lower() != "all":
        return [h for h in items if h.get("platform", "").lower() == platform.lower()]
    return items

def add_custom_handle(platform: str, handle: str, display_name: str = None, category: str = "Custom") -> dict:
    """Add a custom handle to the library."""
    handle = handle.strip()
    if not handle.startswith("@") and not handle.startswith("r/"):
        handle = "@" + handle

    display = display_name or handle

    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sm_platform_handles (platform, handle, display_name, category, is_custom)
            VALUES (%s, %s, %s, %s, true)
            ON CONFLICT (platform, handle) DO UPDATE SET display_name = EXCLUDED.display_name
            RETURNING id, platform, handle, display_name, category, is_custom;
            """,
            (platform.lower(), handle, display, category)
        )
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        item = dict(zip(cols, row))
        conn.commit()
        cur.close()
        conn.close()
        return item
    except Exception as e:
        logger.warning("DB add_custom_handle failed, saving to local JSON: %s", e)
        items = _load_json()
        existing = next((i for i in items if i["platform"].lower() == platform.lower() and i["handle"].lower() == handle.lower()), None)
        if existing:
            return existing
        new_item = {
            "id": len(items) + 1,
            "platform": platform.lower(),
            "handle": handle,
            "display_name": display,
            "category": category,
            "is_custom": True
        }
        items.append(new_item)
        _save_json(items)
        return new_item

def delete_handle(handle_id: int) -> bool:
    """Delete a handle by ID."""
    try:
        conn = _get_db_conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM sm_platform_handles WHERE id = %s;", (handle_id,))
        affected = cur.rowcount > 0
        conn.commit()
        cur.close()
        conn.close()
        return affected
    except Exception as e:
        logger.warning("DB delete_handle failed, removing from local JSON: %s", e)
        items = _load_json()
        filtered = [i for i in items if i.get("id") != handle_id]
        if len(filtered) < len(items):
            _save_json(filtered)
            return True
        return False
