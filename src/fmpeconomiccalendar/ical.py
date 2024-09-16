import hashlib
from datetime import datetime

import pandas as pd


def create_ics(self, df: pd.DataFrame, filename: str):
    ics = """BEGIN:VCALENDAR
PRODID:-//The Steamroom Calendar V1.1//EN
CALSCALE:GREGORIAN
VERSION:2.0\n"""

    for i, row in df.iterrows():
        row['date'] = pd.to_datetime(row['date'], format="%Y-%m-%d %H:%M:%S")
        dtstart = row['date'].strftime("%Y%m%dT%H%M%SZ")
        dtend = row['date'].strftime("%Y%m%dT%H%M%SZ")
        categories = row['category']
        summary = row['event']
        uid_str = f"{summary}-{dtstart}"
        uuid = hashlib.shake_128(uid_str.encode('utf-8')).hexdigest(16)
        trigger = '-PT2M'

        section = f"""BEGIN:VEVENT
CATEGORIES:{categories}
DTSTART:{dtstart}
DTSTAMP:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
CREATED:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
LAST-MODIFIED:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
SEQUENCE:0
STATUS:CONFIRMED
TRANSP:OPAQUE
SUMMARY:{summary}
UID:{uuid}
BEGIN:VALARM
ACTION:DISPLAY
TRIGGER:{trigger}
DESCRIPTION:Default Mozilla Description
END:VALARM
END:VEVENT
"""
        ics += section

    ics += ("END:VCALENDAR")

    try:
        with open(filename, 'w', encoding="utf-8") as f:
            f.writelines(ics)
        print('Success')
    except Exception as e:
        print(f"Error saving file.")
