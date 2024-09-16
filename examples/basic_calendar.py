# Put fmpeconomiccalendar module in PYTHONPATH
import sys

sys.path.append('../src')

# import module
import src.fmpeconomiccalendar as fmpeconomiccalendar

print(fmpeconomiccalendar.__author__)

print(fmpeconomiccalendar.test)
