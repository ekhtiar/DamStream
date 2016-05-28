import schedule
import time

def joba():
    print("I'm working...")

def jobb():
    print("I'm not working...")

schedule.every(1).minutes.do(joba)
schedule.every(1).minute.do(jobb)

while True:
    schedule.run_pending()
    time.sleep(1)