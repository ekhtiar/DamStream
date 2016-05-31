import time
import datetime

ts = time.time()

st = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d_h%H_m%M_s%S")

print st
