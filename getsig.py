import signal, time

def handler(signum, frame):
    print('Signal handler called with signal', signum)
    exit(signum)

for sig in signal.valid_signals():
    try:
        signal.signal(sig, handler)
    except:
        print("Failed to set signal: ", sig)

while(True):
    time.sleep(0.1)
