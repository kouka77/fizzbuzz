"""Fizzbuzz Program"""

import threading
# lock to synchronize the message printing
lock = threading.Lock()
DEBUT = 1
FIN = 100


def printmsg(msg):
    """Print job"""
    global lock
    lock.acquire()
    print(msg)
    lock.release()

def fizzbuzz(nbr):
    """Fizzbuzz job"""
    if nbr % 3 == 0:
        if nbr % 5 == 0:
            printmsg("FizzBuzz")
        else:
            printmsg("Fizz")
    elif nbr % 5 == 0:
        printmsg("Buzz")
    else:
        printmsg(nbr)

# Append threads
threads = [threading.Thread(target=fizzbuzz, args=(i,)) for i in range(DEBUT, FIN+1)]
# Start all threads
for t in threads:
    t.start()
# Ensure all of the threads have finished
for t in threads:
    t.join()
