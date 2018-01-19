"""Fizzbuzz Program"""

import threading
DEBUT = 1
FIN = 100

def fizzbuzz(nbr):
    """Print job"""
    if nbr % 3 == 0:
        if nbr % 5 == 0:
            print("FizzBuzz")
        else:
            print("Fizz")
    elif nbr % 5 == 0:
        print("Buzz")
    else:
        print(nbr)

# append threads
threads = [threading.Thread(target=fizzbuzz, args=(i,)) for i in range(DEBUT, FIN+1)]
# Start all threads
for t in threads:
    t.start()
# Ensure all of the threads have finished
for t in threads:
    t.join()
