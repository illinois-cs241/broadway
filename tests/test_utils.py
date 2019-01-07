from src.utilities import PeriodicCallbackThread
import time


def test_periodic_callback_thread():
    def routine(num):
        num[0] += 1

    counter = [0]
    prev = 0
    thread = PeriodicCallbackThread(callback=routine, interval=0.01, args=[counter])
    thread.start()
    for _ in range(10):
        assert prev <= counter[0]
        prev += 1
        time.sleep(0.01)
    thread.stop()
