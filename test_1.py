from time import time, sleep

def me_sleep(n: int = 1) -> None:
    sleep(n)
    print(f"I've slept for {n} secs")

me_sleep(3)