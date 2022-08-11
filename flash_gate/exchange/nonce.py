from time import time_ns, sleep


class Nonce:
    @staticmethod
    def get():
        sleep(0.002)
        nonce = time_ns()
        return nonce
