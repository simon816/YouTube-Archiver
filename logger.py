from threading import Thread
from queue import Queue, Empty

class ThreadsafeLogger:

    def __init__(self):
        self.logqueue = Queue()
        self.thread = Thread(target=self.log_loop)

    def start(self):
        self.running = True
        self.thread.start()

    def stop(self):
        self.logqueue.join()
        self.running = False
        self.thread.join()

    def log(self, msg):
        self.logqueue.put(msg)

    def _do_log(self, msg):
        print(msg, flush=True)

    def drain_logqueue(self):
        while True:
            try:
                item = self.logqueue.get(False)
                self._do_log(item)
                self.logqueue.task_done()
            except Empty:
                break

    def log_loop(self):
        while self.running:
            try:
                item = self.logqueue.get(True, 1)
                self._do_log(item)
                self.logqueue.task_done()
            except Empty:
                pass
