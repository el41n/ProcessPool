"""Worker-process class for process pool."""
import logging
import multiprocessing
import signal
import queue

LOGGER = logging.getLogger()


class Worker(multiprocessing.Process):
    """Infinite execution class-process.
    Takes callable and run it until timeout or execution finish."""
    n = 0

    def __init__(self, task_queue, stop_event, response_dict, tasks_complete):
        super().__init__()
        # queue with tasks(callable)
        self.queue = task_queue
        # Event() for stopping process
        self.stop_event = stop_event
        # shared dict for sending function return or exception
        self.response_dict = response_dict
        # signal for completing
        self.tasks_complete = tasks_complete

        self.num = Worker.n
        Worker.n += 1

        self.start()

    def run(self):
        while not self.stop_event.is_set():
            # setting try block for timer exception
            try:
                LOGGER.debug('Process %d wait for callable', self.num)
                # getting task from queue
                try:
                    task = self.queue.get(timeout=2)
                except queue.Empty:
                    LOGGER.debug('Process %d has nothing to execute', self.num)
                    continue

                LOGGER.debug('Process %d acquired task', self.num)
                # setting timer if it necessary
                if task.timeout:
                    LOGGER.debug('Process %d sets timer', self.num)
                    signal.alarm(task.timeout)
                    signal.signal(signal.SIGALRM, self._stop_timer)
                # starting executing task and catching all exceptions from task
                try:
                    ret = task.callable(*task.args, **task.kwargs)
                except Exception as err:
                    ret = err
                finally:
                    raise StopExecution

            except StopExecution:
                LOGGER.debug('Process %d complete execution', self.num)
                self.tasks_complete[task.id].set()
                self.response_dict[task.id] = ret
                continue

        LOGGER.debug('Process %d stopped', self.num)

    @staticmethod
    def _stop_timer(signum, frame):
        """Raises StopExecution exception if signal handled"""
        raise StopExecution("Task was exceeded execution time")


class StopExecution(Exception):
    """Exception for stopping callable in worker processes"""
    def __init__(self, message=None):
        self.message = message

    def __repr__(self):
        return self.message
