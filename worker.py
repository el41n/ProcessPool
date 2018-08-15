import multiprocessing
import queue
import signal


class Worker(multiprocessing.Process):
    n = 0

    def __init__(self, task_queue, stop_event, response_dict, tasks_complete):
        super().__init__()
        # callable pipe
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
                print('Wait for callable', self.num)
                # getting task from queue
                try:
                    task = self.queue.get(timeout=2)
                except queue.Empty:
                    print('Nothing')
                    continue

                print('Acquired', self.num)
                # setting timer if it necessary
                if task.timeout:
                    print('Set timer')
                    signal.alarm(task.timer)
                    signal.signal(signal.SIGALRM, self._stop_timer)
                # starting executing task and catching all exceptions from task
                try:
                    ret = task.callable(*task.args, **task.kwargs)
                except Exception as err:
                    ret = err
                finally:
                    print('Complete', self.num)
                    self.response_dict[task.id] = ret
                    self.tasks_complete[task.id].set()

            except StopExecution as err:
                print('Complete', self.num)
                self.response_dict[task.id] = ret
                self.tasks_complete[task.id].set()
                continue

        print('Worker {} stopped'.format(self.num))

    @staticmethod
    def _stop_timer(signum, frame):
        """Raises StopExecution exception if signal handled"""
        raise StopExecution("Task was exceeded execution time")


class StopExecution(Exception):
    """Exception for stopping callable in worker processes"""
    def __init__(self, message):
        self.message = message

    def __repr__(self):
        return self.message

