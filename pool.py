"""Process based pool."""
from collections import namedtuple
import logging
import multiprocessing
import threading

from base import PoolBase, Callable
from worker import Worker

logging.basicConfig(level=logging.DEBUG)

PathosTask = namedtuple('PathosTask', 'callable args kwargs id timeout')

CPU_COUNT = multiprocessing.cpu_count()


class MyPathosPool(PoolBase):
    """Pool class implements process poll. Takes callable and executes it
    in process."""
    def __init__(self, cpu_amount=CPU_COUNT+1):
        self.process_amount = cpu_amount
        # used for dispatching tasks, returned by add_task()
        self.task_id = 0

        self.manager = multiprocessing.Manager()
        # shared between processes dict for returning task result
        self.tasks_response = self.manager.dict()
        # shared between processes dict for waiting task result
        self.tasks_complete = self.manager.dict()

        # blocks add_task() method for one thread
        self.add_mutex = threading.Lock()
        # blocks get_task_result() method for one thread
        self.get_mutex = threading.Lock()

        # list of all tasks
        self.task_list = []
        # queue of yet non executable tasks
        self.queue = multiprocessing.Queue()

        # stopping all processes and pool
        self.stop_event = multiprocessing.Event()

        self.processes = [Worker(self.queue, self.stop_event, self.tasks_response, self.tasks_complete)
                          for _ in range(self.process_amount)]

    def add_task(self, task_callable: Callable, *args, timeout=None, **kwargs):
        logging.debug('Wait for adding task')
        with self.add_mutex:
            task = PathosTask(task_callable, args, kwargs, self.task_id, timeout)
            self.tasks_complete[task.id] = self.manager.Event()
            self.queue.put(task)
            self.task_list.append(task.id)
            self.task_id += 1
            logging.debug('Task %d added', task.id)
        return task.id

    def is_task_done(self, task_id: int):
        if task_id in self.task_list:
            return task_id in self.tasks_response.keys()
        raise ValueError('Task with id {} doesn\'t exists'.format(task_id))

    def get_task_result(self, task_id: int):
        logging.debug('Wait for getting result for task %d', task_id)
        with self.get_mutex:
            while True:
                if self.is_task_done(task_id):
                    response = self.tasks_response.pop(task_id)
                    self.task_list.remove(task_id)
                    break
                else:
                    logging.debug('Task %d not finished yet', task_id)
                    delay = 2
                    self.tasks_complete[task_id].wait(timeout=delay)
            self.tasks_complete.pop(task_id)
            logging.debug('Task %d finished', task_id)
            if isinstance(response, Exception):
                raise response
            return response

    def shutdown(self):
        logging.debug('Shutdown starts wait for processes')
        self.stop_event.set()
        for process in self.processes:
            process.join()
        logging.debug('Pool shut down complete')
