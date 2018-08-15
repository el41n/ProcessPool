from collections import namedtuple
import multiprocessing
import time
import cProfile

from base import PoolBase, Task, Callable
from worker import Worker

PathosTask = namedtuple('PathosTask', 'callable args kwargs id timeout')

CPU_COUNT = multiprocessing.cpu_count()


class MyPathosPool(PoolBase):
    def __init__(self, cpu_amount=CPU_COUNT+1):
        self.process_amount = cpu_amount

        self.task_id = 0
        self.manager = multiprocessing.Manager()
        # shared dict with func returns
        self.tasks_response = self.manager.dict()

        self.tasks_complete = self.manager.dict()

        # list of all tasks
        self.task_list = []
        # queue of non executable tasks
        self.queue = multiprocessing.Queue()

        # event for checking dict
        self.proc_complete = multiprocessing.Event()
        # stopping all processes and pool
        self.stop_event = multiprocessing.Event()

        self.processes = [Worker(self.queue, self.stop_event, self.tasks_response, self.tasks_complete)
                          for _ in range(self.process_amount)]

    def add_task(self, task_callable: Callable, *args, timeout=None, **kwargs):
        task = PathosTask(task_callable, args, kwargs, self.task_id, timeout)
        self.tasks_complete[task.id] = self.manager.Event()
        self.queue.put(task)
        self.task_list.append(task.id)
        self.task_id += 1
        print('Added')
        return task.id

    def is_task_done(self, task_id: int):
        if task_id in self.task_list:
            if task_id in self.tasks_response.keys():
                return True
            else:
                return False
        else:
            raise ValueError('Task with id {} doesn\'t exists'.format(task_id))

    def get_task_result(self, task_id: int):
        while True:
            if self.is_task_done(task_id):
                response = self.tasks_response.pop(task_id)
                self.task_list.remove(task_id)
                if isinstance(response, Exception):
                    raise response
                return response
            else:
                delay = 2
                self.tasks_complete[tasks_id].wait(timeout=delay)
                self.tasks_complete.pop(tasks_id)

    def shutdown(self):
        self.stop_event.set()
        for process in self.processes:
            process.join()


def fun(*args, **kwargs):
    print('{}'.format(args, kwargs))
    time.sleep(2)
    return 123

def fun2(*args, **kwargs):
    print('{}'.format(args, kwargs))
    time.sleep(2000)
    return 222

def fun3(a, b, *, c):
    return a+b+c


if __name__ == '__main__':
    pr = cProfile.Profile()
    pr.enable()
    with MyPathosPool(2) as pool:
        tasks_id = []
        # tasks_id.append(pool.add_task(fun, 3, z=3))
        # #tasks_id.append(pool.add_task(fun2, 87, timer=2, z=3))
        # #print(pool.get_task_result(tasks_id[1]))
        # tasks_id.append(pool.add_task(fun2, 87, timer=2, z=3))
        # tasks_id.append(pool.add_task(fun, 22, z=4))
        tasks_id.append(pool.add_task(fun3, 3, 0, c=10))
        tasks_id.append(pool.add_task(fun, 22, z=4))
        tasks_id.append(pool.add_task(fun, 22, z=4))
        time.sleep(5)
        for i in tasks_id:
            try:
                print(pool.get_task_result(i))
            except Exception as err:
                print(err)
    print('Pool finished')
    pr.disable()
    pr.print_stats()

