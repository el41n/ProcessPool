'''Base classes for pool implementations.'''
import abc
from collections import Callable, namedtuple


Task = namedtuple('Task', 'callable args kwargs id')


class PoolBase(abc.ABC):
    '''Base class for pool implementations.'''

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.shutdown()

    @abc.abstractmethod
    def add_task(self, task_callable: Callable, *args, timeout=None, **kwargs) -> int:
        '''Registers new task to be executed by one of the pool workers. Return task id.'''

    @abc.abstractmethod
    def is_task_done(self, task_id: int) -> bool:
        '''Returns False if the task with given id is not done and True if it is done and it's result hasn't been
        picked up yet. Raises ValueError if task with given id doesn't exist or if it's result had already been
        picked up.
        '''

    @abc.abstractmethod
    def get_task_result(self, task_id: int):
        '''Returns the result of the task with the given id. Blocks till the task will be done.
        Raises ValueError if task with given id doesn't exist or if it's result had already been picked up.
        '''

    @abc.abstractmethod
    def shutdown(self) -> None:
        '''Blocks till all registered tasks will be done and does graceful shutdown of all workers and
        resource cleanup. Pool can't be used after shutdown method was called. Is called automatically on
        Pool object removal.
        '''
