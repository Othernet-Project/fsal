# -*- coding: utf-8 -*-
from Queue import Empty as QueueEmpty
from multiprocessing import Process, Queue, Manager, cpu_count


class BaseWorker(Process):
    """
    A worker process, which does the heavy lifting. Tasks are retrieved through
    the shared task queue. A separate result queue is used to store the results
    of the tasks.
    """
    def __init__(self, task_queue, result_queue, workers_stopped, **kwargs):
        Process.__init__(self)
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.workers_stopped = workers_stopped
        # mark the worker initially that it's not finished
        self.workers_stopped[self.name] = False
        self.kwargs = kwargs

    def schedule(self, task):
        # block until there is space to put the task into the queue
        self.task_queue.put(task, block=True)

    def save_result(self, result):
        # put the result of a task into the result queue
        self.result_queue.put(result)

    def solve(self, task):
        raise NotImplementedError()

    def run(self):
        # a worker process won't terminate itself until all the workers report
        # that they have no tasks left
        while not all(self.workers_stopped.values()):
            try:
                # block until a new task arrives for 3 secs before reporting
                # inactivity.
                task = self.task_queue.get(block=True, timeout=3)
                self.workers_stopped[self.name] = False
            except QueueEmpty:
                # for more than 3 secs no new task has arrived, so this worker
                # will report to the others that it has no tasks left, but it
                # won't terminate itself
                self.workers_stopped[self.name] = True
            else:
                # task retrieved successfully, now solve it
                self.solve(task)


class PoolController(object):
    """
    In charge of creating and managing the worker processes.
    """
    def __init__(self, cls_worker, worker_count=cpu_count(),
                 worker_kwargs=None, task_queue_limit=0,
                 initial_tasks=None):
        self.cls_worker = cls_worker
        self.worker_count = worker_count
        self.worker_kwargs = worker_kwargs or {}
        self.task_queue_limit = task_queue_limit
        self.initial_tasks = initial_tasks or []

    def start(self):
        # limit the size of the task queue for memory efficiency
        task_queue = Queue(maxsize=self.task_queue_limit)
        result_queue = Queue()
        manager = Manager()
        workers_stopped = manager.dict()
        # prepare initial task
        for task in self.initial_tasks:
            task_queue.put(task)
        # create and start the worker processes
        workers = [self.cls_worker(task_queue,
                                   result_queue,
                                   workers_stopped,
                                   **self.worker_kwargs)
                   for _ in range(self.worker_count)]
        [w.start() for w in workers]
        # loop infinitely until the result queue is not empty and until any
        # of the workers is alive
        while True:
            try:
                # when the loop starts the worker processes need a little
                # time to provide some results so block until it arrives
                result = result_queue.get(block=True, timeout=1)
            except QueueEmpty:
                # no result in the queue, check if all the workers terminated
                # themselves. if that's the case, the mission is completed.
                if all(not w.is_alive() for w in workers):
                    break
            else:
                yield result
