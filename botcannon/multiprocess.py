import multiprocessing as mp
from botcannon import BotCannon
import signal
import datetime

def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


class ManageWorkers(mp.Process):
    def __init__(self, service_name, q, cooldown):
        self._service_name = service_name
        self.q = q
        self.cooldown = cooldown

        super().__init__()
        self.start()

    def run(self) -> None:
        print(f'Starting {mp.current_process().name} loop.')
        init_worker()

        ps = []
        h = 0
        l_time = datetime.datetime.now()
        for d in iter(self.q.get, None):
            if d == 'killjobs':
                print(f'[!] Poison pill, terminating {mp.current_process().name}.')
                [i.terminate() for i in ps]

            elif d == 'hot':
                h += 1
                ps.append(TaskWorker(self._service_name, 1000, 1))
                ps[-1].daemon = True
                ps[-1].start()

            elif d == 'idle':
                ps.append(TaskWorker(self._service_name, 30000, 1))
                ps[-1].daemon = True
                ps[-1].start()

            elif d == 'loop_done':
                active = []
                for i in ps:
                    if not i.is_alive():
                        i.join()
                    else:
                        active.append(i)
                if not active:
                    self.q.put('idle')

            if (datetime.datetime.now() - l_time).seconds > self.cooldown:
                l_time = datetime.datetime.now()
                alive = [i for i in ps if i.is_alive()]
                print(f'[W] {len(alive)}p up. {len(ps)}p/{self.cooldown}s, {h} hot.')
                ps = alive
                h = 0

        print(f'Exiting {mp.current_process().name}')


class TaskWorker(mp.Process):
    def __init__(self, service_name, wait_ms, msg_count, recall_shell=False):
        super().__init__()
        self.cannon = BotCannon()
        self.service = self.cannon.init(service_name)
        self.recall_shell = recall_shell

        self.block = wait_ms  # in ms, 0 = wait indefinitely for a message
        self.count = msg_count  # gather this many messages before returning, works best with large block

        self.tasks = []
        t = self.service.get_tasks()
        for task in t:
            cls, params = t[task]
            self.tasks.append([task, cls(**params)] if params else [task, cls()])

    def run(self) -> None:
        for message in self.service.ledger.channels["data"].read(count=self.count, block=self.block):
            results = {}
            for yml_tasks in self.tasks:
                if 'torch' in yml_tasks[0]:
                    # fire/torch has needs (the shell class)
                    if self.recall_shell:  # new everything
                        s = self.cannon.init(self.service.ledger.service_name)
                        s, p = s.get_shell()
                    else:
                        s, p = self.service.get_shell()
                    c = s(**p) if p else s()
                    r = yml_tasks[1].task(c, message, **results)
                    [results.setdefault(i, r[i]) for i in r]
                else:
                    r = yml_tasks[1].task(message, **results)
                    if r:
                        [results.setdefault(i, r[i]) for i in r]

            if results:
                self.service.ledger.write('taskback', {**message.data, **results})
                print(f'[M] {message.message_id} | {self.block} |'
                      f' IN:[{", ".join(i for i in message.data)}]'
                      f' OUT:[{", ".join(i for i in results)}]')
            self.service.ledger.channels['data'].ack(message.message_id)
