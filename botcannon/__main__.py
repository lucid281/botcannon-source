#!/usr/bin/env -S python3
import fire
from pathlib import Path
import inspect
import importlib

from walrus import Database
from .__init__ import MainCannon


class BotcannonCli:
    """
    Welcome to botcannon!
    """
    def __init__(self, socket_path):
        walrus = Database(unix_socket_path=str(socket_path.resolve()))
        self.su = MainCannon('CANNONDB', walrus)

    def bot(self, service):
        """Get ready-to-go plugin as defined by your service."""
        plugin, params = self.su.render.get_plugin(**self.su.render.dict(service, hardfail=True))
        return plugin(**params) if params else plugin

    def bots(self):
        """All .py bot files in ./bot/*, will not inject params from botcannon config"""
        p = {}
        for plugin in self.su.render.plugins:
            p[plugin] = self.su.render.get_entry(self.su.render.plugins[plugin], {}, checks=False)[0]
        return p

    def runner(self, service):
        """Get ready-to-go runner as defined by your service."""
        runner, params = self.su.render.get_runner(**self.su.render.dict(service, hardfail=True))
        return runner(**params) if params else runner

    def run_runner(self, service):
        runner, params = self.su.render.get_runner(**self.su.render.dict(service, hardfail=True))
        running = runner(**params) if params else runner()
        ledger = self.su.ledger(service)
        for message in running.read():
            print('{' + '\n'.join(f'{i}:{str(message[i])}' for i in message) + '}\n', sep='')
            ledger.write('inbox', message)

    def worker(self, service):
        return self.su.ledger(service).worker

    def ls(self):
        _services = [i for i in self.su.service.all()]
        if not _services:
            return f'No services defined. Run "services define".'

        b = [''.join(f'{i:<20}' for i in ['name', 'plugin', 'entry_config', 'runner', 'run_config', 'autostart'])]
        [b.append(f'{s.name:<19} {s.plugin:<19} {s.entry_config:<19} {s.runner:<19}'
                  f' {s.run_config:<19} {s.autostart}') for s in _services]

        return '\n'.join([str(i) for i in b])

    def help(self):
        return inspect.getdoc(self)


if __name__ == '__main__':
    docker_path = Path('/run/redis/redis-server.sock')  # or system
    relative_path = Path('.') / 'sockets/redis-server.sock'

    if docker_path.exists():
        use_path = docker_path
    elif relative_path.exists():
        use_path = relative_path
    else:
        print(f'No redis socket in {docker_path} OR {relative_path}.')
        exit(1)

    try:
        fire.Fire(BotcannonCli(use_path), name='.')
    except KeyboardInterrupt:
        pass
