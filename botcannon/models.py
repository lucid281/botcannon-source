from walrus import Model
from walrus import TextField
from walrus import DateField
from walrus import HashField
from walrus import SetField
from walrus import BooleanField

from datetime import datetime
from dataclasses import dataclass


class Users(Model):
    name = TextField(primary_key=True)
    enabled = BooleanField(default=True)
    dob = DateField(default=datetime.now())
    ids = HashField()  # {'slack': UID, 'rocket': UID}
    groups = SetField()  # [group1, group2]


class Groups(Model):
    name = TextField(primary_key=True)
    perms = SetField()  # [app1, app2]


class Services(Model):
    name = TextField(primary_key=True)  # bot-cf-rufus-chat
    plugin = TextField()  # bot-cf-rufus
    entry_config = TextField()
    runner = TextField()  # slack, discord, rocket, schedule
    run_config = TextField()
    autostart = BooleanField(default=False)


class Params(Model):
    name = TextField(primary_key=True)
    data = HashField()  # {'key': 'value, 'api_key': KEY}


# class StreamsConfig(Model):
#     name = TextField(primary_key=True)
#     consumer_groups = SetField()


class Runner:
    msg = ''
    name = ''

    def read(self):
        pass

    @staticmethod
    def default_parse(event):
        pass

    def send(self):
        pass


# Stream Base classes
@dataclass
class DefaultMsg:
    data: str


# Mutations for Subsystems
@dataclass
class ChatFireMsg(DefaultMsg):
    from_user: str
    to_user: str
    to_channel: str
    data: (list, dict, tuple, str, int, float, bool)
    command: str
    ts_id: str = ''


@dataclass
class Logging(DefaultMsg):
    trace: str
    ts_id: str
