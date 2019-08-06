_entrypoint_ = 'SlackService'

from dataclasses import dataclass
import time
import json
import re

from slackclient import SlackClient
from slackclient.client import SlackNotConnected
from walrus.streams import Message

from ..models import Runner
from ..models import ChatFireMsg


@dataclass
class SlackMsg(ChatFireMsg):
    to_thread: str = ''


class SlackService(Runner):
    msg = SlackMsg
    name = __name__

    def __init__(self, api_key):
        self.slack = SlackClient(api_key)
        self.status = self.slack.api_call("auth.test")
        if 'error' in self.status and 'invalid_auth' in self.status['error']:
            print(self.status)
            print('Unable to sign-in. Check API key.')
            exit(1)

    def read(self):
        if not self.slack.rtm_connect(with_team_state=False, reconnect=True):
            print("Connection to Slack failed....")
            exit(1)

        # while True:
        #     for event in self.slack.rtm_read():
        #         yield event
        if self.slack.server:
            while True:
                json_data = self.slack.server.websocket_safe_read()
                if json_data != "":
                    for d in json_data.split("\n"):
                        outgoing = json.loads(d)
                        outgoing['user_id'] = self.status['user_id']
                        self.slack.process_changes(outgoing)
                        yield outgoing

                time.sleep(0.05)  # websocket buffers for us this way
        else:
            raise SlackNotConnected

    @staticmethod
    def default_parse(msg: Message):
        """
        :param msg: event msg from Slack (data key of stream)
        :return: dict = {type:message, input_cmd:, channel:, thread_ts:, as_user:bool}
        """
        p = json.loads(msg.data['data'])

        if p["type"] == "message" \
                and not "subtype" in p:
            # and ('from_user' in msg and msg['from_user'] not in self.status['user_id'])\
            # msg = {}
            mention = re.search("^<@(|[WU].+?)>(.*)", p['text'])
            mentioned_user = mention.group(1) if mention else None
            input_cmd = mention.group(2).strip() if mention else None
            p['channel'] = p['channel']
            p['from_user'] = p['user']
            p['as_user'] = True
            p['service_name'] = msg.data['service_name']

            # DM
            if p['channel'].startswith('D') and p['user'] != p['user_id']:
                p['input_cmd'] = p['text']

            elif mention and mentioned_user == p['user_id']:
                p['input_cmd'] = input_cmd

            if 'thread_ts' in p:  # Reply to threads
                p['thread_ts'] = p['thread_ts']

            return p if 'input_cmd' in p else None

    def send(self, **kwargs):
        # simple send function for SlackMsg.write
        self.slack.api_call("chat.postMessage", **kwargs)
        # channel=channel, text=text, thread_ts=thread_ts, as_user=as_user)

