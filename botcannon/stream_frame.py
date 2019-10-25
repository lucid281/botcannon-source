from datetime import datetime, timedelta

from pandas import DataFrame
from walrus import Message




class TimeFrames:
    def __init__(self, service):
        self.service = service

    def _get_range(self, end, **kwargs):
        kw = ["years", "months", "weeks", "days", "hours", "minutes", "seconds", "milliseconds"]
        if not kwargs:
            kwargs["hours"] = 1
        begin = end - timedelta(**{i: kwargs[i] for i in kwargs if i in kw})
        return self.service.ledger.channels["data"].range(start=begin, stop=end)

    def now(self, seconds):
        end = datetime.now()
        return self._get_range(end, seconds=seconds)

    def yesterday(self):
        yesterday = datetime.now() - timedelta(hours=24)
        return self._get_range(yesterday, hours=24)

    def hours(self, hours):
        return self._get_range(hours=hours, end=datetime.now())

    def days(self, days):
        return self._get_range(days=days, end=datetime.now())


class BlindDataFrames:
    def __init__(self, service, chat=False):
        self.tf = TimeFrames(service)
        self.chat = chat

    @staticmethod
    def index_messages(messages):
        frames = {}
        index = {}

        def func(msg):
            for stream_key in msg.data:
                if stream_key in frames:
                    frames[stream_key].append(msg.data[stream_key])
                else:
                    frames[stream_key] = [msg.data[stream_key]]
                if stream_key in index:
                    index[stream_key].append(msg.timestamp)
                else:
                    index[stream_key] = [msg.timestamp]

        [func(i) for i in messages]

        dfs = {}
        for key in index:
            f = DataFrame(frames, index=index[key])
            dfs[key] = f
        return dfs

    def today(self):
        return self.index_messages(self.tf.today())

    def yesterday(self):
        return self.index_messages(self.tf.yesterday())

    def hour(self, hours):
        return self.index_messages(self.tf.hours(hours))

    def now(self, seconds):
        return self.index_messages(self.tf.now(seconds))

    def days(self, days):
        return self.index_messages(self.tf.days(days))

    # def _filter_keys(self, messages: list, key):
    #     keys_in_msgs = set()
    #     [[keys_in_msgs.add(x) for x in i.data] for i in messages]
    #     return keys_in_msgs



    # def report(self, days, hours, minutes, period):
    #     """https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#dateoffset-objects"""
    #     end = datetime.now()
    #     begin = end - timedelta(days=days, hours=hours, minutes=minutes)
    #
    #     frames = {}
    #     index = {}
    #
    #     def update_frame(msg: Message):
    #         for key in msg.data:
    #             if key in frames:
    #                 frames[key].append(float(msg.data[key]))
    #                 index[key].append(msg.timestamp)
    #             else:
    #                 frames[key] = [float(msg.data[key])]
    #                 index[key] = [msg.timestamp]
    #
    #     [update_frame(i) for i in self.service.ledger.channels["data"].range(start=begin, stop=end)]
    #
    #     zero = DataFrame(frames, index=index['0'])
    #     final = zero.resample(period).mean()
    #
    #     print(
    #         f'{plot(final["0"])}\n'
    #         f'{begin:%x @ %X} ⇒ {end:%x @ %X}\n'
    #         f'{len(frames["0"])} points over {days}D {hours}H {minutes}min '
    #         f'resampled to {len(final)}, {period} intervals.'
    #     )
