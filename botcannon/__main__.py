#!/usr/bin/env -S python3
import fire
from botcannon import BotCannon


if __name__ == '__main__':
    try:
        fire.Fire(BotCannon(), name='botcannon')
    except KeyboardInterrupt:
        pass
