import argparse

from cmpcfg import ComponentConfig
from datetime import datetime, timezone
from ringest import Ringest


def cli_time(time_str):
    return datetime.strptime(time_str, "%Y%m%d-%H%M").\
        replace(tzinfo=timezone.utc)


class Component(object):

    def __call__(self,credentials):
        args = self.parse_args()
        r = Ringest(credentials,bucket_name=args.bucket)
        r.do_ringest(start_time=args.start_time, end_time=args.end_time,
                     token_count=args.token_count,
                     request_sleep=args.request_sleep)

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('--bucket', type=str, default='cortico-data')
        parser.add_argument('--start-time', type=cli_time)
        parser.add_argument('--end-time', type=cli_time)
        parser.add_argument('--token-count', type=int, default=2)
        parser.add_argument('--request-sleep', type=int, default=1)
        parser.add_argument('--reservation-hours', type=int, default=2)
        parser.add_argument('--reservation-minutes', type=int, default=0)
        return parser.parse_args()
