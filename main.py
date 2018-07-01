from typing import Any, Mapping, MutableMapping, Tuple
import datetime
import optparse
import re
import sys
import urllib.parse
import urllib.request

import pandas


def get_raw_stock(market: str,
                  code: int,
                  tick: datetime.timedelta = datetime.timedelta(days=1),
                  period: str = '1M') -> str:

    base_url = 'https://www.google.com/finance/getprices?f=d,o,h,l,c,v&'
    query = urllib.parse.urlencode({
        'x': market,
        'q': code,
        'i': tick.total_seconds,
        'p': period,
    })

    with urllib.request.urlopen(base_url + query) as res:
        return res.read().decode('utf-8')


def parse_raw_stock(data: str) -> Tuple[MutableMapping[str, Any],
                                        pandas.DataFrame]:

    lines = data.strip().splitlines()
    metadata: MutableMapping[str, Any] = {
        'market': urllib.parse.unquote(lines[0]),
    }

    for header_len, line in enumerate(lines[1:]):
        try:
            key, value = line.split('=')
        except:
            break

        if key == 'INTERVAL':
            metadata['tick'] = datetime.timedelta(seconds=int(value))
        elif key == 'COLUMNS':
            columns = value.strip().split(',')
        elif key == 'DATA_SESSIONS':
            metadata['sessions'] = {}
            for session in re.finditer(r'\[([A-Z0-9]+),([0-9]+),([0-9]+)\]',
                                       value):
                metadata['sessions'][session.group(1)] = {
                    'start': int(session.group(2)),
                    'end': int(session.group(3)),
                }
        elif key == 'TIMEZONE_OFFSET':
            metadata['timezone'] = datetime.timezone(
                datetime.timedelta(minutes=int(value)),
            )

    if 'sessions' in metadata:
        metadata['sessions'] = {k: {
            'start': datetime.time(hour=v['start'] // 60,
                                   minute=v['start'] % 60,
                                   tzinfo=metadata['timezone']),
            'end': datetime.time(hour=v['end'] // 60,
                                 minute=v['end'] % 60,
                                 tzinfo=metadata['timezone']),
        } for k, v in metadata['sessions'].items()}

    prices = []

    for line in lines[header_len + 1:]:
        rawtime, *data = line.split(',')

        if rawtime.startswith('a'):
            last_time = time = datetime.datetime.fromtimestamp(
                int(rawtime[1:]),
                tz=metadata['timezone'],
            )
        else:
            time = last_time + metadata['tick'] * int(rawtime)

        prices.append((time, *data))

    prices = pandas.DataFrame(prices, columns=columns).set_index('DATE')

    return metadata, prices


def get_stock(market: str,
              code: int,
              tick: datetime.timedelta = datetime.timedelta(days=1),
              period: str = '1M') -> Tuple[Mapping[str, Any],
                                           pandas.DataFrame]:

    return parse_raw_stock(get_raw_stock(market, code, tick, period))


def print_metadata(data: Mapping[str, Any], indent: str = '') -> None:
    for k, v in data.items():
        header = '{}{}: '.format(indent, k)
        if isinstance(v, dict):
            print(header)
            print_metadata(v, indent + '  ')
        elif isinstance(v, (list, tuple)):
            print(header + v[0])
            for x in v[1:]:
                print('{}{}'.format(' ' * len(header), x))
        else:
            print('{}{}'.format(header, v))


if __name__ == '__main__':
    parser = optparse.OptionParser(usage='$ %prog [options] market stock')
    parser.add_option('-t', '--tick',
                      type=int,
                      default=24*60*60,
                      help='Period of tick in seconds.')
    parser.add_option('-p', '--period',
                      type=str,
                      default='1M',
                      help='Period of data. like a 1Y, 2M, 3d, 6h or 30m')

    opts, args = parser.parse_args(sys.argv[1:])
    if len(args) != 2:
        parser.print_help()
        sys.exit(1)

    metadata, prices = (get_stock(
        market=args[0],
        code=args[1],
        tick=datetime.timedelta(seconds=opts.tick),
        period=opts.period,
    ))

    print_metadata(metadata)
    print()
    print(prices)
