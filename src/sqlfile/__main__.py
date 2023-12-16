import logging
import argparse
from sqlfile import Sq


log = logging.getLogger()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument('sql_path', help='sqlite db file')
    parser.add_argument('action', choices=['load', 'counts'])

    parser.add_argument('-i', dest='input_file', required=False, default=None)
    parser.add_argument('-t', dest='table', required=False, default=None)
    parser.add_argument('--no-header', default=False)
    parser.add_argument('--log-level', default='INFO')
    parser.add_argument('--replace-db', action='store_true',
                        help='replace sqlite db file')
    parser.add_argument('--append', action='store_true')

    args = parser.parse_args()
    logging.basicConfig(level=args.log_level)
    log.info(f"{args=}")

    if args.action == 'counts':
        with Sq(args.sql_path, replace=False) as sq:
            result = sq.counts()
            print(result)

    elif args.action == 'load':
        with Sq(args.sql_path, replace=args.replace_db) as sq:
            input_file = args.input_file
            if not args.table:
                raise ValueError("specify table -t")
            sq.read_csv(
                table=args.table,
                path=input_file,
                append=args.append,
                has_header=not args.no_header
            )
