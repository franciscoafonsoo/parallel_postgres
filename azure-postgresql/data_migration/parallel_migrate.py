#!/usr/bin/env python3
import argparse
import os


class Config:
    local: str
    dev: str
    tst: str
    wus: str
    fields: str
    devices: str


def parse_args():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-s",
        "--source-env",
        type=str,
        required=True,
        help="db source connection string",
    )

    parser.add_argument(
        "-st",
        "--source-table",
        type=str,
        required=True,
        help="db source table",
    )

    parser.add_argument(
        "-d",
        "--dest-env",
        type=str,
        required=True,
        help="db destination connection string",
    )

    parser.add_argument(
        "-dt",
        "--dest-table",
        type=str,
        required=True,
        help="db destination table",
    )

    # macbook 15inch 2018 has a i9-8950HK
    # with 12 threads
    parser.add_argument(
        "-t", "--threads", type=int, default=12, help="total number of threads"
    )

    parser.add_argument(
        "--days", type=int, default=2, help="number of days to copy"
    )

    parser.add_argument("-c", "--count", type=int, help="number of lines in the table")

    return parser.parse_args()


def parallel_migrate(args):
    interval = int(args.count / args.threads)
    start = 0
    end = start + interval

    source_url = getattr(Config, args.source_env)
    dest_url = getattr(Config, args.dest_env)

    # to properly handle backslash
    copy = r"\COPY"

    copy_cmd = f'{copy} (SELECT {Config.fields} FROM {args.source_table}'
    filter_query = f"device_id in {Config.devices} AND signal_timestamp > current_date - interval '{args.days}' day"

    for i in range(0, args.threads):

        if i != args.threads - 1:
            select_query = f"{copy_cmd} WHERE id>={start} AND id<{end} AND {filter_query}) TO STDOUT"

            read_query = f'psql "{source_url}" -c "{select_query}"'

            write_query = f'psql "{dest_url}" -c "{copy} {args.dest_table} ({Config.fields}) FROM STDIN"'

            os.system(read_query + "|" + write_query + " &")
            # print(read_query + "|" + write_query + " &")

        else:
            select_query = f"{copy_cmd} WHERE id>={start} AND {filter_query}) TO STDOUT"

            read_query = f'psql "{source_url}" -c "{select_query}"'

            write_query = f'psql "{dest_url}" -c "{copy} {args.dest_table} ({Config.fields}) FROM STDIN"'

            os.system(read_query + "|" + write_query)
            # print(read_query + "|" + write_query)

        start = end
        end = start + interval


def main():
    args = parse_args()
    parallel_migrate(args)


if __name__ == "__main__":
    main()
