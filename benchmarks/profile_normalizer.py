"""Profile only the sequential CPU fingerprint hot path with cProfile."""

import argparse
import cProfile
import pstats

from datapipe.transforms.parallel import fingerprint_records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--records", type=int, default=100_000)
    parser.add_argument("--rounds", type=int, default=1000)
    args = parser.parse_args()
    records = [{"value": float(index % 997)} for index in range(args.records)]
    profiler = cProfile.Profile()
    profiler.enable()
    fingerprint_records(records, "value", "fingerprint", args.rounds)
    profiler.disable()
    pstats.Stats(profiler).strip_dirs().sort_stats("cumulative").print_stats(20)


if __name__ == "__main__":
    main()
