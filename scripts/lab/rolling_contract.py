#!/usr/bin/env python3
"""Real-stack local handover contract, kept unchanged across the prototype.

All source writes must survive on the original native streams. The largest gap
between materialized source writes must be at most two seconds, and the Electric
adapter must recover. This is a local probe, not an ALB/gateway qualification.
"""
import argparse
from pathlib import Path

from rolling_prototype import RollingLab


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--engine', type=Path, required=True)
    p.add_argument('--ds', type=Path, default=Path('/Users/mh/.cargo/bin/durable-streams-server'))
    args = p.parse_args()
    args.engine, args.ds = args.engine.resolve(), args.ds.resolve()
    args.scenario, args.shapes, args.rate = 'rolling-contract', 8, 10
    lab = RollingLab(args)
    try:
        lab.rolling()
        gap = lab.result['max_native_progress_gap_ms']
        assert gap <= 2000, f'client progress gap {gap:.1f} ms exceeds the 2000 ms local handover contract'
        assert not lab.result['feed_errors'], lab.result['feed_errors']
        lab.result['outcome'] = 'pass'
    except BaseException as e:
        lab.result.update(outcome='fail', error=repr(e))
        raise
    finally:
        lab.cleanup()


if __name__ == '__main__':
    main()
