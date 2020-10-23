from __future__ import division

import six

from brainzutils import cache

NAMESPACE_METRICS = "metrics"


@cache.init_required
def increment(metric_name, amount=1):
    """Increment the counter of the current bucket by a set amount.

    Arguments:
        metric_name: the name of a metric
        amount: the amount to increase the counter by (default: 1)"""

    ret = cache.hincrby("counter", metric_name, amount, namespace=NAMESPACE_METRICS)

    return ret


@cache.init_required
def stats(metric_names):
    """Get all current counts for this metrics named by ``metric_names``."""

    counters = cache.hgetall("counter", namespace=NAMESPACE_METRICS)
    ret = {}
    str_key_counters = {six.ensure_text(k): int(v) for k, v in counters.items()}
    for metric in metric_names:
        if metric in str_key_counters:
            ret[metric] = str_key_counters[metric]
    return ret
