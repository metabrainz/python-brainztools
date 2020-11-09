from __future__ import division

from functools import wraps

import six
from redis import ResponseError

from brainzutils import cache

NAMESPACE_METRICS = "metrics"
REDIS_MAX_INTEGER = 2**63-1

_metrics_site_name = None


def init(site):
    global _metrics_site_name
    _metrics_site_name = site


def metrics_init_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _metrics_site_name:
            raise RuntimeError("Metrics module needs to be initialized before use")
        return f(*args, **kwargs)
    return decorated


@cache.init_required
@metrics_init_required
def increment(metric_name, amount=1):
    """Increment the counter for a metric by a set amount. If incrementing the counter causes it to go over
    redis' internal counter limit of 2**63-1, the counter is reset to 0.
    The metric name ``tag`` is reserved and cannot be used.

    Arguments:
        metric_name: the name of a metric
        amount: the amount to increase the counter by, must be 0 or greater (default: 1)

    Raises:
        ValueError if amount is less than 0 or greater than 2**63-1
        ValueError if the reserved metric name ``tag`` is used
    """

    if amount < 0:
        raise ValueError("amount must be positive")
    if amount > REDIS_MAX_INTEGER:
        raise ValueError("amount is too large")
    if metric_name == "tag":
        raise ValueError("the name 'tag' is reserved")

    try:
        ret = cache.hincrby(_metrics_site_name, metric_name, amount, namespace=NAMESPACE_METRICS)
    except ResponseError as e:
        # If the current value is too large, redis will return this error message.
        # Reset to 0 and re-increment
        if e.args and "increment or decrement would overflow" in e.args[0]:
            cache.hset(_metrics_site_name, metric_name, 0, namespace=NAMESPACE_METRICS)
            ret = cache.hincrby(_metrics_site_name, metric_name, amount, namespace=NAMESPACE_METRICS)
        else:
            raise

    return ret


@cache.init_required
@metrics_init_required
def remove(metric_name):
    """Remove a metric.

    Arguments:
        metric_name: The metric to delete
    """

    return cache.hdel(_metrics_site_name, [metric_name], namespace=NAMESPACE_METRICS)


@cache.init_required
@metrics_init_required
def stats():
    """Get all current counts for metrics in the currently configured site."""

    counters = cache.hgetall(_metrics_site_name, namespace=NAMESPACE_METRICS)
    ret = {six.ensure_text(k): int(v) for k, v in counters.items()}
    ret['tag'] = _metrics_site_name
    return ret
