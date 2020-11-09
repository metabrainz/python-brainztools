# pylint: disable=invalid-name
"""
This module serves as an interface for Redis.

Module needs to be initialized before use! See init() function.

It basically is a wrapper for redis package with additional
functionality and tweaks specific to serve our needs.

There's also support for namespacing, which simplifies management of different
versions of data saved in the cache. You can invalidate whole namespace using
invalidate_namespace() function. See its description for more info.

More information about Redis can be found at http://redis.io/.
"""
from functools import wraps
import shutil
import hashlib
import tempfile
import datetime
import os.path
import re
import redis
import msgpack
from brainzutils import locks


_r = None  # type: redis.StrictRedis
_glob_namespace = None  # type: bytes
_ns_versions_loc = None  # type: str


SHA1_LENGTH = 40
MAX_KEY_LENGTH = 250
NS_VERSIONS_LOC_DIR = "namespace_versions"
NS_REGEX = re.compile('[a-zA-Z0-9_-]+$')
CONTENT_ENCODING = "utf-8"
ENCODING_ASCII = "ascii"


def init(host="localhost", port=6379, db_number=0, namespace="", ns_versions_loc=None):
    """Initializes Redis client. Needs to be called before use.

    Namespace versions are stored in a local directory.

    Args:
        host (str): Redis server hostname.
        port (int): Redis port.
        db_number (int): Redis database number.
        namespace (str): Global namespace that will be prepended to all keys.
        ns_versions_loc (str): Path to directory where namespace versions will
            be stored. If not specified, creates a temporary directory that uses
            global namespace as a reference to make sure availability to multiple
            processes. See NS_VERSIONS_LOC_DIR value in this module and implementation.
    """
    global _r, _glob_namespace, _ns_versions_loc
    _r = redis.StrictRedis(
        host=host,
        port=port,
        db=db_number,
    )

    _glob_namespace = namespace + ":"
    _glob_namespace = _glob_namespace.encode(ENCODING_ASCII)
    if len(_glob_namespace) + SHA1_LENGTH > MAX_KEY_LENGTH:
        raise ValueError("Namespace is too long.")

    if ns_versions_loc:
        if not os.path.isdir(ns_versions_loc):
            raise ValueError("Can't find directory for storing namespace versions! "
                             "Please check `version_location` argument.")
    else:
        ns_versions_loc = os.path.join(tempfile.gettempdir(), NS_VERSIONS_LOC_DIR)
    _ns_versions_loc = os.path.join(ns_versions_loc, namespace)
    if not os.path.exists(_ns_versions_loc):
        os.makedirs(_ns_versions_loc)


def delete_ns_versions_dir():
    if os.path.isdir(_ns_versions_loc):
        shutil.rmtree(_ns_versions_loc)


def init_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not _r:
            raise RuntimeError("Cache module needs to be initialized before "
                               "use! See documentation for more info.")
        return f(*args, **kwargs)
    return decorated


# pylint: disable=redefined-builtin
@init_required
def set(key, val, time=0, namespace=None, encode=True):
    """Set a key to a given value.

    Args:
        key (str): Key of the item.
        val: Item's value.
        time (int): The time after which this value should expire, in seconds.
        namespace (str): Optional namespace in which key needs to be defined.
        encode: True if the value should be encoded with msgpack, False otherwise

    Returns:
        True if stored successfully.
    """
    # Note that both key and value are encoded before insertion.
    return set_many(
        mapping={key: val},
        time=time,
        namespace=namespace,
        encode=encode
    )


@init_required
def get(key, namespace=None, decode=True):
    """Retrieve an item.

    Args:
        key: Key of the item that needs to be retrieved.
        namespace: Optional namespace in which key was defined.
        decode (bool): True if value should be decoded with msgpack, False otherwise

    Returns:
        Stored value or None if it's not found.
    """
    # Note that key is encoded before retrieval request.
    return get_many([key], namespace, decode).get(key)


@init_required
def delete(key, namespace=None):
    """Delete an item.

    Args:
        key: Key of the item that needs to be deleted.
        namespace: Optional namespace in which key was defined.

    Returns:
          Number of keys that were deleted.
    """
    # Note that key is encoded before deletion request.
    return delete_many([key], namespace)


@init_required
def expire(key, time, namespace=None):
    """Set the expiration time for an item

    Args:
        key: Key of the item that needs to be deleted.
        time: the number of seconds after which the item should expire
        namespace: Optional namespace in which key was defined.

    Returns:
          True if the timeout was set, False otherwise
    """
    # Note that key is encoded before deletion request.
    return _r.pexpire(_prep_key(key, namespace), time * 1000)


@init_required
def expireat(key, timeat, namespace=None):
    """Set the absolute expiration time for an item

    Args:
        key: Key of the item that needs to be deleted.
        timeat: the number of seconds since the epoch when the item should expire
        namespace: Optional namespace in which key was defined.

    Returns:
          True if the timeout was set, False otherwise
    """
    # Note that key is encoded before deletion request.
    return _r.pexpireat(_prep_key(key, namespace), timeat * 1000)


@init_required
def set_many(mapping, time=None, namespace=None, encode=True):
    """Set multiple keys doing just one query.

    Args:
        mapping (dict): A dict of key/value pairs to set.
        time (int): The time after which this value should expire, in seconds.
        namespace (str): Namespace for the keys.
        encode: True if the values should be encoded with msgpack, False otherwise

    Returns:
        True on success.
    """
    # TODO: Fix return value
    result = _r.mset(_prep_dict(mapping, namespace, encode))
    if time:
        for key in _prep_keys_list(list(mapping.keys()), namespace):
            _r.pexpire(key, time * 1000)

    return result


@init_required
def get_many(keys, namespace=None, decode=True):
    """Retrieve multiple keys doing just one query.

    Args:
        keys (list): List of keys that need to be retrieved.
        namespace (str): Namespace for the keys.
        decode (bool): True if values should be decoded with msgpack, False otherwise

    Returns:
        A dictionary of key/value pairs that were available.
    """
    result = {}
    for i, value in enumerate(_r.mget(_prep_keys_list(keys, namespace))):
        result[keys[i]] = _decode_val(value) if decode else value
    return result


@init_required
def delete_many(keys, namespace=None):
    """Delete multiple keys.

    Returns:
        Number of keys that were deleted.
    """
    return _r.delete(*_prep_keys_list(keys, namespace))


@init_required
def increment(key, namespace=None):
    """ Increment the value for given key using the INCR command.

    Args:
        key: Key of the item that needs to be incremented
        namespace: Namespace for the key

    Returns:
        An integer equal to the value after increment
    """
    return _r.incr(_prep_keys_list([key], namespace)[0])


@init_required
def hincrby(name, key, amount, namespace=None):
    """Increment a hashes key by a given amount using HINCRBY

    Args:
        name: Name of the hash
        key: Key of the item in the hash to increment
        amount: the number to increment the key by
        namespace: Namespace for the name

    Returns:
        An integer equal to the value after increment
    """
    return _r.hincrby(_prep_keys_list([name], namespace)[0], key, amount)


@init_required
def hgetall(name, namespace=None):
    """Get all keys and values for a hash using HGETALL

    Args:
        name: Name of the hash
        namespace: Namespace for the name

    Returns:
        A dictionary of {key: value} items for all keys in the hash
    """
    return _r.hgetall(_prep_keys_list([name], namespace)[0])


@init_required
def hkeys(name, namespace=None):
    """Get all keys for a hash using HKEYS

    Args:
        name: Name of the hash
        namespace: Namespace for the name

    Returns:
        A list of [key] values for all keys in the hash
    """
    return _r.hkeys(_prep_keys_list([name], namespace)[0])


@init_required
def hset(name, key, value, namespace=None):
    """Delete the specified keys from a hash using HDEL.
    Note that the ``keys`` argument must be a list. This differs from the underlying redis
    library's version of this command, which takes varargs.

    Args:
        name: Name of the hash
        key: Key of the item in the hash to set
        value: value to set the item to
        namespace: Namespace for the name

    Returns:
        the number of keys deleted from the hash
    """
    return _r.hset(_prep_keys_list([name], namespace)[0], key, value)


@init_required
def hdel(name, keys, namespace=None):
    """Delete the specified keys from a hash using HDEL.
    Note that the ``keys`` argument must be a list. This differs from the underlying redis
    library's version of this command, which takes varargs.

    Args:
        name: Name of the hash
        keys: a list of the keys to delete from the has
        namespace: Namespace for the name

    Returns:
        the number of keys deleted from the hash
    """
    if not isinstance(keys, list):
        keys = [keys]
    return _r.hdel(_prep_keys_list([name], namespace)[0], *keys)


@init_required
def flush_all():
    _r.flushdb()


def gen_key(key, *attributes):
    """Helper function that generates a key with attached attributes.

    Args:
        key: Original key.
        attributes: Attributes that will be appended a key.

    Returns:
        Key that can be used with cache.
    """
    if not isinstance(key, str):
        key = str(key)
    key = key.encode(ENCODING_ASCII, errors='xmlcharrefreplace')

    for attr in attributes:
        if not isinstance(attr, str):
            attr = str(attr)
        key += b'_' + attr.encode(ENCODING_ASCII, errors='xmlcharrefreplace')

    key = key.replace(b' ', b'_')  # spaces are not allowed

    return key


def _prep_dict(dictionary, namespace=None, encode=True):
    """Wrapper for _prep_key and _encode_val functions that works with dictionaries."""
    if namespace:
        namespace_and_version = _append_namespace_version(namespace)
    else:
        namespace_and_version = None
    return {_prep_key(key, namespace_and_version): _encode_val(value) if encode else value
            for key, value in dictionary.items()}


def _prep_key(key, namespace_and_version=None):
    """Prepares a key for use with Redis."""
    # TODO(roman): Check if this is actually required for Redis.
    if namespace_and_version:
        key = "%s:%s" % (namespace_and_version, key)
    if not isinstance(key, bytes):
        key = key.encode(ENCODING_ASCII, errors='xmlcharrefreplace')
    key = hashlib.sha1(key).hexdigest().encode(ENCODING_ASCII)
    return _glob_namespace + key


def _prep_keys_list(l, namespace=None):
    """Wrapper for _prep_key function that works with lists.

    Returns:
        Prepared keys in the same order.
    """
    if namespace:
        namespace_and_version = _append_namespace_version(namespace)
    else:
        namespace_and_version = None
    return [_prep_key(k, namespace_and_version) for k in l]


@init_required
def _append_namespace_version(namespace):
    version = get_namespace_version(namespace)
    if version is None:  # namespace isn't initialized
        version = invalidate_namespace(namespace)
    return "%s:%s" % (namespace, version)


def _encode_val(value):
    if value is None:
        return value
    return msgpack.packb(value, use_bin_type=True, default=_msgpack_default)


def _decode_val(value):
    if value is None:
        return value
    return msgpack.unpackb(value, encoding=CONTENT_ENCODING, ext_hook=_msgpack_ext_hook)


############
# NAMESPACES
############


@init_required
def invalidate_namespace(namespace):
    """Invalidates specified namespace.

    Invalidation is done by incrementing version of the namespace

    Args:
        namespace: Namespace that needs to be invalidated.

    Returns:
        New version number.
    """
    validate_namespace(namespace)
    current_version = get_namespace_version(namespace)
    if current_version is None:  # namespace isn't initialized
        new_version = 1
    else:
        new_version = current_version + 1
    with locks.locked_open(_get_ns_version_file_path(namespace), mode=locks.M_WRITE) as f:
        f.write(str(new_version).encode(ENCODING_ASCII))
    return new_version


@init_required
def get_namespace_version(namespace):
    """Get version of a namespace.

    Args:
        namespace (str): Namespace itself.

    Returns:
        Namespace version as an integer if it exists, otherwise None.
    """
    validate_namespace(namespace)
    path = _get_ns_version_file_path(namespace)
    if not os.path.isfile(path):
        return None
    with locks.locked_open(path) as f:
        try:
            cont = f.read()
            return int(cont.decode(ENCODING_ASCII))
        except ValueError as e:
            raise RuntimeError("Failed to get version of namespace. Error: %s" % e)


def validate_namespace(namespace):
    """Checks that namespace value is supported."""
    if not NS_REGEX.match(namespace):
        raise ValueError("Invalid namespace. Must match regex /[a-zA-Z0-9_-]+$/.")


def _get_ns_version_file_path(namespace):
    return os.path.join(_ns_versions_loc, namespace)


######################
# CUSTOM SERIALIZATION
######################

TYPE_DATETIME_CODE = 1
DATETIME_FORMAT = "%Y%m%dT%H:%M:%S.%f"


def _msgpack_default(obj):
    if isinstance(obj, datetime.datetime):
        return msgpack.ExtType(TYPE_DATETIME_CODE, obj.strftime(DATETIME_FORMAT).encode(CONTENT_ENCODING))
    raise TypeError("Unknown type: %r" % (obj,))


def _msgpack_ext_hook(code, data):
    if code == TYPE_DATETIME_CODE:
        return datetime.datetime.strptime(data.decode(CONTENT_ENCODING), DATETIME_FORMAT)
    return msgpack.ExtType(code, data)
