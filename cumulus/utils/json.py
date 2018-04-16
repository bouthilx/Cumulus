from collections import OrderedDict
import types


DOT_KEY = ":"


def flatten(d, parents=tuple(), ignore_keys=tuple()):
    flattened_d = {}
    for k, v in d.iteritems():
        if k in ignore_keys:
            continue
        path = parents + (k, )
        if not isinstance(v, dict):
            flattened_d[".".join(path)] = v
        else:
            flattened_d.update(flatten(v, path, ignore_keys))

    return flattened_d


def expand(d):
    if not isinstance(d, dict):
        return d

    expanded_d = type(d)()
    for key, value in d.iteritems():
        path = key.split(".")
        parent = path[0]

        if len(path) == 1:
            expanded_d[parent] = value
            continue

        child = ".".join(path[1:])
        if parent not in expanded_d:
            expanded_d[parent] = type(d)()

        expanded_d[parent][child] = value

    return dict((k, expand(v)) for k, v in expanded_d.iteritems())


def get_subkeys(subkeys, d):
    if not isinstance(subkeys, (list, tuple, types.GeneratorType)):
        subkeys = [subkeys]

    new_d = dict(_id=d["_id"])
    new_d.update(dict(get_subkey(subkey, d) for subkey in subkeys))

    return new_d


def get_subkey(subkey, d):
    keys = subkey.split(".")
    for key in keys:
        if key not in d:
            return key, None
        d = d[key]

    return key, d


def convert_json_property_name_to_float(property_name):
    return float(property_name.replace(DOT_KEY, "."))


def convert_float_to_json_property_name(floating_number):
    return str(floating_number).replace(".", DOT_KEY)


def turn_to(d, new_type):
    if not isinstance(d, dict) and not isinstance(d, (list, tuple)):
        return d

    if isinstance(d, dict):
        if not type(d) is new_type:
            d = new_type(sorted(d.items()))

        for key, item in list(d.items()):
            d[key] = turn_to(item, new_type)

    elif isinstance(d, (list, tuple)):
        d = list(d)
        for i in range(len(d)):
            d[i] = turn_to(d[i], new_type)

    return d


def _is_subsame(a, b):
    return (isinstance(a, (list, tuple)) and
            all(type(a_i) is type(b) for a_i in a))


def is_subsame(a, b):
    return _is_subsame(a, b) or _is_subsame(b, a)


def assert_same_type(a, b, a_is_subset):
    is_same = type(a) is type(b)
    if not a_is_subset:
        assert is_same, ("%s (%s) is not %s (%s)" %
                         (type(a), str(a), type(b), str(b)))

    if a_is_subset:
        assert_string = ("%s (%s) is not %s (%s)" %
                         (type(a), str(a), type(b), str(b)))
        assert is_same or is_subsame(a, b), assert_string


def update_subdict(a, b, a_is_subset=True):
    """
    Notes
    -----

    a_is_subset: bool
        If True, asserts that if key in a, then key in b, otherwise invalid but
        if key in b, does not mean key in a
    """
    assert_same_type(a, b, a_is_subset)

    if isinstance(a, dict):
        iterator = a.iteritems()
        restore_type = dict
    elif isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        iterator = enumerate(a)
        restore_type = type(b)
        b = list(b)
    else:
        return a

    for key, kwargs in iterator:
        if not a_is_subset and isinstance(a, dict) and key not in b:
            b[key] = a[key]
        else:
            b[key] = update_subdict(a[key], b[key], a_is_subset)

    return restore_type(b)
