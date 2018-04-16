import numpy


def asarray(l):
    try:
        return numpy.asarray(l).squeeze()
    except TypeError:
        new_l = [asarray(e) for e in l]
        return numpy.asarray(new_l).squeeze()


def is_int(s):
    try:
        int(s)
    except ValueError:
        return False
    else:
        return True
