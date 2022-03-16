def get_backend_loads(backend='pickle'):
    if backend == 'pickle':
        import pickle
        return pickle.loads
    elif backend == 'quickle':
        import quickle
        return quickle.loads
    else:
        raise NotImplementedError('Currently only support `pickle` and `quickle`')


def get_backend_dumps(backend='pickle'):
    if backend == 'pickle':
        import pickle
        return pickle.dumps
    elif backend == 'quickle':
        import quickle
        return quickle.dumps
    else:
        raise NotImplementedError('Currently only support `pickle` and `quickle`')
