import sys

PY35 = sys.version_info[0] > 2 and sys.version_info[1] > 4

<<<<<<< HEAD
def get_carbon(**kwargs):
    if PY35:
        from async_graphite import CarbonEmitter
    else:
        from graphite import CarbonEmitter
=======
get_carbon():
    if PY35:
        from async_graphite import CarbonEmitter
    else:
        from graphite import CarbonEmitter(**kwargs)
>>>>>>> 02aa7d1dcc222a221dcbed0b55e7859e3a3f8b3c
    
    return CarbonEmitter(**kwargs)
