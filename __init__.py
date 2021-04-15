import sys

PY35 = sys.version_info[0] > 2 and sys.version_info[1] > 4

def get_carbon(**kwargs):
    
    if PY35:
        from async_graphite import CarbonEmitter
    else:
        from graphite import CarbonEmitter
   
    return CarbonEmitter(**kwargs)
