import numpy as np
for attr, val in [('float',float),('int',int),('complex',complex),('bool',bool),('object',object),('str',str)]:
    if not hasattr(np, attr): setattr(np, attr, val)
if not hasattr(np, 'VisibleDeprecationWarning'): np.VisibleDeprecationWarning = DeprecationWarning
