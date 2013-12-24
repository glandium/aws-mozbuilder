import imp
import os
import sys


boto_base = os.path.join(os.path.dirname(__file__), 'boto')
fp, path, desc = imp.find_module('boto', [boto_base])
try:
    sys.modules[__name__] = imp.load_module('boto', fp, path, desc)
finally:
    if fp:
        fp.close()
