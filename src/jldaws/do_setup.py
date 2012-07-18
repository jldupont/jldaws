"""
    @author: Jean-Lou Dupont
"""

import sys,os

## force stdout + stderr flushing
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)

from tools_logging import setup_basic_logging

setup_basic_logging()

