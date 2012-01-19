"""
    @author: Jean-Lou Dupont
"""

import logging

FORMAT='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
formatter = logging.Formatter(FORMAT)

logging.basicConfig(level=logging.INFO, format=FORMAT)

#logger=logging.getLogger()
#console = logging.StreamHandler(sys.stdout)
#console.setLevel(logging.INFO)
#console.setFormatter(formatter)
#logger.addHandler(console)