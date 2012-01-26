import os, sys, argparse
op=os.path

try:
    import jldaws
except:
    ### must be in dev mode then    
    this_dir=op.dirname(__file__)
    lib_path=op.abspath(op.join(this_dir, "..", ".."))
    sys.path.insert(0, lib_path)
    import jldaws

