"""
    Created on 2012-01-19
    @author: jldupont
"""
import os
import subprocess

def resolve_path(path):
    try:
        path=os.path.expandvars(path)
        path=os.path.expanduser(path)
        return ("ok", path)
    except:
        return ("error", None)



def safe_isfile(path):
    try:
        return os.path.isfile(path)
    except:
        return False
    
    
def simple_popen(path_script, env={}):
    new_env=os.environ.copy()
    new_env.update(env)
    return subprocess.Popen(path_script, env=new_env)
    