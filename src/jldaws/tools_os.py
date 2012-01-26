"""
    Created on 2012-01-19
    @author: jldupont
"""
import os
import subprocess

def get_path_extension(path):
    """
    >>> get_path_extension("/some/path/config.yaml")
    ('/some/path/config', '.yaml')
    >>> get_path_extension(".bashrc")
    ('.bashrc', '')
    >>> get_path_extension("/some/path/file")
    ('/some/path/file', '')
    """
    return os.path.splitext(path)


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

    
    
def file_contents(path):
    """
    Simple "get file contents"
    """
    try:
        fh=open(path, "r")
        return ('ok', fh.read())
    except:
        return ("error", None)
    finally:
        try:
            fh.close()
        except:
            pass

if __name__=="__main__":
    import doctest
    doctest.testmod()
    