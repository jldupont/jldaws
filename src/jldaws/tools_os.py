"""
    Created on 2012-01-19
    @author: jldupont
"""
import os, errno
import subprocess

def touch(path):
    """
    >>> touch("/tmp/JustTouching")
    ('ok', '/tmp/JustTouching')
    >>> rm('/tmp/JustTouching')
    ('ok', '/tmp/JustTouching')
    """
    fhandle = file(path, 'a')
    try:
        os.utime(path, None)
        return ('ok', path)
    except OSError, exc:
        return ("error", (exc.errno, errno.errorcode[exc.errno]))
    finally:
        fhandle.close()        

def rm(path):
    """
    Silently (i.e. no exception thrown) removes a path if possible
    
    >>> rm("/tmp/NotAFile") ## no need to complain if there is no file
    ('ok', '/tmp/NotAFile')
    """
    try:
        os.remove(path)
        return ('ok', path)
    except OSError, exc:
        if exc.errno==errno.ENOENT:
            return ('ok', path)
        return ("error", (exc.errno, errno.errorcode[exc.errno]))

def can_write(path):
    """
    Checks if the current user (i.e. the script) can delete the given path
    Must check both user & group level permissions
    
    FOR THIS TEST, NEED TO CREATE A DIRECTORY /tmp/_test_root_sipi THROUGH ROOT
    
    >>> p="/tmp/_test_sipi"
    >>> rm(p)
    ('ok', '/tmp/_test_sipi')
    >>> touch(p)
    ('ok', '/tmp/_test_sipi')
    >>> can_write(p)
    ('ok', True)
    >>> rm(p)
    ('ok', '/tmp/_test_sipi')
    >>> can_write("/tmp/_test_root_sipi/")
    ('ok', False)
    """
    try:
        return ("ok", os.access(path, os.W_OK))
    except:
        return ("error", None)



def remove_common_prefix(common_prefix, path):
    """
    >>> remove_common_prefix("/tmp", "/tmp/some_dir/some_file.ext")
    ('ok', '/some_dir/some_file.ext')
    """
    try:
        _head, _sep, tail=path.partition(common_prefix)
        return ("ok", tail)
    except:
        return ("error", path)


def gen_walk(path, max_files=None):
    count=0
    done=False
    for root, _dirs, files in os.walk(path):
        
        for f in files:
            yield os.path.join(root, f)
        
            count=count+1
            if max_files is not None:
                if count==max_files:
                    done=True
                    break
            
        if done: break
        



def safe_listdir(path):
    try:
        return ("ok", os.listdir(path))
    except Exception, e:
        return ("error", e)

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

def mkdir_p(path):
    """
    Silently (i.e. no exception thrown) makes a directory structure if possible
    
    This function can be found in the 'sipi' package.
    """
    try:
        os.makedirs(path)
        return ('ok', path)
    except OSError, exc:
        if exc.errno == errno.EEXIST:
            return ('ok', path)

        return ('error', (exc.errno, errno.errorcode[exc.errno]))


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
    