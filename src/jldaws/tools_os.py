"""
    Created on 2012-01-19
    @author: jldupont
"""
import os, errno,re
import subprocess

RE_EXT=re.compile(r'^(.*?)\-(.*?)\.([a-zA-Z]+|[a-zA-Z]+\.[a-zA-Z]+)$')

def split_path_version(path):
    """
    >>> print split_path_version("file.ext")
    (None, None, None)
    >>> print split_path_version("file-0.1.ext")
    ('file', '0.1', 'ext')
    >>> print split_path_version("file-0.1.1.ext")
    ('file', '0.1.1', 'ext')
    >>> print split_path_version("file-0.1.1.tar.gz")
    ('file', '0.1.1', 'tar.gz')
    >>> print split_path_version("file-0.1.1a.tar.gz")
    ('file', '0.1.1a', 'tar.gz')
    >>> print split_path_version("file-0.1b.1a.tar.gz")
    ('file', '0.1b.1a', 'tar.gz')
    >>> print split_path_version("file-0.1b.1-dev.tar.gz")
    ('file', '0.1b.1-dev', 'tar.gz')
    >>> print split_path_version("file-0.4.tar.gz")
    ('file', '0.4', 'tar.gz')
    """
    try:
        groups=RE_EXT.match(path).groups()
        return groups
    except:
        return (None, None, None)
    


def safe_can_write(path):
    """
    Check if 'path' can be written to
    
    - If 'path' exists, do safe test (i.e. non-destructive)
    - If 'path' !exists, do 'append' test
    
    >>> safe_can_write("/tmp/!+$,@")
    ('ok', True)
    >>> rm("/tmp/!+$,@")
    ('ok', '/tmp/!+$,@')
    >>> safe_can_write("/tmp/")
    ('ok', True)
    """
    try:
        maybe_exists=os.path.exists(path)
        if maybe_exists:
            return can_write(path)
    except:
        pass

    try:
        f=open(path, "a")
    except:
        return ("error", False)
    finally:
        try:
            rm(path)
            f.close()
        except:
            pass
    
    return ("ok", True)

def check_can_write_paths(src_dir, paths):
    """
    >>> check_can_write_paths("/tmp", ["allo", "!$[].+$", ".X11-unix"])
    [('ok', ('allo', True)), ('ok', ('!$[].+$', True)), ('ok', ('.X11-unix', True))]
    """
    results=[]
    for path_fragment in paths:
        try:
            path=os.path.join(src_dir, path_fragment)
            code, resp=safe_can_write(path)
            
            results.append((code, (path_fragment, resp)))
        except Exception,e:
            results.append(("error", (path_fragment, e)))
            
    return results
    

def check_exist_paths(src_dir, paths):
    """
    >>> check_exist_paths("/tmp", ["allo", "!$[].+$", ".X11-unix"])
    [('ok', ('allo', False)), ('ok', ('!$[].+$', False)), ('ok', ('.X11-unix', True))]
    """
    results=[]
    for path_fragment in paths:
        try:
            path=os.path.join(src_dir, path_fragment)
            exists=os.path.exists(path)
            results.append(("ok", (path_fragment, exists)))
        except Exception,e:
            results.append(("error", (path_fragment, e)))
            
    return results
        


def safe_path_exists(path):
    try:
        return ("ok", os.path.exists(path))
    except:
        return ("error", False)

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
    
    @return: (code, path | msg)
    
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
    