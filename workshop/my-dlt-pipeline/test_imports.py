import sys
sys.settrace(lambda *args: None) # not needed

# A better way is to see what import fails
import builtins
orig_import = builtins.__import__

def my_import(name, globals=None, locals=None, fromlist=(), level=0):
    print("IMPORTING: " + name)
    sys.stdout.flush()
    return orig_import(name, globals, locals, fromlist, level)

builtins.__import__ = my_import

print("STARTING")
import dlt_mcp
print("SUCCESS")
