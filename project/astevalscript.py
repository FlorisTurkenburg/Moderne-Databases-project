from asteval import Interpreter
import functools
import re

class Script(object):
    def __init__(self):
        """
        Sets up an interpreter.
        """
        self.interpreter = Interpreter()
        self.symtable['re'] = re

    @property
    def symtable(self):
        """
        Expose the internal symbol table.
        """
        return self.interpreter.symtable

    @symtable.setter
    def symtable(self, symtable):
        """
        Apply changes to the internal symbol table.
        """
        self.interpreter.symtable = value

    def add_file(self, path):
        """
        Adds and loads code from a script file.
        """
        with open(path, 'rb') as f:
            self.interpreter(f.read())

    def add_string(self, string):
        """
        Adds and loads code from a string.
        """
        self.interpreter(string)

    def invoke(self, name, *args, **kwargs):

        """
        Invokes a function in the script with the appropriate arguments.
        """
        f = self.interpreter.symtable.get(name, None)

        if not callable(f):
            return

        return f(*args, **kwargs)

    def __getattr__(self, name):
        """
        Returns the function to invoke a function in the script, if a function
        with that name exists within the symbol table. Otherwise, an attribute
        error is being raised (default behaviour).
        """
        if name in ['symtable', 'interpreter']:
            raise AttributeError("{} instance has no attribute '{}'".format(
                self.__class__.__name__, name))

        if not callable(self.symtable.get(name, None)):
            raise AttributeError("{} instance has no attribute '{}'".format(
                self.__class__.__name__, name))

        return functools.partial(self.invoke, name)