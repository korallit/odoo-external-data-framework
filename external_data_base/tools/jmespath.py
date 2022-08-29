# coding: utf-8

import jmespath
from jmespath import functions


class CustomFunctions(functions.Functions):
    @functions.signature({'types': ['object']})
    def _func_items(self, o):
        return [list(item) for item in o.items()]

    @functions.signature({'types': ['array']})
    def _func_from_items(self, d):
        return dict(d)


options = jmespath.Options(custom_functions=CustomFunctions())
