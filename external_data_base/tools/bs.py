# coding: utf-8

from bs4 import element as bs_element

import logging
_logger = logging.getLogger(__name__)


def get_index(i):
    if i == '':
        return None
    try:
        return int(i)
    except ValueError:
        _logger.warning("Index should be an integer")
        return None


def compute_conditions(item, name=None, attrs={}, attrs_not={}):
    conditions = []
    conditions_not = []
    if name:
        conditions.append(item.name == name)
    for key, value in attrs.items():
        attr_vals = item.get(key)
        if isinstance(attr_vals, list):
            conditions.append(value in attr_vals)
        else:
            conditions.append(attr_vals == value)
    for key, value in attrs_not.items():
        attr_vals = item.get(key)
        if isinstance(attr_vals, list):
            conditions_not.append(value in attr_vals)
        else:
            conditions_not.append(attr_vals == value)
    conditions.append(not any(conditions_not))
    return all(conditions)


def findall(tag, name, attrs, attrs_not, direction=None,
            start=None, end=None, recursive=None):
    if start or end:
        for item in tag.find_all(name=name, attrs=attrs)[start:end]:
            yield item
    else:
        if recursive:
            generator = tag.children
        elif direction == 'next':
            generator = tag.next_siblings
        elif direction == 'prev':
            generator = tag.previous_siblings
        else:
            generator = tag.descendants
        found = False
        for item in generator:
            if isinstance(item, bs_element.Tag):
                if compute_conditions(item, name, attrs, attrs_not):
                    found = True
                    yield item
        if not found:
            yield None
