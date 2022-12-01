# coding: utf-8


class Command():
    @staticmethod
    def create(vals={}):
        return 0, 0, vals

    @staticmethod
    def update(oid, vals={}):
        return 1, oid, vals

    @staticmethod
    def delete(oid):
        return 2, oid, 0

    @staticmethod
    def unlink(oid):
        return 3, oid, 0

    @staticmethod
    def link(oid):
        return 4, oid, 0

    @staticmethod
    def clear():
        return 5, 0, 0

    @staticmethod
    def set(oids):
        return 6, 0, oids
