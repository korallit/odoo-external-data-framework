# -*- coding: utf-8 -*-

# from odoo import models, fields, api


# class meleget_hu(models.Model):
#     _name = 'meleget_hu.meleget_hu'
#     _description = 'meleget_hu.meleget_hu'

#     name = fields.Char()
#     value = fields.Integer()
#     value2 = fields.Float(compute="_value_pc", store=True)
#     description = fields.Text()
#
#     @api.depends('value')
#     def _value_pc(self):
#         for record in self:
#             record.value2 = float(record.value) / 100
