# coding: utf-8

from odoo import fields, models


class KlimatszeretnekProductTemplate(models.Model):
    _inherit = 'product.template'

    description_sale_html = fields.Html("Sales Description HTML")
