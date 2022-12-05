# coding: utf-8

import jinja2
from odoo import fields, models


class ExternalDataSerializer(models.Model):
    _inherit = 'external.data.serializer'

    engine = fields.Selection(
        selection_add=[('jinja2', "Jinja2")]
    )
    jinja2_template = fields.Text("Jinja2 template")

    def _render_jinja2(self, data, metadata, key=False):
        self.ensure_one()
        environment = jinja2.Environment()
        template = environment.from_string(self.jinja2_template)
        return template.render(vals=data, metadata=metadata)
