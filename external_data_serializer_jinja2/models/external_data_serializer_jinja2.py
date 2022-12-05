# coding: utf-8

import jinja2
from odoo import fields, models


class ExternalDataSerializer(models.Model):
    _inherit = 'external.data.serializer'

    engine = fields.Selection(
        selection_add=[('jinja2', "Jinja2")]
    )
    jinja2_template = fields.Text("Jinja2 template")

    def _render_jinja2(self, data, metadata, key='items'):
        self.ensure_one()
        if key:
            data = data.get(key)

        environment = jinja2.Environment()
        template = environment.from_string(self.jinja2_template)
        return template.render(items=data, metadata=metadata)
