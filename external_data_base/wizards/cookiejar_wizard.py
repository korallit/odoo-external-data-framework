# coding: utf-8

import logging

from odoo import fields, models

_logger = logging.getLogger(__name__)


class ExternalDataCookiejarWizard(models.TransientModel):
    _name = 'external.data.cookiejar.wizard'
    _description = "Create Cookiejar"

    name = fields.Char(required=True)

    def button_submit(self):
        for record in self:
            record._create_attachment()

    def _create_attachment(self):
        self.ensure_one()
        att = self.env['ir.attachment'].create([{
            'name': self.name,
            'res_model': 'external.data.transporter',
            'res_field': 'http_cookiejar',
            'mimetype': 'text/plain',
        }])
        att.raw = "# Netscape HTTP Cookie File\n"
        _logger.info(f"Cookiejar '{self.name}' created")
