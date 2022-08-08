# coding: utf-8

import re
import requests
from base64 import b64encode

from odoo import api, fields, models
from odoo.fields import Command
from odoo.exceptions import ValidationError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataFieldMappingLine(models.Model):
    _name = 'external.data.field.mapping.line'
    _description = "External Data Field Mapping Line"

    name = fields.Char(compute="_compute_name")
    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        ondelete='cascade',
        string="Field mapping",
        required=True,
    )
    foreign_field_name = fields.Char("Source key", required=True)
    odoo_model = fields.Char(related='field_mapping_id.model_id.model')
    odoo_field = fields.Many2one(
        'ir.model.fields',
        ondelete='cascade',
        string="Odoo field",
        required=True,
    )

    @api.depends(
        'field_mapping_id',
        'foreign_field_name',
        'odoo_field',
    )
    def _compute_name(self):
        for record in self:
            src = '.'.join([
                record.field_mapping_id.foreign_type,
                record.foreign_field_name,
            ])
            dst = f"{record.odoo_field.model}.{record.odoo_field.name}"
            record.name = f"{src} > {dst}"


class ExternalDataFieldMapping(models.Model):
    _name = 'external.data.field.mapping'
    _description = "External Data Field Mapping"
    _order = 'sequence'

    name = fields.Char(required=True)
    sequence = fields.Integer(
        required=True,
        default=1,
    )
    foreign_type = fields.Char(
        "Foreign Type",
        required=True,
    )
    model_id = fields.Many2one(
        'ir.model',
        ondelete='cascade',
        string="Model",
        required=True,
    )
    foreign_id_key = fields.Char(
        "Foreign ID key",
        required=True,
        help="The ID parameter key in the external data structure."
    )
    data_source_id = fields.Many2one(
        'external.data.source',
        ondelete='cascade',
        string="Data source",
        required=True,
    )
    field_mapping_line_ids = fields.One2many(
        'external.data.field.mapping.line',
        inverse_name='field_mapping_id',
        string="Field mapping line",
    )
    rule_ids = fields.One2many(
        'external.data.rule',
        inverse_name='field_mapping_id',
        string="Rules",
    )
    rule_ids_pre = fields.One2many(
        'external.data.rule',
        inverse_name='field_mapping_id',
        string="Pre rules",
        domain=[('pre_post', '=', 'pre')],
    )
    rule_ids_post = fields.One2many(
        'external.data.rule',
        inverse_name='field_mapping_id',
        string="Post rules",
        domain=[('pre_post', '=', 'post')],
    )

    def apply_mapping(self, data):
        self.ensure_one()
        if isinstance(data, dict):  # pull
            vals = data.copy()
            for mapping_line in self.field_mapping_line_ids:
                source_key = mapping_line.foreign_field_name
                target_key = mapping_line.odoo_field.name
                if key in vals.keys():
                    continue
                vals[target_key] = vals.get(source_key)
            return vals
        elif isinstance(data, models.Model):  # push
            data.ensure_one()
            vals = {}
            for mapping_line in self.field_mapping_line_ids:
                source_key = mapping_line.odoo_field.name
                target_key = mapping_line.foreign_field_name
                vals[target_key] = data[source_key]
            return vals
        else:
            raise ValidationError(
                "Mapping can process a dictionary (pull) "
                "or an odoo record (push)."
            )

    def sanitize_values(self, vals):
        self.ensure_one()
        model = self.env[self.model_id.model]
        fields_data = model.fields_get()
        defaults = model.default_get(fields_data.keys())
        enough_to_create = True
        for name, field in fields_data.items():
            if name not in vals.keys():
                continue
            value = vals[name]
            # if values is a recordset, get ids/id
            if isinstance(value, models.Model):
                if not bool(value):
                    value = False
                elif len(value) > 1:
                    value = value.ids
                else:
                    value = value.id
            # check required fields
            if (
                    not value and field.get('required') and
                    name not in defaults and
                    field.get('type') not in ['one2many', 'many2many']
            ):
                _logger.warning(
                    "Missing required field of model {}: {}".format(
                        model._name, name
                    )
                )
                enough_to_create = False
            # check value by type
            if field.get('type') == 'binary':
                if isinstance(value, str):
                    data = self._fetch_binary_data(vals[name])
                    if data:
                        vals[name] = data
                    else:
                        vals.pop(name)
                elif value is not False:
                    vals.pop(name)
            elif field.get('type') == 'many2one':
                if isinstance(value, str):
                    try:
                        vals[name] = int(value)
                    except Exception as e:
                        _logger.error(e)
                        vals.pop(name)
                elif isinstance(value, list) and value:
                    vals[name] = value[0]
                elif not (isinstance(value, int) or value is False):
                    vals.pop(name)
            elif field.get('type') in ['one2many', 'many2many']:
                # only clear and link is supported
                if isinstance(value, int):
                    value = [value]
                elif isinstance(value, str):
                    try:
                        value = [
                            int(i) for i in
                            re.sub(' ', '', value).split(',')
                        ]
                    except Exception as e:
                        _logger.error(e)

                if isinstance(value, list):
                    vals[name] = [
                        Command.link(i) for i in value
                        if isinstance(i, int)
                    ]
                elif value is False:
                    vals[name] = [Command.clear()]
                else:
                    vals.pop(name)

        # drop irrelevant items
        irrelevant_keys = set(vals.keys()) - set(fields_data.keys())
        for key in irrelevant_keys:
            vals.pop(key)
        return enough_to_create

    @api.model
    def _fetch_binary_data(self, url):
        if not isinstance(url, str):
            _logger.error(f"Invalid URL: {url}")
            return False
        try:
            res = requests.get(url)
        except Exception as e:
            _logger.error(e)
            return False
        if isinstance(res.content, bytes):
            return b64encode(res.content)
        return False

    def button_details(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.field.mapping",
            "views": [[False, "form"]],
            "res_id": self.id,
        }
