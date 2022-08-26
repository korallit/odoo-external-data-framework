# coding: utf-8

import json
import logging

from odoo import api, fields, models
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)


class ExternalDataType(models.Model):
    _name = 'external.data.type'
    _description = "External Data Type"

    name = fields.Char(required=True)
    field_ids = fields.One2many(
        'external.data.type.field',
        inverse_name='foreign_type_id',
        string="Foreign field",
    )
    resource_ids = fields.Many2many(
        comodel_name='external.data.resource',
        string="Resources",
    )
    field_mapping_ids = fields.One2many(
        'external.data.field.mapping',
        inverse_name='foreign_type_id',
        string="Field mapping",
    )


class ExternalDataTypeField(models.Model):
    _name = 'external.data.type.field'
    _description = "External Data Type Field"
    _order = 'priority'

    name = fields.Char(required=True)
    foreign_type_id = fields.Many2one(
        'external.data.type',
        string="Foreign type",
        required=True,
        ondelete='cascade',
    )
    priority = fields.Integer(default=10)


class ExternalDataFieldMappingLine(models.Model):
    _name = 'external.data.field.mapping.line'
    _description = "External Data Field Mapping Line"

    name = fields.Char(compute="_compute_name")
    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        string="Field mapping",
        required=True,
        ondelete='cascade',
    )
    foreign_type_id = fields.Many2one(
        related='field_mapping_id.foreign_type_id',
    )
    foreign_field_id = fields.Many2one(
        'external.data.type.field',
        string="Foreign field",
        required=True,
        ondelete='restrict',
        domain="[('foreign_type_id', '=', foreign_type_id)]",
    )
    odoo_model = fields.Char(related='field_mapping_id.model_id.model')
    odoo_field_id = fields.Many2one(
        'ir.model.fields',
        string="Odoo field",
        required=True,
        ondelete='cascade',
        domain="[('model', '=', odoo_model)]",
    )
    pre_post = fields.Selection(
        string="Pre/Post",
        help="'Both' if unspecified",
        selection=[('pre', 'pre'), ('post', 'post')],
        default='pre',
    )

    @api.depends(
        'field_mapping_id',
        'foreign_field_id',
        'odoo_field_id',
    )
    def _compute_name(self):
        for record in self:
            src = '.'.join([
                record.field_mapping_id.foreign_type,
                record.foreign_field_id.name,
            ])
            dst = f"{record.odoo_field_id.model}.{record.odoo_field_id.name}"
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
    model_id = fields.Many2one(
        'ir.model',
        string="Model",
        required=True,
        ondelete='cascade',
    )
    model_model = fields.Char(related='model_id.model')
    filter_domain = fields.Char("Filter")
    foreign_type_id = fields.Many2one(
        'external.data.type',
        string="Foreign Type",
    )
    data_source_id = fields.Many2one(
        'external.data.source',
        string="Data source",
        required=True,
        ondelete='cascade',
    )
    strategy_id = fields.Many2many(
        'external.data.strategy',
        string="Strategy",
        domain="['data_source_id', '=', data_source_id]",
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
    test_data = fields.Text("Test data", default="{}")
    test_metadata = fields.Text("Test metadata", default="{}")

    def apply_mapping(self, data, metadata={}):
        self.ensure_one()
        field_mapping_lines = self.field_mapping_line_ids
        pre_post = metadata.get('pre_post')
        if pre_post:
            field_mapping_lines = field_mapping_lines.filtered(
                lambda l: not l.pre_post or l.pre_post == pre_post
            )
        if isinstance(data, dict):  # pull
            source_field = 'foreign_field_id'
            target_field = 'odoo_field_id'
            vals = data.copy()
        elif isinstance(data, models.Model):  # push
            data.ensure_one()
            source_field = 'odoo_field_id'
            target_field = 'foreign_field_id'

            source_keys = field_mapping_lines.mapped('odoo_field_id.name')
            vals = data.read(source_keys)[0]
        else:
            raise ValidationError(
                "Mapping can process a dictionary (pull) "
                "or an odoo record (push)."
            )

        if 'processed_keys' not in metadata.keys():
            metadata['processed_keys'] = []

        for mapping_line in field_mapping_lines:
            source_key = mapping_line[source_field].name
            target_key = mapping_line[target_field].name
            if target_key not in vals.keys():
                vals[target_key] = vals.get(source_key)

            metadata['processed_keys'].append(target_key)

        return vals

    def button_details(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.field.mapping",
            "views": [[False, "form"]],
            "res_id": self.id,
        }
