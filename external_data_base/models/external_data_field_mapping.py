# coding: utf-8

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
        for rec in self:
            src = "{}.{}".format(rec.foreign_type_id.name, rec.foreign_field_id.name)
            dst = "{}.{}".format(rec.odoo_field_id.model, rec.odoo_field_id.name)
            rec.name = "{} > {}".format(src, dst)


class ExternalDataFieldMapping(models.Model):
    _name = 'external.data.field.mapping'
    _description = "External Data Field Mapping"
    _order = 'sequence'

    name = fields.Char(required=True)
    sequence = fields.Integer(
        required=True,
        default=10,
    )
    model_id = fields.Many2one(
        'ir.model',
        string="Model",
        required=True,
        ondelete='cascade',
    )
    model_model = fields.Char(related='model_id.model')
    filter_domain = fields.Char("Filter")
    record_count = fields.Integer(compute='_count_records')
    prune_vals = fields.Boolean(
        "Prune values",
        help="Delete values from data before write that are "
        "not included in the mapping or the ruleset",
        default=True,
    )
    skip_write = fields.Boolean("Skip write")
    export_xml_id = fields.Boolean("Export XML ID")
    foreign_type_id = fields.Many2one(  # TODO: required if not mass edit
        'external.data.type',
        string="Foreign Type",
    )
    object_link_variant_tag = fields.Char(
        "Object link variant tag"
    )
    data_source_id = fields.Many2one(
        'external.data.source',
        string="Data source",
        required=True,
        ondelete='cascade',
    )
    strategy_ids = fields.Many2many(
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
    name_is_unique = fields.Boolean("Name is unique")

    @api.depends('filter_domain')
    def _count_records(self):
        for record in self:
            model = record.model_model
            domain = eval(record.filter_domain) if record.filter_domain else []
            record.record_count = self.env[model].search_count(domain)

    def button_details(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'external.data.field.mapping',
            'views': [(False, 'form')],
            'res_id': self.id,
        }

    def apply_mapping(self, data, metadata={}):
        self.ensure_one()
        field_mapping_lines = self.field_mapping_line_ids
        pre_post = metadata.get('pre_post')
        if pre_post:
            field_mapping_lines = field_mapping_lines.filtered(
                lambda l: not l.pre_post or l.pre_post == pre_post
            )
        if 'processed_keys' not in metadata.keys():
            metadata['processed_keys'] = []

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
            metadata['processed_keys'].append('id')
            if self.export_xml_id:
                vals['xml_id'] = data._export_rows([['id']])[0][0]
                metadata['processed_keys'].append('xml_id')
        else:
            raise ValidationError(
                "Mapping can process a dictionary (pull) "
                "or an odoo record (push)."
            )

        if metadata.get('operation') == 'edit':
            return vals

        for mapping_line in field_mapping_lines:
            source_key = mapping_line[source_field].name
            target_key = mapping_line[target_field].name
            if target_key not in vals.keys():
                vals[target_key] = vals.get(source_key)

            metadata['processed_keys'].append(target_key)

        return vals
