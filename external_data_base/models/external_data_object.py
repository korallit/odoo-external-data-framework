# coding: utf-8

import re

from datetime import datetime
from odoo import api, fields, models
from odoo.exceptions import MissingError, UserError
from odoo.fields import Command
from odoo.tools import image

import logging
_logger = logging.getLogger(__name__)


class ExternalDataObject(models.Model):
    _name = 'external.data.object'
    _description = "External Data Object"

    name = fields.Char(compute='_compute_name')
    foreign_type_id = fields.Many2one(
        'external.data.type',
        string="Foreign type",
        required=True,
    )
    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        string="Type mappings",
    )
    foreign_id = fields.Char(
        "Foreign ID",
        help="A unique identifier that helps the CRUD methods "
        "to match the foreign object with an odoo record.",
        required=True,
    )
    object_link_id = fields.Many2one(  # TODO: move to ir.model.data
        'external.data.object.link',
        ondelete='set null',
        string="Object link",
    )
    data_source_id = fields.Many2one(
        'external.data.source',
        string="Data source",
    )
    resource_ids = fields.Many2many(
        'external.data.resource',
        string="Resources",
    )
    priority = fields.Integer(default=10)
    rule_ids = fields.One2many(
        'external.data.rule',
        inverse_name='object_id',
        string="Rules",
    )
    rule_ids_pre = fields.One2many(
        'external.data.rule',
        inverse_name='object_id',
        string="Pre rules",
        domain=[('pre_post', '=', 'pre')],
    )
    rule_ids_post = fields.One2many(
        'external.data.rule',
        inverse_name='object_id',
        string="Post rules",
        domain=[('pre_post', '=', 'post')],
    )
    last_sync = fields.Datetime("Last sync")

    @api.depends('foreign_id')
    def _compute_name(self):
        for record in self:
            related_rec = record._record()
            if related_rec:
                record.name = related_rec.display_name
            else:
                record.name = record.foreign_id

    def _record(self):
        self.ensure_one()
        if not self.object_link_id:
            return False
        res_model = self.object_link_id.model_model
        res_id = self.object_link_id.record_id
        return self.env[res_model].browse(res_id).exists()

    def write_odoo_record(self, vals,
                          model_id=False, model_model=False, **kw):
        self.ensure_one()
        # getting model
        if self.object_link_id:
            model_model = self.object_link_id.model_model
            self.sanitize_values(vals, model_model)
            record = self._record()
            record.write(vals)
        elif model_id and model_model:
            if self.sanitize_values(vals, model_model):
                record = self.env[model_model].create(vals)
                object_link = self.object_link_id.create({
                    'model_id': model_id,
                    'record_id': record.id,
                })
                self.object_link_id = object_link.id
            else:
                _logger.error(
                    "Provided values are not enough "
                    f"for creating a record in model {model_model}"
                )
        else:
            raise MissingError(
                "If no object link, parameter 'model' is mandatory!"
            )
        self.last_sync = datetime.now()

    def find_and_set_object_link_id(self, model_id, **kw):
        """Tries to find object in other data sources by foreign_id,
        sets on record if found one, returns boolean."""

        # find similar object links
        similar_type_ids = self.env['external.data.field.mapping'].search([
            ('data_source_id', '!=', self.data_source_id.id),
            ('model_id', '=', model_id),
        ]).mapped('foreign_type_id')
        object_link_ids = self.env['external.data.object'].search([
            ('foreign_type_id', 'in', similar_type_ids),
            ('foreign_id', '=', self.foreign_id),
        ]).mapped('object_link_id')
        if not object_link_ids:
            return False

        if len(object_link_ids) > 1:
            _logger.warning(
                f"Multiple object links found for object ID {self.id}, "
                "picking first. Consider manual data consolidation."
            )
        self.object_link_id = object_link_ids[0]
        return True

    @api.model
    def sanitize_values(self, vals, model_model, **kw):
        model = self.env[model_model]
        fields_data = model.fields_get()
        fields_keys = list(fields_data.keys())
        vals_copy = vals.copy()  # can't pop from the iterated dict
        for key, value in vals_copy.items():
            # drop irrelevant item
            if key not in fields_keys:
                vals.pop(key)
                continue

            # transform recordset
            if isinstance(value, models.Model):
                value = self._recordset_to_int_list(value)

            # check value by type
            field_data = fields_data[key]
            ttype = field_data.get('type')
            if ttype in ['many2one', 'one2many', 'many2many']:
                self._sanitize_relational(ttype, key, value, vals)
            elif ttype == 'binary':
                self._sanitize_binary(model, key, value, vals)

        # check required
        fields_with_default = model.default_get(fields_keys).keys()
        for name, data in fields_data.items():
            conditions = [
                name not in vals.keys(),
                data.get('required'),
                name not in fields_with_default,
                data.get('type') not in ['one2many', 'many2many'],
            ]
            if all(conditions):
                _logger.warning(
                    f"Missing required field of model {model_model}: {name}"
                )
                return False
        return True

    @api.model
    def _sanitize_relational(self, ttype, key, value, vals):
        if ttype == 'many2one':
            if isinstance(value, str):
                try:
                    vals[key] = int(value)
                except Exception as e:
                    _logger.error(e)
                    vals.pop(key)
            elif isinstance(value, list) and value:
                vals[key] = value[0]
            elif isinstance(value, int) or value is False:
                vals[key] = value
            else:
                vals.pop(key)
        elif ttype in ['one2many', 'many2many']:
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
                vals[key] = [
                    Command.link(i) for i in value
                    if isinstance(i, int)
                ]
            elif value is False:
                vals[key] = [Command.clear()]
            else:
                vals.pop(key)

    @api.model
    def _sanitize_binary(self, model, key, value, vals):
        model_dict = model.__class__.__dict__
        field_classname = model_dict[key].__class__.__name__
        if field_classname == 'Image':
            if isinstance(value, str) or isinstance(value, bytes):
                try:
                    img = image.image_process(value)
                    vals[key] = img
                except UserError as e:
                    _logger.error(e)
                    vals.pop(key)
            elif value is False:
                vals[key] = value
            else:
                vals.pop(key)
        elif field_classname == 'Binary':
            if isinstance(value, bytes) or value is False:
                vals[key] = value
            else:
                vals.pop(key)

    @api.model
    def _recordset_to_int_list(self, records):
        if not bool(records):
            return False
        elif len(records) > 1:
            return records.ids
        else:
            return records.id


class ExternalDataObjectLink(models.Model):
    _name = 'external.data.object.link'
    _description = "External Data Object Link"

    name = fields.Char(compute='_compute_name')
    model_id = fields.Many2one(
        'ir.model',
        string="Model",
        ondelete='cascade',
        required=True,
    )
    model_model = fields.Char(
        "Model name",
        related='model_id.model',
    )
    record_id = fields.Many2oneReference(
        "Related record",
        model_field='model_model',
        required=True,
    )
    object_ids = fields.One2many(
        'external.data.object',
        inverse_name='object_link_id',
        string="External objects",
    )

    @api.depends('model_id', 'record_id')
    def _compute_name(self):
        for record in self:
            related_rec = record._record()
            if related_rec:
                record.name = related_rec.display_name

    def _record(self):
        if not self:
            return False
        self.ensure_one()
        return self.env[self.model_id.model].browse(self.record_id).exists()
