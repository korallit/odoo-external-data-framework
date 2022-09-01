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
    object_link_ids = fields.Many2many(  # TODO: move to ir.model.data
        'external.data.object.link',
        string="Object links",
    )
    link_count = fields.Integer(
        "Links",
        compute='_compute_link_count',
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

    @api.depends('object_link_ids')
    def _compute_link_count(self):
        for record in self:
            record.link_count = len(record.object_link_ids)

    @api.depends('foreign_id')
    def _compute_name(self):  # TODO: should be default
        for record in self:
            record.name = record.foreign_id

    def get_object_link(self, model_id, variant_tag=False):
        self.ensure_one()
        object_link = self.object_link_ids.filtered(
            lambda r: r.model_id.id == model_id and
            r.variant_tag == variant_tag
        )
        msg_tail = f"external object ID {self.id}, model ID {model_id}"
        if not object_link:
            _logger.warning(f"No object link found: {msg_tail}")
        elif len(object_link) > 1:
            _logger.warning(
                "Multiple object link found, "
                f"consider data consolidation: {msg_tail}"
            )
            object_link = object_link[0]
        return object_link

    def _record(self, model_id):
        return self.get_object_link(model_id)._record()

    def write_odoo_record(self, vals, metadata):
        self.ensure_one()
        model_id = metadata.get('model_id')
        model_model = metadata.get('model_model')
        variant_tag = metadata.get('variant_tag')
        # getting model
        object_link = self.get_object_link(model_id, variant_tag)
        if object_link:
            self.sanitize_values(vals, prune_false=False, **metadata)
            record = object_link._record()
            record.write(vals)
        elif model_id and model_model:
            if self.sanitize_values(vals, **metadata):
                record = self.env[model_model].create(vals)
                self.object_link_ids = [Command.create({
                    'model_id': model_id,
                    'record_id': record.id,
                    'variant_tag': variant_tag,
                })]
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

    def link_similar_objects(self, model_id, search_own_source=False, **kw):
        """Tries to find similar objects in other data_sources by foreign_id,
        sets on record if found, returns boolean."""
        if not self:
            return False
        self.ensure_one()

        # TODO: is variant_tag mandatory?
        # find similar object links
        foreign_type_domain = [('model_id', '=', model_id)]
        if not search_own_source:
            foreign_type_domain.append(
                ('data_source_id', '!=', self.data_source_id.id))

        similar_type_ids = self.env['external.data.field.mapping'].search(
            foreign_type_domain).mapped('foreign_type_id').ids
        similar_object_links = self.search([
            ('foreign_id', '=', self.foreign_id),
            ('foreign_type_id', 'in', similar_type_ids),
        ]).mapped('object_link_ids').filtered(
            lambda r:
            r.model_id.id == model_id and
            r.variant_tag == kw.get('variant_tag') and
            r.id not in self.object_link_ids.ids and
            r._record()
        )
        if not similar_object_links:
            return False

        _logger.info(
            "Adding similar object links ids to object "
            f"ID {self.id}: {similar_object_links.ids}"
        )
        self.object_link_ids = [
            Command.link(link.id) for link in similar_object_links
        ]
        return True

    def healthcheck(self):
        """Delete empty object links, try to find valid ones."""
        for record in self:
            record.object_link_ids.prune()
            model_ids = self.data_source_id.field_mapping_ids.mapped(
                'model_id').ids
            for model_id in model_ids:
                record.link_similar_objects(model_id, search_own_source=True)

    @api.model
    def sanitize_values(self, vals, **kw):
        if kw.get('operation') in ['pull', 'edit'] and kw.get('model_model'):
            return self._sanitize_vals_pull(vals, **kw)
        elif kw.get('operation') == 'push' and kw.get('foreign_type_id'):
            # TODO: sanitize push values
            pass

    @api.model
    def _sanitize_vals_pull(self, vals, model_model, prune_false=True, **kw):
        model = self.env[model_model]
        fields_data = model.fields_get()
        fields_keys = list(fields_data.keys())
        vals_copy = vals.copy()  # can't pop from the iterated dict
        for key, value in vals_copy.items():
            # drop irrelevant item
            if key not in fields_keys or (prune_false and not value):
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
                data.get('type') not in ['one2many', 'many2many'],  # TODO: ???
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
            elif type(value) == int or value is False:
                vals[key] = value
            else:
                vals.pop(key)
        elif ttype in ['one2many', 'many2many']:
            # only clear and link is supported
            if type(value) == int:
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
                    if type(i) == int
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

    def button_details(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.object",
            "views": [[False, "form"]],
            "res_id": self.id,
        }


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
    object_ids = fields.Many2many(
        'external.data.object',
        string="External objects",
    )
    variant_tag = fields.Char(
        "Variant tag",
        help=("When multiple records of a certain type "
              "are linked to one external object, the only way to"
              "distinguish them is by this tag. "
              "It comaes from the field mapping.")
    )

    @api.depends('model_id', 'record_id')
    def _compute_name(self):
        for record in self:
            related_rec = record._record()
            if related_rec:
                record.name = related_rec.display_name
            else:
                record.name = "N/A"

    def _record(self):
        if not self:
            return False
        self.ensure_one()
        return self.env[self.model_id.model].browse(self.record_id).exists()

    def button_open(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": self.model_model,
            "views": [[False, "form"]],
            "res_id": self.record_id,
        }

    def prune(self):
        self.filtered(lambda r: not r._record()).unlink()
