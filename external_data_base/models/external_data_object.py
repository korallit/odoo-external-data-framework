# coding: utf-8

from datetime import datetime
from odoo import fields, models

import logging
_logger = logging.getLogger(__name__)


class ExternalDataObject(models.Model):
    _name = 'external.data.object'
    _description = "External Data Object"

    name = fields.Char()  # TODO: compute?
    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        string="Type mappings",
        required=True,
    )
    foreign_id = fields.Char(  # TODO: foreign_id
        "Foreign ID",
        help="A unique identifier that helps the CRUD methods "
        "to match the foreign object with an odoo record.",
        required=True,
    )
    field_mapping_model = fields.Char(
        "Mapped model",
        related='field_mapping_id.model_id.model',
        store=True,
    )
    object_link_id = fields.Many2one(
        'external.data.object.link',
        ondelete='set null',
        string="Object link",
    )
    object_link_record_id = fields.Many2oneReference(
        "Linked record id",
        model_field='field_mapping_model',
        related='object_link_id.record_id',
    )
    package_ids = fields.Many2many(
        'external.data.package',
        string="Data source packages",
    )
    data_source_id = fields.Many2one(related='field_mapping_id.data_source_id')
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
        domain=[
            ('direction', '=', 'pull'),
            ('pre_post', '=', 'pre'),
        ],
    )
    rule_ids_post = fields.One2many(
        'external.data.rule',
        inverse_name='object_id',
        string="Post rules",
        domain=[
            ('direction', '=', 'pull'),
            ('pre_post', '=', 'post'),
        ],
    )
    rule_ids_push = fields.One2many(
        'external.data.rule',
        inverse_name='object_id',
        string="Post rules",
        domain=[('direction', '=', 'push')],
    )
    last_sync = fields.Datetime("Last sync")

    def write_odoo_object(self, vals):
        self.ensure_one()
        self.field_mapping_id.sanitize_values(vals)
        if not self.object_link_id:
            model = self.field_mapping_id.model_id
            record = self.env[model.model].create(vals)
            object_link = self.object_link_id.create({
                'model_id': model.id,
                'record_id': record.id,
            })
            self.object_link_id = object_link.id
        else:
            record = self.object_link_id._record()
            record.write(vals)
        self.last_sync = datetime.now()

    def find_and_set_object_link_id(self):
        """Tries to find object in other data sources,
        sets on record if found one, returns boolean."""
        self.ensure_one()
        if self.object_link_id:
            return True

        other_data_source_ids = self.field_mapping_id.relevant_data_source_ids
        if not other_data_source_ids:
            return False

        other_field_mappings = self.field_mapping_id.search([
            ('data_source_id', 'in', other_data_source_ids),
            ('model_id', '=', self.field_mapping_id.model_id.id)
        ])
        if not other_field_mappings:
            return False

        similar_objects = self.search([
            ('field_mapping_id', 'in', other_field_mappings.ids)
        ])
        if not similar_objects:
            return False

        object_link_ids = similar_objects.mapped('object_link_id')
        if len(object_link_ids) > 1:
            _logger.warning(
                f"Multiple object links found for object ID {self.id}, "
                "picking first. Consider manual data consolidation."
            )
        self.object_link_id = object_link_ids[0]
        return True


class ExternalDataObjectLink(models.Model):
    _name = 'external.data.object.link'
    _description = "External Data Object Link"

    name = fields.Char()
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

    def _record(self):
        if  not self:
            return False
        self.ensure_one()
        return self.env[self.model_id.model].browse(self.record_id).exists()
