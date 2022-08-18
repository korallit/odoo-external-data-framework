# coding: utf-8

from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command
from odoo.exceptions import MissingError, UserError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataStrategy(models.Model):
    _name = 'external.data.strategy'
    _description = "External Data Strategy"

    name = fields.Char(required=True)
    operation = fields.Selection(
        string="Operation",
        selection=[
            ('list', "list"),
            ('pull', "pull"),
            ('push', "push"),
        ],
        required=True,
    )
    data_source_id = fields.Many2one(
        'external.data.source',
        string="Data source",
        required=True,
    )
    priority = fields.Integer(default=10)
    transporter_id = fields.Many2one(
        'external.data.transporter',
        string="Transporter",
    )
    transporter_create_id = fields.Many2one(
        'external.data.transporter',
        string="Transporter (push create)",
    )
    serializer_id = fields.Many2one(
        'external.data.serializer',
        string="Parser",
    )
    deferred_create = fields.Boolean(
        "Deferred create",
        help="If set record creation executed in batch, after parsing. "
        "Recommended when a resource contains lots of objects.",
    )
    field_mapping_ids = fields.Many2many(
        'external.data.field.mapping',
        string="Field mappings",
        domain="[('data_source_id', '=', data_source_id)]",
    )
    prune_vals = fields.Boolean(
        "Prune values",
        help="Delete values from data before write that are "
        "not included in the mapping or the ruleset",
        default=True,
    )
    batch_size = fields.Integer("Batch size", default=10)

    @api.model
    def get_strategy(self, operation=False,
                     data_source_id=False, resource_ids=False):
        data_source_ids = []
        domain = []
        if data_source_id:
            data_source_ids.append(data_source_id)
        if resource_ids:
            resources = self.env['external.data.resource'].search([
                ('id', 'in', resource_ids)
            ])
            data_source_ids += resources.mapped('data_source_id').ids
            if not operation and resources:
                foreign_type_ids = resources.mapped('forein_type_ids').ids
                if foreign_type_ids:
                    field_mapping_ids = self.field_mapping_id.search([
                        ('data_source_id', 'in', data_source_ids),
                        ('foreign_type_id', 'in', foreign_type_ids),
                    ]).ids
                    if field_mapping_ids:
                        domain.append(
                            ('field_mapping_id', 'in', field_mapping_ids)
                        )

        domain.append(('data_source_id', 'in', data_source_ids))
        if operation:
            domain.append(('operation', '=', operation))

        strategy = self.search(domain)
        if not strategy:
            err_msg = "Couldn't find strategy!"
        elif len(strategy) > 1:
            # TODO: chooser wizard
            err_msg = "Multiple strategy found!"
        else:
            return strategy
        raise UserError(err_msg)

    def list(self):
        self.ensure_one()
        if self.operation != 'list':
            raise UserError(f"Wrong operation type for pull: {self.operation}")
        resource_ids = self.field_mapping_ids.filtered(
            lambda r: r.model_id.model == 'external.data.resource'
        ).mapped('foreign_type_id.resource_ids').ids
        if len(resource_ids) == 1:
            self.pull_resource(resource_ids[0], sync=True, prune=True)
        elif len(resource_ids) > 1:
            self.batch_pull(resource_ids, do_all=True, sync=True, prune=True)
        else:
            raise MissingError("No resources defined for this lister")

    def batch_pull(self, resource_ids, do_all=False, sync=False, prune=False):
        i = 0
        for res_id in resource_ids:
            try:
                self.pull_resource(res_id, sync=sync, prune=prune)
            except Exception as e:
                _logger.error(e)
                resource = self.env['external.data.resource'].browse(res_id)
                if resource.exists():
                    resource.notes = ("Pull error:\n" + str(e))
                    resource.skip = True
            i += 1
            if i == self.batch_size and not do_all:
                break

    def pull_resource(self, resource_id, sync=False, prune=False):
        self.ensure_one()
        if self.operation not in ['list', 'pull']:
            raise UserError(f"Wrong operation type for pull: {self.operation}")
        resource = self.env['external.data.resource'].browse(resource_id)
        if not resource.exists():
            raise MissingError(f"Missing external resource ID {resource_id}")
        resource_name = resource.name
        _logger.info(f"Pulling resource {resource_name}")

        # fetch
        raw_data = self.transporter_id.fetch(resource_id)

        # parse
        data_source = self.data_source_id
        parser = self.serializer_id
        metadata = {  # TODO: could it be the context?
            'operation': self.operation,
            'deferred_create': self.deferred_create,
            'data_source_id': data_source.id,
            'resource_id': resource_id,
            'strategy_id': self.id,
            'strategy_name': self.name,
            'parser_id': parser.id,
        }
        ext_object_vals = {
            'data_source_id': data_source.id,
            'resource_ids': [Command.link(resource_id)],
        }
        object_data_generators = parser.parse(raw_data)
        foreign_objects = []
        for field_mapping in self.field_mapping_ids:
            foreign_type = field_mapping.foreign_type_id
            data_generator = object_data_generators.get(foreign_type.id)
            if not data_generator:
                continue
            resource.foreign_type_ids = [Command.link(foreign_type.id)]

            # executing db queries outside of the parsing loop when possible
            foreign_id_key = field_mapping.foreign_id_field_id.name
            field_mapping_id = field_mapping.id
            metadata.update({
                'field_mapping_id': field_mapping_id,
                'model_id': field_mapping.model_id.id,
                'model_model': field_mapping.model_id.model,
                'foreign_type_id': foreign_type.id,
                'now': datetime.now(),
                'record': False,
            })
            external_objects = resource.object_ids.filtered(
                lambda o: o.foreign_type_id.id == foreign_type.id
            )
            deferred_create_data = {'vals': [], 'data': [], 'object_vals': []}

            # map & process
            index = 0
            for data in data_generator:
                index += 1
                foreign_id = data.get(foreign_id_key)
                if not foreign_id:
                    _logger.error(
                        f"Missing foreign ID from resource {resource_name}"
                    )
                    continue
                if prune:
                    foreign_objects.append((foreign_id, field_mapping_id))
                if not sync:
                    continue
                metadata.update({
                    'foreign_id': foreign_id,
                    'processed_keys': [],
                })
                ext_object_vals.update({
                    'foreign_id': foreign_id,
                    'priority': index,
                })

                record = False
                if metadata['operation'] == 'list':
                    record = resource.search([
                        ('url', '=', foreign_id),
                    ], limit=1)  # TODO: check if found more than one
                    if record:
                        res_last_mod = record.last_mod
                        if not res_last_mod:
                            continue
                else:  # try to find record through object links
                    ext_object = False
                    for o in external_objects:
                        if o.foreign_id == foreign_id:
                            o.resource_ids = [Command.link(resource_id)]
                            record = o._record()
                            ext_object = o
                            break
                    if not (ext_object or metadata['deferred_create']):
                        ext_object = external_objects.create(ext_object_vals)
                        if ext_object.find_and_set_object_link_id(**metadata):
                            record = ext_object._record()

                # pre processing
                metadata.update({
                    'record': record,
                    'pre_post': 'pre',
                })
                vals = field_mapping.apply_mapping(data, metadata)
                field_mapping.rule_ids_pre.apply_rules(vals, metadata)
                if metadata['operation'] == 'list':
                    vals['data_source_id'] = metadata['data_source_id']
                    if 'processed_keys' in metadata.keys():
                        metadata['processed_keys'].append('data_source_id')
                    else:
                        metadata['processed_keys'] = ['data_source_id']

                # deferred create or list
                if not record and metadata['deferred_create']:
                    self._prune_vals(vals, **metadata)
                    if external_objects.sanitize_values(vals, **metadata):
                        deferred_create_data['vals'].append(vals)
                        deferred_create_data['data'].append(data)
                        deferred_create_data['object_vals'].append(
                            ext_object_vals)
                    continue
                if metadata['operation'] == 'list':
                    vals_last_mod = vals.get('last_mod')
                    if vals_last_mod and res_last_mod < vals_last_mod:
                        _logger.debug(
                            f"Updating resource #{index}: {foreign_id}")
                        self._prune_vals(vals, **metadata)
                        external_objects.sanitize_values(vals, **metadata)
                        record.write(vals)
                    continue

                # apply object rules and update record
                ext_object.rule_ids_pre.apply_rules(vals, metadata)
                self._prune_vals(vals, **metadata)
                ext_object.write_odoo_object(vals, **metadata)

                # post processing
                if field_mapping.rule_ids_post or ext_object.rule_ids_post:
                    metadata.update({
                        'record': ext_object._record(),
                        'pre_post': 'post',
                    })
                    vals = field_mapping.apply_mapping(data, metadata)
                    field_mapping.rule_ids_post.apply_rules(vals, metadata)
                    ext_object.rule_ids_post.apply_rules(vals, metadata)
                    self._prune_vals(vals, **metadata)
                    ext_object.write_odoo_object(vals, **metadata)

                resource.last_pull = datetime.now()

            if sync and deferred_create_data['vals']:
                self._pull_deferred_create(
                    metadata=metadata,
                    **deferred_create_data,
                )

        if prune:
            resource.prune_objects(foreign_objects)

    def _prune_vals(self, vals, processed_keys, **kw):
        self.ensure_one()
        if self.prune_vals:
            implicit_keys = set(vals.keys()) - set(processed_keys)
            for key in implicit_keys:
                vals.pop(key)

    def _pull_deferred_create(self, vals, data, object_vals, metadata):
        """At this point we are sure that we encountered a new external object,
        therefore there is no need to lookup existing object links.
        """

        # assertions
        self.ensure_one()
        assert({
            'model_model',
            'field_mapping_id',
            'resource_id',
        }.issubset(set(metadata.keys())))

        # creating records
        _logger.info(
            f"Creating {len(vals)} records in model {metadata['model_model']}"
        )
        records = self.env[metadata['model_model']].create(vals)

        # getting field mapping and resource
        field_mapping = self.env['external.data.field.mapping'].search([
            ('id', '=', metadata['field_mapping_id'])
        ], limit=1)
        resource = self.env['external.data.resource'].browse(
            metadata['resource_id'])

        # post processing
        ext_objects = self.env['external.data.object']
        post_rules = field_mapping.rule_ids_post
        if post_rules:
            model_model = field_mapping.model_id.model
            metadata.update({'pre_post': 'post'})
            data_count = len(data)
            i = 0
            while i < len(data_count):
                data = data[i]
                record = records[i]
                metadata.update({'record': record})
                vals = field_mapping.apply_mapping(data, metadata)
                post_rules.apply_rules(vals, metadata)
                ext_objects.sanitize_values(vals, model_model)
                record.write(vals)
                i += 1

        resource.last_pull = datetime.now()
        if self.operation != 'pull':
            return True

        # create external objects and object_links from record
        ext_objects = ext_objects.create(object_vals)
        ext_object_ids = ext_objects.ids
        record_ids = records.ids
        model_id = field_mapping.model_id.id
        object_link = self.env['external.data.object.link']
        object_vals_count = len(object_vals)
        i = 0
        while i < len(object_vals_count):
            object_link.create({
                'model_id': model_id,
                'record_id': record_ids[i],
                'object_ids': [Command.link(ext_object_ids[i])],
            })
            i += 1

        return True

    def button_details(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.strategy",
            "views": [[False, "form"]],
            "res_id": self.id,
        }
