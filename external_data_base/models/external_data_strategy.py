# coding: utf-8

from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command
from odoo.osv import expression
from odoo.addons.http_routing.models.ir_http import slugify_one
from odoo.exceptions import MissingError, UserError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataStrategy(models.Model):
    _name = 'external.data.strategy'
    _description = "External Data Strategy"
    _order = 'priority'

    name = fields.Char(required=True)
    slug = fields.Char(compute='_compute_slug', store=True)
    operation = fields.Selection(
        string="Operation",
        selection=[
            ('list', "list"),
            ('pull', "pull"),
            ('push', "push"),
            ('edit', "mass edit"),
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
        string="Parser/Serializer",
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
    batch_size = fields.Integer("Batch size", default=10)
    exposed = fields.Boolean("Exposed to REST")

    @api.depends('name')
    @api.onchange('name')
    def _compute_slug(self):
        for record in self:
            record.slug = slugify_one(record.name)

    def button_details(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.strategy",
            "views": [[False, "form"]],
            "res_id": self.id,
        }

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

    def pull_resource(self, resource_id, sync=False, prune=False,
                      debug=False):
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
        field_mappings_all = self.field_mapping_ids
        foreign_types = field_mappings_all.mapped('foreign_type_id')
        data_source_objects = data_source.object_ids
        object_data_generators = parser.parse(raw_data)
        foreign_objects = []
        debug_data, debug_metadata = {}, {}
        deferred_create_data = {}
        for foreign_type in foreign_types:
            if not foreign_type.field_ids:
                raise MissingError(
                    f"No fields defined for foreign type ID {foreign_type.id}")
            data_generator = object_data_generators.get(foreign_type.id)
            if not data_generator:
                continue
            resource.foreign_type_ids = [Command.link(foreign_type.id)]
            metadata.update({
                'foreign_type_id': foreign_type.id,
                'foreign_type_name': foreign_type.name,
                'foreign_id_key': foreign_type.field_ids[0].name,
                'now': datetime.now(),
                'record': False,
            })

            # executing db queries outside of the parsing loop when possible
            field_mappings = field_mappings_all.filtered(
                lambda m: m.foreign_type_id.id == foreign_type.id
            )
            external_objects = data_source_objects.filtered(
                lambda o: o.foreign_type_id.id == foreign_type.id
            )
            object_vals = {
                'data_source_id': data_source.id,
                'foreign_type_id': foreign_type.id,
                'resource_ids': [Command.link(resource_id)],
            }
            if debug:
                debug_data[foreign_type.name] = []
                debug_metadata[foreign_type.name] = []

            # map & process
            for index, data in enumerate(data_generator):
                foreign_id = data.get(metadata['foreign_id_key'])
                if not foreign_id:
                    msg = f"Missing foreign ID from resource {resource_name}"
                    _logger.error(msg)
                    continue
                metadata.update({
                    'index': index,
                    'foreign_id': foreign_id,
                    'processed_keys': [],
                })
                if debug:
                    foreign_type_name = metadata['foreign_type_name']
                    debug_data[foreign_type_name].append(data.copy())
                    debug_metadata[foreign_type_name].append(metadata.copy())
                if prune:
                    foreign_objects.append(
                        (metadata['foreign_type_id'], foreign_id))
                if not sync:
                    continue
                object_vals.update({
                    'foreign_id': foreign_id,
                    'priority': index,
                })

                for field_mapping in field_mappings:
                    metadata.update({
                        'field_mapping_id': field_mapping.id,
                        'model_id': field_mapping.model_id.id,
                        'model_model': field_mapping.model_id.model,
                    })
                    if metadata['operation'] == 'pull':
                        vals = self._pull(
                            resource, field_mapping,
                            external_objects, object_vals,
                            data, metadata,
                        )
                    elif metadata['operation'] == 'list':
                        vals = self._list(
                            resource, field_mapping,
                            external_objects, object_vals,
                            data, metadata,
                        )
                    else:
                        continue
                    if not vals:
                        continue

                    # deferred create
                    record = metadata.get('record')
                    if metadata['deferred_create'] and not record:
                        model_model = metadata['model_model']
                        dc_data = deferred_create_data.get(model_model)
                        if not dc_data:
                            dc_data = deferred_create_data[model_model] = {
                                'vals': [],
                                'data': [],
                                'object_vals': [],
                            }
                            dc_data.update(metadata)

                        dc_data['vals'].append(vals)
                        dc_data['data'].append(data.copy())
                        dc_data['object_vals'].append(object_vals.copy())

                    # post processing
                    postprocess_rules = metadata.get('postprocess_rules')
                    if record and postprocess_rules:
                        metadata.update(pre_post='post')
                        vals = field_mapping.apply_mapping(data, metadata)
                        postprocess_rules.apply_rules(vals, metadata)
                        if metadata.get('drop'):
                            if record and metadata.get('delete'):
                                record.unlink()
                            return True
                        self._prune_vals(vals, **metadata)
                        external_objects.sanitize_values(vals, **metadata)
                        metadata['record'].write(vals)

                resource.last_pull = datetime.now()

            if sync and deferred_create_data:
                for model, dc_data in deferred_create_data.items():
                    self._pull_deferred_create(model, **dc_data)

        if prune:
            resource.prune_objects(foreign_objects)
        if debug:
            return debug_data, debug_metadata

    def _pull(self, resource, field_mapping,
              external_objects, object_vals,
              data, metadata):
        self.ensure_one()

        foreign_id = metadata.get('foreign_id')
        resource_id = metadata.get('resource_id')
        if not (foreign_id and resource_id):
            return False

        # get record and external object
        record = ext_object = False
        for o in external_objects:
            if o.foreign_id == foreign_id:
                o.resource_ids = [Command.link(resource_id)]
                ext_object = o
                break
        if not (ext_object or metadata['deferred_create']):
            ext_object = external_objects.create(object_vals)
            external_objects += ext_object
        if ext_object.link_similar_objects(**metadata):
            record = metadata['record'] = ext_object._record(
                metadata['model_id'])

        # pre processing
        metadata.update(pre_post='pre')
        vals = field_mapping.apply_mapping(data, metadata)
        preprocess_rules = field_mapping.rule_ids_pre
        preprocess_rules += ext_object.rule_ids_pre
        preprocess_rules.apply_rules(vals, metadata)
        if metadata.get('drop'):
            if record and metadata.get('delete'):
                record.unlink()
            return False
        self._prune_vals(vals, **metadata)

        # return vals for deferred create
        if not record and metadata['deferred_create']:
            if external_objects.sanitize_values(vals, **metadata):
                return vals
            else:
                return False

        # write record
        ext_object.write_odoo_record(vals, **metadata)
        metadata['record'] = ext_object._record(metadata['model_id'])
        metadata['postprocess_rules'] = field_mapping.rule_ids_post
        metadata['postprocess_rules'] += ext_object.rule_ids_post

    def _list(self, resource, field_mapping,
              external_objects, object_vals,
              data, metadata):
        self.ensure_one()

        foreign_id = metadata.get('foreign_id')
        resource_id = metadata.get('resource_id')
        data_source_id = metadata.get('data_source_id')
        index = metadata.get('index')
        if not (foreign_id and resource_id and data_source_id):
            return False

        # get resource
        record = metadata['record'] = resource.search([
            ('url', '=', foreign_id),
        ], limit=1)  # TODO: check if found more than one
        if record:
            res_last_mod = record.last_mod  # for later use
            if not res_last_mod:
                return False

        # pre processing
        metadata.update(pre_post='pre')
        vals = field_mapping.apply_mapping(data, metadata)
        vals.update(data_source_id=data_source_id)
        if not metadata.get('processed_keys'):
            metadata['processed_keys'] = []
        metadata['processed_keys'].append('resource_id')
        field_mapping.rule_ids_pre.apply_rules(vals, metadata)
        if metadata.get('drop'):
            if record and metadata.get('delete'):
                record.unlink()
            return False
        self._prune_vals(vals, **metadata)

        # return vals for deferred create
        if not record and metadata['deferred_create']:
            if external_objects.sanitize_values(vals, **metadata):
                return vals
            else:
                return False

        # write resource
        vals_last_mod = vals.get('last_mod')
        if not record and external_objects.sanitize_values(
                vals, **metadata):
            metadata['record'] = resource.create(vals)
        elif vals_last_mod and res_last_mod < vals_last_mod:
            msg = f"Updating resource #{index}: {foreign_id}"
            _logger.debug(msg)
            record.write(vals)

        metadata['postprocess_rules'] = field_mapping.rule_ids_post

    @api.model
    def _prune_vals(self, vals, processed_keys, prune_implicit=True, **kw):
        if prune_implicit:
            implicit_keys = set(vals.keys()) - set(processed_keys)
            for key in implicit_keys:
                vals.pop(key)

    def _pull_deferred_create(self, model_model, vals, data, object_vals,
                              metadata):
        """At this point we are sure that we encountered a new external object,
        therefore there is no need to lookup existing object links.
        """
        self.ensure_one()
        for key in ['field_mapping_id', 'resource_id']:
            assert isinstance(metadata.get(key), int)

        # creating records
        _logger.info(f"Creating {len(vals)} records in model {model_model}")
        records = self.env[model_model].create(vals)

        # getting field mapping and resource
        field_mapping = self.env['external.data.field.mapping'].browse(
            metadata['field_mapping_id']).exists()
        resource = self.env['external.data.resource'].browse(
            metadata['resource_id']).exists()
        if not (field_mapping and resource):
            _logger.error("Deferred create is not possible, missing metadata")
            return False

        # post processing
        ext_objects = self.env['external.data.object']
        post_rules = field_mapping.rule_ids_post
        if post_rules:
            metadata.update({'pre_post': 'post'})
            for i, data_i in enumerate(data):
                record = metadata['record'] = records[i]
                vals = field_mapping.apply_mapping(data_i, metadata)
                post_rules.apply_rules(vals, metadata)
                ext_objects.sanitize_values(vals, model_model)
                record.write(vals)

        resource.last_pull = datetime.now()
        if self.operation != 'pull':
            return True

        # create external objects and object_links from record
        record_ids = records.ids
        model_id = metadata['model_id']
        object_link = self.env['external.data.object.link']
        obj_id, foreign_id = foreign_type_id = 0
        for i, o_vals in enumerate(object_vals):
            if (
                o_vals['foreign_id'] != foreign_id or
                o_vals['foreign_type_id'] != foreign_type_id
            ):
                obj_id = ext_objects.create(o_vals).id
            foreign_id = o_vals['foreign_id']
            foreign_type_id = o_vals['foreign_type_id']

            object_link.create({
                'model_id': model_id,
                'record_id': record_ids[i],
                'object_ids': [Command.link(obj_id)],
            })

        return True

    def mass_edit(self, field_mapping_id=False, debug=False):
        self.ensure_one()
        if self.operation != 'edit':
            raise UserError(f"Wrong operation type for edit: {self.operation}")

        debug_data = []
        metadata = {'updated_ids': []}
        for vals in self._gather_items(metadata=metadata):
            model_model = metadata.get('model_model')
            res_id = vals.get('id')
            try:
                record = self.env[model_model].browse(res_id).exists()
            except Exception as e:
                _logger.error(f"An error occured while retrieving record: {e}")
                continue
            if not record:
                _logger.error(
                    f"Could not find record: ({model_model}, {res_id})")
                continue

            self.env['external.data.object'].sanitize_values(vals, **metadata)
            metadata['updated_ids'].append(res_id)

            if debug:
                debug_data.append({
                    'before': record.read(vals.keys()),
                    'after': vals,
                })
            else:
                record.write(vals)

        if debug:
            return debug_data

    def push(self, field_mapping_id=False):
        self.ensure_one()
        if self.operation != 'push':
            raise UserError(f"Wrong operation type for push: {self.operation}")

        batch = []  # TODO: gather batch if possible, send if filled or no more
        metadata = {}
        for vals in self._gather_items(metadata=metadata):
            data = self.serializer_id.serialize(vals)
            result = self.transporter_id.deliver(data)
            # TODO: refresh resources, external objects from result

    def _gather_items(self, metadata, res_id=False, limit=None, offset=0,
                      prune_implicit=None):
        self.ensure_one()
        mapping = self.field_mapping_ids[0]

        # get recordset
        domain_str = mapping.filter_domain
        if domain_str:
            domain = expression.normalize_domain(eval(domain_str))
        else:
            domain = []
        if res_id:
            domain.append(('id', '=', res_id))
        if not limit:
            limit = self.batch_size
        records = self.env[mapping.model_model].search(
            domain, limit=limit, offset=offset)
        if not records:
            raise UserError("No records found")

        if isinstance(prune_implicit, type(None)):
            prune_implicit = mapping.prune_vals
        metadata.update({
            'field_mapping_id': mapping.id,
            'model_id': mapping.model_id.id,
            'model_model': mapping.model_id.model,
            'now': datetime.now(),
            'operation': self.operation,
            'pre_post': 'pre',
            'prune_implicit': prune_implicit,
            'prune_false': mapping.prune_vals,  # TODO: like prune_implicit
        })
        foreign_type = mapping.foreign_type_id
        if foreign_type:
            metadata.update({
                'foreign_type_id': foreign_type.id,
                'foreign_type_name': foreign_type.name,
                'foreign_id_key': foreign_type.field_ids[0].name,
            })
        for data in records:
            metadata['record'] = data
            vals = mapping.apply_mapping(data, metadata)
            mapping.rule_ids_pre.apply_rules(vals, metadata)
            if metadata.get('drop'):
                continue

            self._prune_vals(vals, **metadata)
            yield vals.copy()
