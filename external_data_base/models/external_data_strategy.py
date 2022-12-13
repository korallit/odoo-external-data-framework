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
    resource_ids = fields.Many2many(
        'external.data.resource',
        string="Resources",
        domain="[('data_source_id', '=', data_source_id)]",
    )
    batch_size = fields.Integer("Batch size", default=10)
    offset = fields.Integer("Page", help="offset", default=0)
    exposed = fields.Boolean("Exposed to REST")
    export_filename = fields.Char(
        "Export filename",
        default="export",
    )
    export_url = fields.Char(
        string="Export URL",
        comute='_compute_export_url',
        store=True,
    )

    @api.depends('name')
    @api.onchange('name')
    def _compute_slug(self):
        for record in self:
            record.slug = slugify_one(record.name)

    @api.depends('export_filename', 'slug')
    @api.onchange('export_filename', 'slug')
    def _compute_export_url(self):
        for record in self:
            path_parts = [
                "external-data", "web",
                record.data_source.slug,
                record.slug, "items",
                record.export_filename,
            ]
            if all(path_parts):
                url = '/'.join(path_parts)
                params = []
                if record.batch_size:
                    params.append("page_size=" + str(record.batch_size))
                if record.offset:
                    params.append("page=" + str(record.offset))
                if params:
                    url += "?" + '&'.join(params)
                record.export_url = url

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
        if len(self.resource_ids) == 1:
            self.pull_resource(self.resource_ids.id, sync=True, prune=True)
        elif len(self.resource_ids) > 1:
            self.batch_pull(self.resource_ids.ids, sync=True, prune=True)
        else:
            raise MissingError("No resources defined for this lister")

    def batch_pull(self, resource_ids, sync=False, prune=False,
                   do_all=False, batch_size=False):

        if not batch_size:
            batch_size = self.batch_size
        for i, res_id in enumerate(resource_ids):
            try:
                self.pull_resource(res_id, sync=sync, prune=prune)
            except Exception as e:
                _logger.error(e)
                resource = self.env['external.data.resource'].browse(res_id)
                if resource.exists():
                    resource.notes = ("Pull error:\n" + str(e))
                    resource.skip = True
            if i == batch_size and not do_all:
                break

    def pull_resource(self, resource_id, sync=False, prune=False, debug=False):
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

        # extract & parse
        data_source = self.data_source_id
        parser = self.serializer_id
        processed_data = parser.extraxt(raw_data)

        metadata = {  # TODO: could it be the context?
            'operation': self.operation,
            'deferred_create': self.deferred_create,
            'data_source_id': data_source.id,
            'resource_id': resource_id,
            'resource_name': resource_name,
            'strategy_id': self.id,
            'strategy_name': self.name,
            'parser_id': parser.id,
            'sync': sync,
            'prune': prune,
            'debug': debug,
            'keep': {},
        }
        if self.operation == 'list':
            metadata['resources'] = self.data_source_id.resource_ids
        field_mappings_all = self.field_mapping_ids
        foreign_types = field_mappings_all.mapped('foreign_type_id')
        object_data_generators = parser.parse(processed_data)
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

            # executing db queries outside of the parsing loop when possible
            field_mappings = field_mappings_all.filtered(
                lambda m: m.foreign_type_id.id == foreign_type.id
            )
            external_objects = self.env['external.data.object'].search([
                ('data_source_id', '=', data_source.id),
                ('foreign_type_id', '=', foreign_type.id),
            ])
            metadata.update({
                'foreign_type_id': foreign_type.id,
                'foreign_type_name': foreign_type.name,
                'foreign_id_key': foreign_type.field_ids[0].name,
                'external_objects': external_objects,
                'now': datetime.now(),
                'record': False,
            })
            if debug:
                debug_data[foreign_type.name] = []
                debug_metadata[foreign_type.name] = []

            # map & process
            for index, data in enumerate(data_generator):
                metadata['index'] = index
                if not data:
                    continue
                if debug:
                    type_name = metadata['foreign_type_name']
                    debug_data[type_name].append(data.copy())
                    debug_metadata[type_name].append(metadata.copy())
                    continue
                for field_mapping in field_mappings:
                    foreign_id = data.get(metadata['foreign_id_key'])
                    if not foreign_id:
                        msg = "Missing foreign ID from resource {}".format(
                            metadata.get('resource_name'))
                        _logger.error(msg)
                        continue
                    metadata['obj_link_variant_tag'] = \
                        field_mapping.object_link_variant_tag
                    metadata['foreign_id'] = foreign_id
                    if prune:
                        foreign_objects.append(
                            (metadata['foreign_type_id'], foreign_id))
                    if sync:
                        self._pull_mapping(
                            data, metadata, deferred_create_data,
                            field_mapping,
                        )
                if sync:
                    resource.last_pull = datetime.now()
                    resource.foreign_type_ids = [
                        Command.link(metadata['foreign_type_id'])]
                if not ((index + 1) % 100):  # don't want to log #0
                    _logger.info(f"Processing object #{index + 1}")
            if sync and deferred_create_data:
                for model, dc_data in deferred_create_data.items():
                    self._pull_deferred_create(model, **dc_data)

        if prune:
            resource.prune_objects(foreign_objects)
        if debug:
            return debug_data, debug_metadata

    @api.model
    def _append_deferred_create_data(self, vals, data, metadata, dc_data):
        model_model = metadata['model_model']
        dc_data_model = dc_data.get(model_model)
        if not dc_data_model:
            dc_data_model = dc_data[model_model] = {
                'vals': [],
                'data': [],
                'object_vals': [],
                'metadata': metadata.copy(),
            }
        dc_data_model['vals'].append(vals.copy())
        dc_data_model['data'].append(data.copy())
        if metadata.get('object_vals'):
            dc_data_model['object_vals'].append(metadata['object_vals'])

    @api.model
    def _pull_mapping(self, data, metadata, dc_data, field_mapping):
        metadata.update({
            'field_mapping_id': field_mapping.id,
            'model_id': field_mapping.model_id.id,
            'model_model': field_mapping.model_id.model,
            'processed_keys': [],
        })
        vals = False
        if metadata['operation'] == 'pull':
            vals = self._pull(field_mapping, data, metadata)
        elif metadata['operation'] == 'list':
            vals = self._list(field_mapping, data, metadata)

        # deferred create
        record = metadata.get('record')
        if metadata['deferred_create'] and vals and not record:
            self._append_deferred_create_data(vals, data, metadata, dc_data)
            return True

        # post processing
        postprocess_rules = metadata.get('postprocess_rules')
        if record and postprocess_rules:
            metadata.update({
                'pre_post': 'post',
                'processed_keys': [],
            })
            vals = field_mapping.apply_mapping(data, metadata)
            postprocess_rules.apply_rules(vals, metadata)
            if metadata.get('drop'):
                if record and metadata.get('delete'):
                    record.unlink()
                metadata.pop('drop')
                return True
            self._prune_vals(vals, **metadata)
            if vals:
                metadata['external_objects'].sanitize_values(vals, **metadata)
                if not field_mapping.skip_write:
                    metadata['record'].write(vals)

    @api.model
    def _pull(self, field_mapping, data, metadata):
        foreign_id = metadata.get('foreign_id')
        resource_id = metadata.get('resource_id')
        if not (foreign_id and resource_id):
            return False

        object_vals = {
            'data_source_id': metadata['data_source_id'],
            'foreign_type_id': metadata['foreign_type_id'],
            'resource_ids': [Command.link(metadata['resource_id'])],
            'foreign_id': foreign_id,
            'priority': metadata['index'],
        }
        metadata['object_vals'] = object_vals.copy()  # for deferred create too
        # get record and external object
        variant_tag = metadata.get('obj_link_variant_tag', False)
        record = ext_object = False
        for o in metadata['external_objects']:
            if o.foreign_id == foreign_id:
                o.resource_ids = [Command.link(resource_id)]
                ext_object = o
                break
        if not (ext_object or metadata['deferred_create']):
            ext_object = metadata['external_objects'].create(object_vals)
            metadata['external_objects'] += ext_object

        # looking for record created by other data sources
        if ext_object.link_similar_objects(**metadata):
            record = metadata['record'] = \
                ext_object._record(metadata['model_id'], variant_tag)
        metadata['external_object_id'] = ext_object.id

        # pre processing
        metadata.update(pre_post='pre')
        vals = field_mapping.apply_mapping(data, metadata)
        preprocess_rules = field_mapping.rule_ids_pre
        preprocess_rules += ext_object.rule_ids_pre
        preprocess_rules.apply_rules(vals, metadata)
        if metadata.get('drop'):
            if record and metadata.get('delete'):
                record.unlink()
            metadata.pop('drop')
            return False
        self._prune_vals(vals, **metadata)

        # looking for record with the same name and type if name is unique
        if field_mapping.name_is_unique:
            metadata['search_link_by_name'] = vals.get('name')
            if ext_object.link_similar_objects(**metadata):
                record = metadata['record'] = \
                    ext_object._record(metadata['model_id'], variant_tag)
        else:
            metadata['search_link_by_name'] = False

        # return vals for deferred create
        if not record and metadata['deferred_create']:
            if metadata['external_objects'].sanitize_values(vals, **metadata):
                return vals
            else:
                return False

        # write record
        if vals and not field_mapping.skip_write:
            ext_object.write_odoo_record(vals, metadata)
            metadata['record'] = ext_object._record(
                metadata['model_id'], variant_tag)
        metadata['postprocess_rules'] = field_mapping.rule_ids_post
        metadata['postprocess_rules'] += ext_object.rule_ids_post
        return vals

    @api.model
    def _list(self, field_mapping, data, metadata):
        foreign_id = metadata.get('foreign_id')
        resource_id = metadata.get('resource_id')
        data_source_id = metadata.get('data_source_id')
        index = metadata.get('index')
        if not (foreign_id and resource_id and data_source_id):
            return False

        # get resource
        resource = False
        for res in metadata['resources']:
            if res.url == foreign_id:
                resource = metadata['record'] = res
                break
        if resource:
            metadata['resources'] -= resource
            res_last_mod = resource.last_mod  # for later use
            if not res_last_mod:
                return False

        # pre processing
        metadata.update(pre_post='pre')
        vals = field_mapping.apply_mapping(data, metadata)
        vals.update(data_source_id=data_source_id)
        if not metadata.get('processed_keys'):
            metadata['processed_keys'] = []
        metadata['processed_keys'].append('data_source_id')
        field_mapping.rule_ids_pre.apply_rules(vals, metadata)
        if metadata.get('drop'):
            if resource and metadata.get('delete'):
                resource.unlink()
            metadata.pop('drop')
            return False
        self._prune_vals(vals, **metadata)

        # return vals for deferred create
        if not resource and metadata['deferred_create']:
            if metadata['external_objects'].sanitize_values(vals, **metadata):
                return vals
            else:
                return False

        # write resource
        vals_last_mod = vals.get('last_mod')
        if not resource and metadata['external_objects'].sanitize_values(
                vals, **metadata):
            metadata['record'] = metadata['resources'].create(vals)
        elif vals_last_mod and res_last_mod < vals_last_mod:
            msg = f"Updating resource #{index}: {foreign_id}"
            _logger.debug(msg)
            resource.write(vals)

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
        if self.operation != 'pull' or not object_vals:
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

        # TODO: paginated resource
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

        if prune_implicit is None:
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
            # reset processed_keys
            metadata['processed_keys'] = []
            vals = mapping.apply_mapping(data, metadata)
            mapping.rule_ids_pre.apply_rules(vals, metadata)
            if metadata.get('drop'):
                metadata.pop('drop')
                continue

            self._prune_vals(vals, **metadata)
            yield vals.copy()
