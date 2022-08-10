# coding: utf-8

from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command
from odoo.exceptions import MissingError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataSource(models.Model):
    _name = 'external.data.source'
    _description = "External Data Source"

    name = fields.Char(required=True)
    data_source_type_id = fields.Reference(
        string="Data source type",
        selection='_selection_data_source_type',
    )
    batch_size = fields.Integer("Batch size", default=10)
    last_fetch = fields.Datetime("Last fetched")
    field_mapping_ids = fields.One2many(
        'external.data.field.mapping',
        inverse_name='data_source_id',
        string="Field mappings",
    )
    package_ids = fields.One2many(
        'external.data.package',
        inverse_name='data_source_id',
        string="Packages",
    )
    fetch_limit = fields.Integer(default=0)

    @api.model
    def _selection_data_source_type(self):
        return []

    def fetch_package_data(self):
        self.ensure_one()
        if not self.data_source_type_id:
            raise MissingError("Source type is not set!")

        _logger.info(f"Fetching packages from data source {self.name}")
        packages = self.data_source_type_id.fetch_package_data()
        # first fetch
        if not self.last_fetch and not self.package_ids:
            _logger.info(
                f"Creating package objects for data source {self.name}"
            )
            self.package_ids = [Command.create(p_data) for p_data in packages]
            self.last_fetch = datetime.now()
            return True

        # It can be a very long loop, getting object beforehand when possible
        _logger.info("Syncing package data...")
        model_package = self.env['external.data.package']
        self_id = self.id
        self_name = self.name
        data_source_type = self.data_source_type_id
        last_fetch = self.last_fetch
        new_packages = []
        i = 0
        limit = self.fetch_limit
        for p_data in packages:
            i += 1
            # Don't process old data
            last_mod = p_data.get('last_mod')
            if not last_mod:
                continue

            p_name = p_data.get('name')
            if not p_name:
                p_name = data_source_type.get_package_name(p_data)

            package = model_package.search([
                ('data_source_id', '=', self_id),
                ('name', '=', p_name),
                ('last_mod', '!=', last_mod),
            ], limit=1)
            # TODO: deduplication later...
            if package:
                msg = "Updating package object #{nr} of data source {ds}: {p}"
                _logger.info(msg.format(nr=i, ds=self_name, p=p_name))
                package.write(p_data)
            else:
                new_packages.append(p_data)
            if i == limit:
                break

        self.package_ids = [Command.create(p_data) for p_data in new_packages]
        self.last_fetch = datetime.now()
        return True

    def pull_package(self, package_id):
        self.ensure_one()
        package = self.package_ids.browse(package_id)
        if not package.exists():
            raise MissingError(f"Package with ID {package_id} doesn't exist")
        if not self.data_source_type_id:
            raise MissingError("Source type is not set!")
        if not hasattr(self.data_source_type_id, 'pull_package'):
            raise MissingError(
                "Pull is not available for data source "
                f"type '{self.data_source_type_id.name}'"
            )
        return self.data_source_type_id.pull_package(package.name)

    def batch_pull(self, filter_lambda=None, sync=False, prune=False):
        packages = self.package_ids.filtered(
            lambda p: not p.skip and (
                not p.last_pull or
                (p.last_mod and p.last_pull and p.last_mod > p.last_pull)
                # TODO: or (not p.last_mod and p.last_pull + week < now)
            )
        )
        if callable(filter_lambda):
            packages = packages.filtered(filter_lambda)
        return packages.batch_pull(
            batch_size=self.batch_size,
            sync=sync, prune=prune,
        )


class ExternalDataPackage(models.Model):
    _name = 'external.data.package'
    _description = "External Data Package"

    name = fields.Char(required=True)
    priority = fields.Float("Priority")
    skip = fields.Boolean()
    notes = fields.Text()
    last_mod = fields.Datetime("Last modification")
    last_pull = fields.Datetime("Last pull")
    last_push = fields.Datetime("Last push")
    valid_until = fields.Datetime("Valid until")
    data_source_id = fields.Many2one(
        'external.data.source',
        ondelete='cascade',
        string="Data source",
        required=True,
    )
    field_mapping_ids = fields.Many2many(
        comodel_name='external.data.field.mapping',
        string="Type mappings",
    )
    object_ids = fields.Many2many(
        comodel_name='external.data.object',
        string="External objects",
    )
    # TODO: Language

    def toggle_skip(self):
        for record in self:
            record.skip = not record.skip

    def batch_pull(self, sync=False, prune=False, batch_size=1, do_all=False):
        res = []
        i = 0
        for package in self:
            try:
                res.append(package.pull(sync=sync, prune=prune))
            except Exception as e:
                _logger.error(e)
                package.notes = ("Pull error:\n" + str(e))
                package.skip = True
            i += 1
            if i == batch_size and not do_all:
                break
        return res

    def pull(self, sync=False, prune=False):
        self.ensure_one()
        _logger.info(f"Pulling package {self.name}")
        dataset = self.data_source_id.pull_package(self.id)

        # find foreign_types
        found_foreign_type_names = list(dataset.keys())
        field_mappings = self.field_mapping_ids.search([
            ('data_source_id', '=', self.data_source_id.id),
            ('foreign_type_id.name', 'in', found_foreign_type_names),
        ])
        if field_mappings:
            self.field_mapping_ids = [Command.set(field_mappings.ids)]

        metadata = {
            'package_id': self.id,
            'direction': 'pull',
        }
        foreign_objects = []
        for field_mapping in field_mappings:
            foreign_type = field_mapping.foreign_type_id.name
            foreign_id_key = field_mapping.foreign_id_field_id.name
            index = 0
            for data in dataset[foreign_type]:
                index += 1
                foreign_id = data.get(foreign_id_key)
                if not foreign_id:
                    _logger.error(
                        f"Missing foreign ID from package {self.name}"
                    )
                    continue
                foreign_objects.append((foreign_id, field_mapping.id))
                if not sync:
                    continue

                # get external_object
                external_object = self.object_ids.search([
                    ('field_mapping_id', '=', field_mapping.id),
                    ('foreign_id', '=', foreign_id),
                ], limit=1)
                if not external_object:
                    external_object = self.object_ids.create({
                        'field_mapping_id': field_mapping.id,
                        'foreign_id': foreign_id,
                        'priority': index,
                    })
                    external_object.find_and_set_object_link_id()
                if self.id not in external_object.package_ids.ids:
                    external_object.package_ids = [Command.link(self.id)]

                # pre processing
                metadata.update({
                    'field_mapping_id': field_mapping.id,
                    'foreign_type': foreign_type,
                    'foreign_id': foreign_id,
                    'record': external_object.object_link_id._record(),
                    'pre_post': 'pre',
                })
                vals = field_mapping.apply_mapping(data, metadata)
                field_mapping.rule_ids_pre.apply_rules(vals, metadata)
                external_object.rule_ids_pre.apply_rules(vals, metadata)
                external_object.write_odoo_object(vals, metadata)

                # post processing
                metadata.update({
                    'record': external_object.object_link_id._record(),
                    'pre_post': 'post',
                })
                vals = field_mapping.apply_mapping(data, metadata)
                field_mapping.rule_ids_post.apply_rules(vals, metadata)
                external_object.rule_ids_post.apply_rules(vals, metadata)
                external_object.write_odoo_object(vals, metadata)

                self.last_pull = datetime.now()

        if prune:
            self.prune_objects(foreign_objects)

        return dataset

    def prune_objects(self, foreign_objects):
        self.ensure_one()
        found_object_ids = []
        for foreign_id, field_mapping_id in self.object_ids:
            found_object_ids += self.object_ids.filtered(lambda o: (
                o.foreign_id == foreign_id and
                o.field_mapping_id == field_mapping_id
            )).ids
        unrelated_object_ids = set(self.object_ids.ids) - set(found_object_ids)
        self.object_ids = [Command.unlink(i) for i in unrelated_object_ids]
        # TODO: delete orphan objects

    def button_open(self):
        self.ensure_one()
        res_id = self.env.context.get('default_res_id')
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.package",
            "views": [[False, "form"]],
            "res_id": res_id,
        }
