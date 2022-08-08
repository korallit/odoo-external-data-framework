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
        string="Scraper",
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
    shared_field_mapping_ids = fields.Many2many(
        'external.data.field.mapping',
        string="Shared field mappings",
        domain=[('data_source_id', '!=', id)],
    )

    @api.model
    def _selection_data_source_type(self):
        return []

    def fetch_package_data(self):
        self.ensure_one()
        if not self.data_source_type_id:
            raise MissingError("Source type is not set!")

        _logger.info(f"Fetching packages from data source {self.name}")
        package_dataset = self.data_source_type_id.fetch_package_data()
        if not self.last_fetch and not self.package_ids:
            _logger.info(
                f"Creating package objects for data source {self.name}"
            )
            self.package_ids = [
                Command.create(package_data)
                for package_data in package_dataset
            ]

        _logger.info("Syncing package data...")
        for p_data in package_dataset:
            p_name = p_data.get('name')
            if not p_name:
                p_name = self.data_source_type_id.get_package_name(p_data)
            package = self.package_ids.filtered(lambda p: p.name == p_name)
            if len(package) > 1:
                _logger.warning(
                    f"Multiple packages {package.ids} has the same name: "
                    f"{p_name}, selecting the first."
                )
                package = package[0]
            if package:
                package.write(p_data)
            else:
                _logger.info(
                    f"Creating package object '{p_name}' "
                    f"for data source {self.name}"
                )
                self.package_ids = [Command.create(p_data)]

        self.last_fetch = datetime.now()

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
                f"type '{data_source_type_id.name}'"
            )
        _logger.info(f"Pulling package {package.name}")
        return self.data_source_type_id.pull_package(package.name)

    def batch_pull(self, filter_lambda=None, sync=False, prune=False):
        filter_changed = lambda p: not p.skip and (
            not p.last_pull or
            (p.last_mod and p.last_pull and p.last_mod > p.last_pull)
            # TODO: or (not p.last_mod and p.last_pull + week < now)
        )

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
        found_foreign_types = dataset.keys()
        field_mappings = self.field_mapping_ids.search([
            ('data_source_id', '=', self.data_source_id.id),
            ('foreign_type', 'in', found_foreign_types),
        ])
        if field_mappings:
            self.field_mapping_ids = [Command.set(field_mappings.ids)]

        metadata = {
            'package_id': self.id,
            'direction': pull,
        }
        foreign_ids = []
        for field_mapping in field_mappings:
            index = 0
            for data in dataset[field.mapping.foreign_type]:
                index += 1
                foreign_id = data.get(field_mapping.foreign_id_key)
                if not foreign_id:
                    _logger.error(
                        f"Missing foreign ID from package {self.name}"
                    )
                    continue
                foreign_ids.append(foreign_id)
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
                if self.id not in external_object.package_ids:
                    external_object.package_ids = [Command.link(self.id)]

                metadata.update({
                    'field_mapping_id': field_mapping.id,
                    'foreign_type': field_mapping.foreign_type,
                    'foreign_id': foreign_id,
                    'odoo_model': field_mapping.model_id.model,
                    'odoo_id': external_object.object_link_id.record_id,
                })
                # pre processing
                vals = field_mapping.apply_mapping(data)
                field_mapping.rule_ids_pre.apply_rules(vals, metadata)
                external_object.rule_ids_pre.apply_rules(vals, metadata)
                external_object.write_odoo_object(vals)
                metadata.update(odoo_id=external_object.object_link_id.record_id)

                # post processing
                vals = field_mapping.apply_mapping(data)
                field_mapping.rule_ids_post.apply_rules(vals, metadata)
                external_object.rule_ids_post.apply_rules(vals, metadata)
                external_object.write_odoo_object(vals)

                self.last_pull = datetime.now()

        if prune:
            self.prune_objects(foreign_ids)

        return dataset

    def prune_objects(self, foreign_ids):
        self.ensure_one()
        unrelated_objects = self.object_ids.filtered(
            lambda r: r.foreign_id not in foreign_ids
        )
        # TODO: delete orphan objects
        self.object_ids = [Command.unlink(i) for i in unrelated_objects.ids]

    def button_open(self):
        self.ensure_one()
        res_id = self.env.context.get('default_res_id')
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.package",
            "views": [[False, "form"]],
            "res_id": res_id,
        }
