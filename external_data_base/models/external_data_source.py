# coding: utf-8

import logging

from odoo import api, fields, models
from odoo.fields import Command
from odoo.addons.http_routing.models.ir_http import slugify_one
from odoo.exceptions import MissingError

_logger = logging.getLogger(__name__)


class ExternalDataSource(models.Model):
    _name = 'external.data.source'
    _description = "External Data Source"

    name = fields.Char(required=True)
    slug = fields.Char(compute='_compute_slug', store=True)
    list_strategy_id = fields.Many2one(
        'external.data.strategy',
        string="List strategy",
        compute='_compute_list_strategy_id',
    )
    list_strategy_ids = fields.One2many(
        'external.data.strategy',
        string="List strategies",
        inverse_name='data_source_id',
        domain=[('operation', '=', 'list')],
    )
    pull_strategy_ids = fields.One2many(
        'external.data.strategy',
        string="Pull strategies",
        inverse_name='data_source_id',
        domain=[('operation', '=', 'pull')],
    )
    push_strategy_ids = fields.One2many(
        'external.data.strategy',
        string="Push strategies",
        inverse_name='data_source_id',
        domain=[('operation', '=', 'push')],
    )
    rest_strategy_ids = fields.One2many(
        'external.data.strategy',
        string="Push strategies",
        inverse_name='data_source_id',
        domain=[('operation', '=', 'rest')],
    )
    edit_strategy_ids = fields.One2many(
        'external.data.strategy',
        string="Push strategies",
        inverse_name='data_source_id',
        domain=[('operation', '=', 'edit')],
    )
    field_mapping_ids = fields.One2many(
        'external.data.field.mapping',
        inverse_name='data_source_id',
        string="Field mappings",
    )
    last_fetch = fields.Datetime("Last fetched")
    resource_ids = fields.One2many(
        'external.data.resource',
        inverse_name='data_source_id',
        string="Resources",
    )
    object_ids = fields.One2many(
        'external.data.object',
        inverse_name='data_source_id',
        string="Objects",
    )
    fetch_limit = fields.Integer(default=0)
    strategy_count = fields.Integer(compute='_compute_strategy_count')

    @api.depends('name')
    @api.onchange('name')
    def _compute_slug(self):
        for record in self:
            record.slug = slugify_one(record.name)

    @api.depends('list_strategy_ids')
    def _compute_list_strategy_id(self):
        for ds in self:
            if ds.list_strategy_ids:
                ds.list_strategy_id = ds.list_strategy_ids[0].id
            else:
                ds.list_strategy_id = False

    def _compute_strategy_count(self):
        for record in self:
            record.strategy_count = record.list_strategy_ids.search_count([
                ('data_source_id', '=', record.id)
            ])

    def list(self):
        self.ensure_one()
        if self.list_strategy_id:
            self.list_strategy_id.list()
        else:
            raise MissingError("No list strategy defined")

    def batch_pull(self, strategy_id=False, sync=False, prune=False):
        res_ids = self.resource_ids.filtered(
            lambda p: not p.skip and (
                not p.last_pull or
                (p.last_mod and p.last_pull and p.last_mod > p.last_pull)
                # TODO: or (not p.last_mod and p.last_pull + week < now)
            )
        ).ids
        # getting strategy
        strategy = self.env['external.data.strategy']
        if strategy_id:
            strategy = strategy.browse(strategy_id)
        if not strategy.exists():
            strategy = strategy.get_strategy(
                operation='pull',
                data_source_id=self.id,
            )

        if strategy:
            strategy.batch_pull(res_ids, do_all=True, sync=sync, prune=prune)


class ExternalDataResource(models.Model):
    _name = 'external.data.resource'
    _description = "External Data Resource"

    name = fields.Char(required=True)
    url = fields.Char(required=True)
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
    foreign_type_ids = fields.Many2many(
        comodel_name='external.data.type',
        string="Foreign types",
    )
    object_ids = fields.Many2many(
        comodel_name='external.data.object',
        string="External objects",
    )
    object_relation_ids = fields.One2many(
        'external.data.object.relation',
        string="Related objects",
        inverse_name='resource_id',
    )
    # TODO: Language

    def toggle_skip(self):
        for record in self:
            record.skip = not record.skip

    # move all operation logic to strategies
    def pull(self, strategy_id=False, sync=False, prune=False):
        self.ensure_one()
        strategy = self.env['external.data.strategy']
        if strategy_id:
            strategy = strategy.browse(strategy_id)
        if not strategy.exists():
            if self.env['external.data.field.mapping'].search_count([
                ('model_id.model', '=', 'external.data.resource'),
                ('data_source_id', '=', self.data_source_id.id),
                ('foreign_type_id', 'in', self.foreign_type_ids.ids),
            ]):
                operation = 'list'
                _logger.warning("Found list field mapping, switch to listing.")
            else:
                operation = 'pull'

            strategy = strategy.get_strategy(
                operation=operation,
                resource_ids=self.ids,
            )
        if strategy:
            strategy.pull_resource(self.id, sync=sync, prune=prune)

    def batch_pull(self, strategy_id=False, sync=False, prune=False):
        strategy = self.env['external.data.strategy']
        if strategy_id:
            strategy = strategy.browse(strategy_id)
        if not strategy.exists():
            strategy = strategy.get_strategy(
                operation='pull',
                resource_ids=self.ids,
            )
        if strategy:
            strategy.batch_pull(self.ids, do_all=True, sync=sync, prune=prune)

    def prune_objects(self, foreign_objects):
        self.ensure_one()
        found_object_ids = []
        for foreign_type_id, foreign_id in self.object_ids:
            found_object_ids += self.object_ids.filtered(lambda o: all([
                o.foreign_type_id == foreign_type_id,
                o.foreign_id == foreign_id,
            ])).ids
        unrelated_object_ids = set(self.object_ids.ids) - set(found_object_ids)
        self.object_ids = [Command.unlink(i) for i in unrelated_object_ids]
        # TODO: delete orphan objects

    def button_open(self):
        self.ensure_one()
        res_id = self.env.context.get('default_res_id')
        return {
            "type": "ir.actions.act_window",
            "res_model": "external.data.resource",
            "views": [[False, "form"]],
            "res_id": res_id,
        }

    def visit_url(self):
        self.ensure_one()
        return {
            "type": "ir.actions.act_url",
            "url": self.url,
            "target": "new",
        }
