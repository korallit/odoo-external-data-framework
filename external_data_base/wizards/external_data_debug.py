# coding: utf-8

import json
from datetime import datetime

from odoo import api, fields, models
from odoo.osv import expression
from odoo.exceptions import MissingError, ValidationError


class ExternalDataDebugWizard(models.TransientModel):
    _name = 'external.data.debug.wizard'
    _description = "External Data Debug Wizard"

    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        string="Field mapping",
    )
    resource_id = fields.Many2one(
        'external.data.resource',
        string="Resource",
    )
    strategy_id = fields.Many2one(
        'external.data.strategy',
        string="Strategy",
    )
    sub_operation = fields.Selection(
        string="Operation method",
        selection=[
            ('parse', "parse"),
            ('map', "map"),
        ],
        default='map',
    )
    operation = fields.Selection(
        selection=[
            ('pull', "pull"),
            ('push', "push"),
            ('edit', "mass edit")
        ],
        default='pull',
    )
    debug = fields.Boolean(default=True)
    prune = fields.Boolean(default=True)
    sanitize = fields.Boolean(default=True)
    sanitize_prune_false = fields.Boolean(default=True)
    pre_post = fields.Selection(
        string="pre/post",
        selection=[
            ('pre', "pre"),
            ('post', "post"),
            ('all', "all"),
        ],
        default='pre',
    )
    output = fields.Text()
    button_run = fields.Boolean("Run test")

    @api.onchange('button_run')
    def _button_run_pressed(self):
        for record in self:
            if record.button_run:
                record._run_test()
                record.button_run = False

    @api.onchange('strategy_id')
    def _onchange_strategy_id(self):
        for record in self:
            record.operation = record.strategy_id.operation

    def run(self):
        for record in self:
            record._run()

    def _run(self):
        self.ensure_one()
        if self.debug:
            return False

        if all([self.operation == 'edit',
                self.strategy_id,
                self.field_mapping_id,
                ]):
            self.strategy_id.mass_edit(
                field_mapping_id=self.field_mapping_id.id)

    def _run_test(self):
        self.ensure_one()
        if not self.debug:
            self.output = "To show output, check 'debug'"
            return

        result = False
        if not (self.sub_operation or self.operation):
            self.output = "Choose an operation method to start!"
        elif self.sub_operation == 'map' and self.field_mapping_id:
            result = self.test_mapping()
        elif self.sub_operation == 'parse' and self.resource_id:
            result, _ = self.test_parser()
        elif self.operation == 'pull' and self.resource_id:
            result = self.test_pull()

        # the above code runs mass edit, debug mode is optionsl
        elif all([self.operation == 'edit',
                  self.strategy_id,
                  self.field_mapping_id,
                  ]):
            result = self.strategy_id.mass_edit(
                field_mapping_id=self.field_mapping_id.id, debug=True)
        else:
            self.output = (
                "Something is missing, or "
                "operation '{}' is not implemented yet...".format(self.sub_operation)
            )

        if result:
            self.output = json.dumps(
                result, ensure_ascii=False, indent=4, default=str,
            )

    def test_mapping(self, data=False, metadata=False, field_mapping_id=False):
        self.ensure_one()
        pre = post = False
        if self.pre_post == 'pre':
            pre, post = True, False
        elif self.pre_post == 'pre':
            pre, post = False, True
        elif self.pre_post == 'all':
            pre = post = True
        prune = self.prune
        sanitize = self.sanitize

        mapping = self.field_mapping_id
        if field_mapping_id:
            mapping = mapping.browse(field_mapping_id).exists()
        if not mapping:
            raise MissingError("No field mapping specified!")

        record = False
        if self.operation == 'push':
            domain_str = mapping.filter_domain
            if domain_str:
                domain = expression.normalize_domain(eval(domain_str))
            else:
                domain = []
            data = self.env[mapping.model_model].search(domain, limit=1)
            record = data

        try:
            if self.operation == 'pull' and not data:
                data = json.loads(mapping.test_data)
            if not metadata:
                metadata = json.loads(mapping.test_metadata)
        except json.decoder.JSONDecodeError:
            raise ValidationError("Invalid JSON test data")

        if not data:
            raise ValidationError("No test data")

        foreign_type = mapping.foreign_type_id
        foreign_id_key = foreign_type.field_ids[0].name,
        foreign_id = data.get(foreign_id_key)
        metadata.update({
            'field_mapping_id': mapping.id,
            'model_id': mapping.model_id.id,
            'model_model': mapping.model_id.model,
            'foreign_type_id': foreign_type.id,
            'foreign_type_name': foreign_type.name,
            'foreign_id_key': foreign_id_key,
            'foreign_id': foreign_id,
            'variant_tag': mapping.object_link_variant_tag,
            'now': datetime.now(),
            'operation': self.operation,
            'prune_false': self.sanitize_prune_false,
            'processed_keys': [],
            'debug': True,
            'record': record,
        })
        vals = mapping.apply_mapping(data, metadata)
        if pre:
            metadata.update(pre_post='pre')
            mapping.rule_ids_pre.apply_rules(vals, metadata)
        if post:
            metadata.update(pre_post='post')
            mapping.rule_ids_post.apply_rules(vals, metadata)
        if metadata.get('drop'):
            vals = {}
        if prune:
            implicit_keys = set(vals.keys()) - set(metadata['processed_keys'])
            for key in implicit_keys:
                vals.pop(key)
        sane = "N/A"
        if sanitize:
            sane = self.env['external.data.object'].sanitize_values(
                vals, **metadata)

        result = {'vals': vals, 'metadata': metadata, 'sane_for_create': sane}
        return result

    def test_parser(self):
        self.ensure_one()
        strategy = self.strategy_id
        resource = self.resource_id
        if not resource.exists():
            raise MissingError("Please specify a resource!")
        if not strategy.exists():
            strategy = strategy.get_strategy(
                operation='pull',
                resource_ids=resource.ids,
            )
        if not strategy:
            raise MissingError("Couldn't find strategy")

        debug_data, _ = strategy.pull_resource(resource.id, debug=True)
        return debug_data, _

    def test_pull(self):
        self.ensure_one()
        d_data, d_metadata = self.test_parser()
        field_mappings_all = self.strategy_id.field_mapping_ids
        result = {'parser_output': d_data, 'mapped_data': {}}
        for type_name, data_set in d_data.items():
            metadata_set = d_metadata[type_name]
            for i, data in enumerate(data_set):
                metadata = metadata_set[i]
                foreign_type_id = metadata.get('foreign_type_id')
                field_mappings = field_mappings_all.filtered(
                    lambda m: m.foreign_type_id.id == foreign_type_id
                )
                for field_mapping in field_mappings:
                    res = self.test_mapping(
                        data, metadata, field_mapping_id=field_mapping.id)
                    result['mapped_data'][field_mapping.name] = res

        return result


class ExternalDataFieldSelector(models.TransientModel):
    _name = 'external.data.field.selector'
    _description = "External Data Field Selector"

    model_model = fields.Char()
    field_id = fields.Many2one(
        'ir.model.fields',
        string="Field",
        domain="[('model', '=', model_model)]",
    )

    def set_key(self):
        for record in self:
            ctx = self.env.context
            model = ctx.get('active_model')
            rule_id = ctx.get('active_id')
            if rule_id and model == 'external.data.rule':
                rule = self.env[model].browse(rule_id).exists()
                if rule:
                    rule.key = record.field_id.name
