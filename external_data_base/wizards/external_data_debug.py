# coding: utf-8

import json

from odoo import api, fields, models


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
    operation = fields.Selection(
        selection=[
            ('parse', "parse"),
            ('map', "map"),
            ('pull', "pull"),
        ]
    )
    debug = fields.Boolean(default=True)
    prune = fields.Boolean(default=True)
    sanitize = fields.Boolean(default=True)
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

    def _run_test(self):
        self.ensure_one()
        if not self.debug:
            self.output = "To show output, check 'debug'"
            return

        result = False
        if not self.operation:
            self.output = "Choose an operation to start!"
        elif self.operation == 'map' and self.field_mapping_id:
            pre = post = False
            if self.pre_post == 'pre':
                pre, post = True, False
            elif self.pre_post == 'pre':
                pre, post = False, True
            elif self.pre_post == 'all':
                pre = post = True
            result = self.field_mapping_id.test_mapping(
                pre=pre, post=post,
                prune=self.prune,
                sanitize=self.sanitize,
            )
        elif self.operation == 'parse' and self.resource_id:
            result, _ = self.resource_id.test_parser(
                strategy_id=self.strategy_id.id)
        elif self.operation == 'pull' and self.resource_id:
            result = self.resource_id.test_pull(
                strategy_id=self.strategy_id.id)
        else:
            self.output = (
                "Something is missing, or "
                f"operation '{self.operation}' is not implemented yet..."
            )

        if result:
            self.output = json.dumps(
                result, ensure_ascii=False, indent=4, default=str,
            )
