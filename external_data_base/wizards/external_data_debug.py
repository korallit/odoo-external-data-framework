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
    output = fields.Text()

    @api.onchange('operation')
    def _onchange_operation(self):
        for record in self:
            result = False
            if not record.operation:
                record.output = "Choose an operation to start!"
            elif record.operation == 'map' and record.field_mapping_id:
                result = record.field_mapping_id.test_mapping()
            elif record.operation == 'parse' and record.resource_id:
                result, _ = record.resource_id.test_parser(
                    strategy_id=record.strategy_id.id)
            elif record.operation == 'pull' and record.resource_id:
                result = record.resource_id.test_pull(
                    strategy_id=record.strategy_id.id)
            else:
                record.output = (
                    "Something is missing, or "
                    f"operation '{record.operation}' is not implemented yet..."
                )

            if result:
                record.output = json.dumps(
                    result, ensure_ascii=False, indent=4, default=str,
                )
