# coding: utf-8

import logging

from odoo import api, fields, models
from odoo.osv import expression

_logger = logging.getLogger(__name__)


class DomainWizard(models.TransientModel):
    _name = 'external.data.domain.wizard'
    _description = "Domain Wizard"

    @staticmethod
    def _get_term_operators():
        operators = []
        for o in expression.TERM_OPERATORS:
            operators.append((o, o))
        return operators

    model_id = fields.Many2one(
        'ir.model',
        string="Model",
        required=True,
    )
    model_model = fields.Char(related='model_id.model')
    field_id = fields.Many2one(
        'ir.model.fields',
        string="Field",
        domain="[('model', '=', model_model)]",
        required=True,
    )
    domain_operator = fields.Selection(
        string="Logical",
        selection=[
            ('and', "AND"),
            ('or', "OR"),
        ],
        default="and",
    )
    operator_apply_global = fields.Boolean(
        string="Apply to all",
        help=(
            "If set, the logical operator will compare current leaf "
            "to the whole domain, like it would be in parentheses."
        ),
    )
    negate = fields.Boolean("NOT")
    term_operator = fields.Selection(
        string="Operator",
        selection=lambda r: r._get_term_operators(),
        required=True,
    )
    value_type = fields.Selection(
        string="Value type",
        selection=[
            ('str', "string"),
            ('int', "integer"),
            ('float', "float"),
            ('expr', "expression"),
        ],
        default='expr',
    )
    value_str = fields.Char("Value", required=True)
    append = fields.Boolean()
    domain = fields.Text(default="[]")
    record_count = fields.Integer("Record count")
    button_refresh = fields.Boolean("Compute domain")
    button_count = fields.Boolean("Count records")
    button_clear = fields.Boolean("Clear domain")

    @api.depends('field_id', 'domain_operator', 'value_type', 'value_str')
    @api.onchange('button_refresh')
    def button_refresh_pressed(self):
        for record in self:
            if record.button_refresh:
                record.compute_domain()
                record.button_refresh = False

    @api.depends('model_model')
    @api.onchange('button_count')
    def button_count_pressed(self):
        for record in self:
            if record.button_count:
                record.count_records()
                record.button_count = False

    @api.onchange('button_clear')
    def button_clear_pressed(self):
        for record in self:
            if record.button_clear:
                record.domain = "[]"
                record.button_clear = False

    @api.depends('field_id', 'domain_operator', 'value_type', 'value_str')
    def compute_domain(self):
        for record in self:
            record._compute_domain()

    def _compute_domain(self):
        self.ensure_one()
        try:
            value = self._eval_value()
        except Exception as e:
            _logger.error(e)
            return False

        leaf = expression.normalize_leaf(
            (self.field_id.name, self.term_operator, value)
        )
        if self.negate:
            domain = expression.distribute_not(
                [expression.NOT_OPERATOR, leaf]
            )
        else:
            domain = expression.normalize_domain([leaf])

        if self.domain:
            domain_set = expression.normalize_domain(eval(self.domain))
            if self.domain_operator == 'and':
                domain = expression.AND([domain_set, domain])
            elif self.domain_operator == 'or':
                domain = expression.OR([domain_set, domain])

        domain_str_list = []
        for leaf in domain:
            if isinstance(leaf, str):
                domain_str_list.append(f"'{leaf}'")
            else:
                domain_str_list.append(str(leaf))
        domain_str = "[\n  "
        domain_str += ",\n  ".join(domain_str_list)
        domain_str += ",\n]"
        self.domain = domain_str

    def _eval_value(self):
        self.ensure_one()
        if self.value_type == 'expr':
            return eval(self.value_str)
        elif self.value_type == 'int':
            return int(self.value_str)
        elif self.value_type == 'float':
            return float(self.value_str)
        elif self.value_type == 'str':
            return self.value_str

    def count_records(self):
        for record in self:
            if not (record.model_model and record.domain):
                return False
            domain = expression.normalize_domain(eval(self.domain))
            model = self.env[record.model_model]
            record.record_count = model.search_count(domain)
