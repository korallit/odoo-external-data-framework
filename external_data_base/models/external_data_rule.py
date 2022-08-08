# coding: utf-8

import re
from datetime import datetime

from odoo import api, fields, models
from odoo.exceptions import ValidationError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataRule(models.Model):
    _name = 'external.data.rule'
    _description = "External Data Processing Rule"
    _order = 'sequence'

    name = fields.Char(required=True)
    sequence = fields.Integer(
        required=True,
        default=1,
    )
    key = fields.Char(
        "Key/Field",
        required=True,
    )
    pre_post = fields.Selection(
        string="Pre/Post",
        selection=[('pre', 'pre'), ('post', 'post')],
        default='pre',
    )
    field_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        ondelete='set null',
        string="Field mapping",
    )
    object_id = fields.Many2one(
        'external.data.object',
        ondelete='set null',
        string="External object",
    )
    operation = fields.Selection(
        string="Operation",
        selection=[
            ('exclude', "exclude"),
            ('clear', "clear"),
            ('replace', "regexp replace"),
            ('lambda', "lambda"),
            ('eval', "eval"),
            ('orm_ref', "ORM external ID"),
            ('orm_expr', "ORM expression"),
        ],
        required=True,
    )
    operation_help = fields.Selection(
        string="Description",
        selection=[
            ('exclude', "Pops value from 'vals' dictionary."),
            ('clear', "Set value to 'False'"),
            ('replace', "Replace regexp with re.sub(pattern, repl, count)"),
            (
                'lambda',
                "lambda expression evaluated to value ('v' in input)."
                "Other values can be injected in '{}' brackets."
            ),
            (
                'eval', "Evaluates the given expression. "
                "Available variables: vals(dict), metadata(dict)."
            ),
            ('orm_ref', "ORM external ID"),
            (
                'orm_expr', "Valid formats (parts are optional):\n"
                "model.search(domain, limit).filtered(lmabda).mapped(lambda)\n"
                "model.search(domain, limit).filtered(lmabda).field"
            ),
        ],
        readonly=True,
        compute="_compute_help",
    )
    sub_pattern = fields.Char()
    sub_repl = fields.Char()
    sub_count = fields.Integer()
    lambda_str = fields.Char("lambda v:")
    eval_str = fields.Char("eval")
    orm_ref = fields.Char("ORM external ID")
    orm_model = fields.Many2one(
        comodel_name='ir.model',
        string="Model",
    )
    orm_domain = fields.Char("domain")
    orm_limit = fields.Integer("limit")
    orm_filter = fields.Char("filtered(lambda r:")
    orm_map = fields.Char("mapped(lambda r:")
    orm_field = fields.Char("field")
    condition = fields.Char(
        help="A python expression that evaluates to a boolean (default=True). "
        "Available variables: vals(dict), metadata(dict)."
    )

    @api.model
    def default_get(self, fields):
        fields += ['field_mapping_id', 'object_id']
        res = super(WebscrapeRule, self).default_get(fields)
        return res

    @api.depends('operation')
    @api.onchange('operation')
    def _compute_help(self):
        for record in self:
            record.operation_help = record.operation

    def apply_rules(self, vals, metadata={}):
        if not isinstance(vals, dict):
            raise ValidationError(
                f"vals should be a dictionary, got this: {vals}"
            )

        for rule in self:
            if rule.condition:
                if not bool(rule._eval_expr(rule.condition, vals, metadata)):
                    continue

            value = vals.get(rule.key)
            result = None
            if rule.operation == 'exclude':
                if rule.key in vals.keys():
                    vals.pop(rule.key)
            elif rule.operation == 'clear':
                result = False
            elif rule.operation == 'replace':
                result = rule._regexp_replace(value, vals)
            elif rule.operation == 'lambda' and rule.lambda_str:
                if rule.lambda_str:
                    lambda_str = f"lambda v: {rule.lambda_str}"
                    f = rule._get_lambda(lambda_str, vals)
                    if f:
                        result = f(value)
            elif rule.operation == 'eval':
                result = rule._eval_expr(rule.eval_str, vals, metadata)
            elif rule.operation == 'orm_ref' and rule.orm_ref:
                record = rule.env.ref(rule.orm_ref)
                if record:
                    result = record.id
            elif rule.operation == 'orm_expr':
                result = rule._orm_expr(value, vals)

            if not isinstance(result, type(None)):
                vals[rule.key] = result

    def _regexp_replace(self, value, vals):
        self.ensure_one()
        if not value:
            return None
        pattern = re.compile(self.sub_pattern) if self.sub_pattern else '.*'
        repl = self.sub_repl.format(**vals) if self.sub_repl else ''
        count = int(self.sub_count)  # converts False to 0
        return re.sub(pattern, repl, value, count=count)

    def _orm_expr(self, value, vals):
        self.ensure_one()
        if self.orm_model:
            records = self.env[self.orm_model.model]
        else:
            records = value
        if isinstance(records, models.Model):
            domain = self._eval_domain_str(vals)
            f_filter = (
                self._get_lambda("lambda r:" + self.orm_filter, vals)
                if self.orm_filter else False
            )
            f_map = (
                self._get_lambda("lambda r:" + self.orm_map, vals)
                if self.orm_map else False
            )
            if domain:
                limit = int(self.orm_limit) if self.orm_limit else None
                records = records.search(domain, limit=limit)
            if f_filter:
                records = records.filtered(f_filter)
            if f_map:
                return records.mapped(f_map)
            if self.orm_field and records:
                try:
                    return records[0][self.orm_field]
                except AttributeError as e:
                    _logger.error(e)
            return records
        else:
            return None

    def _eval_domain_str(self, vals):
        self.ensure_one()
        if not self.orm_domain:
            return False
        domain_match = re.search(r'\[.*\]', self.orm_domain.format(**vals))
        if domain_match:
            domain_str = domain_match.group()
            try:
                domain = eval(domain_str)
            except SyntaxError:
                _logger.error(
                    f"Failed to evaluate domain string: {domain_str}"
                )
                return False
            if isinstance(domain, list):
                return domain
        return False

    @api.model
    def _eval_expr(self, expr, vals={}, metadata={}):
        if not isinstance(expr, str):
            return None
        try:
            return eval(expr)
        except SyntaxError:
            _logger.error(f"Failed to evaluate expression: {expr}")
            return None

    @api.model
    def _get_lambda(self, lambda_str, vals={}):
        if not isinstance(lambda_str, str):
            return False
        match = re.search(r'lambda( [a-z]+)?:.*', lambda_str.format(**vals))
        if match:
            lambda_str = match.group()
            try:
                f = eval(lambda_str)
                is_callable = callable(f)
                return f if is_callable else is_callable
            except SyntaxError:
                _logger.error(f"Failed to evaluate lambda: {lambda_str}")
                return False
        return False
