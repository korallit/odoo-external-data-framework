# coding: utf-8

import re
import requests
from base64 import b64encode
from time import strptime, mktime
from datetime import datetime

from odoo import api, fields, models
from odoo.exceptions import ValidationError

# may be used in user input
from datetime import datetime

from cryptography.utils import CryptographyDeprecationWarning
import warnings
import logging
_logger = logging.getLogger(__name__)

# Ignoring pyOpenSSL warnings
warnings.simplefilter('ignore', category=CryptographyDeprecationWarning)


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
        required=True,
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
            ('drop', "Drop item"),
            ('exclude', "exclude"),
            ('clear', "clear"),
            ('replace', "regexp replace"),
            ('hashtable', "hashtable"),
            ('parse_time', "Parse time"),
            ('lambda', "lambda"),
            ('eval', "eval"),
            ('orm_ref', "ORM external ID"),
            ('orm_expr', "ORM expression"),
            ('object_link', "Object link"),
            ('fetch_binary', "Fetch binary"),
        ],
        required=True,
    )
    operation_help = fields.Selection(
        string="Description",
        selection=[
            ('drop', "Drop item if the conditions met"),
            ('exclude', "Pops value from 'vals' dictionary."),
            ('clear', "Set value to 'False'"),
            ('replace', "Replace value with re.sub(pattern, repl, count)"),
            ('parse_time', "Parse time by pattern with time.strptime"),
            ('hashtable', "Map parsed data as key to a hashtable"),
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
            (
                'object_link', "Searches a linked external object by "
                "data source, mapping and value as foreign ID."
            ),
            (
                'fetch_binary', "Fetches a binary from URL and "
                "encodes it to base64 byte object"
            ),
        ],
        readonly=True,
        compute="_compute_help",
    )
    drop_delete = fields.Boolean("delete")
    sub_pattern = fields.Char()
    sub_repl = fields.Char()
    sub_count = fields.Integer()
    hashtable = fields.Text(default="{}")
    parse_time_pattern = fields.Char("pattern")
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
    obj_source_id = fields.Many2one(
        'external.data.source',
        string="Data source",
    )
    obj_mapping_id = fields.Many2one(
        'external.data.field.mapping',
        "Field mapping",
    )
    obj_foreign_type_id = fields.Many2one(
        'external.data.type',
        "Foreign type",
        domain="[('field_mapping_ids', 'in', [obj_mapping_id])]",
    )
    obj_model_id = fields.Many2one(
        'ir.model',
        "Model",
    )
    condition = fields.Text(
        "Conditions",
        help="A python expression that evaluates to a boolean (default=True). "
        "Available variables: vals(dict), metadata(dict)."
    )
    condition_negate = fields.Boolean("Not")
    condition_operator = fields.Selection(
        string="Logical operator",
        selection=[
            ('all', "all"),
            ('any', "any"),
        ],
        help=(
            "If set, value has to be a comma separated list of expressions, "
            "free form expected otherwise."
        )
    )

    @api.model
    def default_get(self, fields):
        fields += ['field_mapping_id', 'object_id']
        res = super(ExternalDataRule, self).default_get(fields)
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
            metadata.update(key=rule.key)
            if rule.condition:
                operator = rule.condition_operator
                if operator:
                    conditions = operator + "([" + rule.condition + "])"
                else:
                    conditions = "(" + rule.condition + ")"
                if rule.condition_negate:
                    conditions = "not " + conditions
                if not bool(rule._eval_expr(conditions, vals, metadata)):
                    continue
            if rule.operation == 'drop':
                metadata.update(drop=True)
                if rule.drop_delete:
                    metadata.update(delete=True)
                return

            value = vals.get(rule.key)
            result = None
            if rule.operation == 'exclude':
                if rule.key in vals.keys():
                    vals.pop(rule.key)
            elif rule.operation == 'clear':
                result = False
            elif rule.operation == 'replace':
                result = rule._regexp_replace(value, vals)
            elif rule.operation == 'hashtable':
                if rule.hashtable and value:
                    hashtable = self._eval_expr(rule.hashtable)
                    if isinstance(hashtable, dict):
                        result = hashtable.get(value)
            elif rule.operation == 'parse_time':
                result = rule._parse_time(value)
            elif rule.operation == 'lambda' and rule.lambda_str:
                if rule.lambda_str:
                    lambda_str = f"lambda v: {rule.lambda_str}"
                    f = rule._get_lambda(lambda_str, vals)
                    if f:
                        result = f(value)
            elif rule.operation == 'eval':
                result = rule._eval_expr(rule.eval_str, vals, metadata)
            elif rule.operation == 'orm_ref' and rule.orm_ref:
                try:
                    record = rule.env.ref(rule.orm_ref)
                except ValueError as e:
                    _logger.error(e)
                    continue
                if record:
                    result = record.id
            elif rule.operation == 'orm_expr':
                result = rule._orm_expr(value, vals)
            elif rule.operation == 'object_link':
                result = rule._search_object_link(value)
            elif rule.operation == 'fetch_binary':
                result = rule._fetch_binary(value)

            if not isinstance(result, type(None)):
                vals[rule.key] = result
                if 'processed_keys' in metadata.keys():
                    metadata['processed_keys'].append(rule.key)
                else:
                    metadata['processed_keys'] = [rule.key]

    def _regexp_replace(self, value, vals):
        self.ensure_one()
        if not value:
            value = ''
        pattern = re.compile(self.sub_pattern) if self.sub_pattern else '.*'
        repl = self.sub_repl.format(**vals) if self.sub_repl else ''
        count = int(self.sub_count)  # converts False to 0
        return re.sub(pattern, repl, value, count=count)

    def _parse_time(self, value):
        self.ensure_one()
        if not (value and self.parse_time_pattern):
            return None
        ts = mktime(strptime(value, self.parse_time_pattern))
        return datetime.fromtimestamp(ts)

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

    def _search_object_link(self, value):
        # This method returns False instead of None to clear irrelevant values
        self.ensure_one()
        field_mapping = self.obj_mapping_id
        foreign_type_id = self.obj_foreign_type_id.id
        model_id = self.obj_model_id.id
        if field_mapping:
            if not foreign_type_id:
                foreign_type_id = field_mapping.foreign_type_id.id
            if not model_id:
                model_id = field_mapping.model_id.id
        if not (foreign_type_id and model_id):
            return False
        ext_object = self.env['external.data.object'].search([
            ('foreign_type_id', '=', foreign_type_id),
            ('foreign_id', '=', value),
        ], limit=1)  # TODO: check if more than one found
        if ext_object:
            object_link = ext_object.get_object_link(model_id)
            if object_link:
                return object_link.record_id
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

    @api.model
    def _fetch_binary(self, url):
        if not isinstance(url, str):
            _logger.error(f"Invalid URL: {url}")
            return None
        try:
            res = requests.get(url)
        except Exception as e:
            _logger.error(e)
            return None
        if isinstance(res.content, bytes):
            return b64encode(res.content)
        return None
