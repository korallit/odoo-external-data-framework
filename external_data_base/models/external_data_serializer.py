# coding: utf-8

import gzip
import json
import jmespath
from io import BufferedReader

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError
from odoo.tools import etree
from bs4 import BeautifulSoup
from bs4 import element as bs_element
from urllib.parse import parse_qsl

from ..tools import bs
from ..tools.jmespath import options as jmespath_options

import logging
_logger = logging.getLogger(__name__)


class ExternalDataSerializer(models.Model):
    _name = 'external.data.serializer'
    _description = "External Data Serializer"

    name = fields.Char(required=True)
    engine = fields.Selection(
        selection=[
            ('json', "JSON"),
            ('bs', "BeautifulSoup"),
            ('lxml_etree', "lxml.etree"),
            ('csv', "CSV"),
            ('qweb', "Qweb"),
            ('custom', "custom"),
        ],
        required=True,
        default='json',
    )
    pretty_print = fields.Boolean("Pretty print", default=True)
    lxml_root = fields.Char("lxml root element")
    qweb_template = fields.Many2one(
        'ir.ui.view',
        string="Qweb template",
        domain="[('type', '=', 'qweb')]"
    )
    custom_name = fields.Char("Custom method name")
    jmespath_line_ids = fields.One2many(
        'external.data.jmespath.line',
        inverse_name='serializer_id',
        string="JMESPath Expressions",
    )
    parser_line_ids = fields.One2many(
        'external.data.parser.line',
        inverse_name='serializer_id',
        string="Directives",
        domain="[('engine', '=', engine)]",
    )
    parser_line_count = fields.Integer(
        compute='_compute_parser_line_count',
    )
    packaging = fields.Selection(
        selection=[
            ('tar', "tar"),
            ('gzip', "gzip"),
            ('zip', "zip"),
        ]
    )
    encryption = fields.Selection(
        selection=[
            ('pgp', "PGP"),
            ('userpass', "user/password"),
        ],
    )
    credential_ids = fields.Many2many(
        'external.data.credential',
        string="Credentials",
    )

    @api.depends('parser_line_ids')
    def _compute_parser_line_count(self):
        for record in self:
            record.parser_line_count = record.parser_line_ids.search_count([
                ('serializer_id', '=', self.id)])

    def list_parser_lines(self):
        self.ensure_one()
        form_view_id = self.env.ref(
            "external_data_base.external_data_parser_line_form_view").id
        return {
            'type': 'ir.actions.act_window',
            'name': 'Parser directives',
            'view_mode': 'tree',
            'views': [(False, 'tree'), (form_view_id, 'form')],
            'res_model': 'external.data.parser.line',
            'domain': [('serializer_id', '=', self.id)],
        }

    def decrypt(self, data):
        return data

    def extraxt(self, data):
        self.ensure_one()
        # TODO: check if data is bytes
        if self.packaging == 'gzip':
            return gzip.decompress(data)
        return data

    def parse(self, data):
        """Returns a dict of generator objects providing object data"""
        self.ensure_one()
        return self.parser_line_ids.objects(data)

    def render(self, data, metadata={}, key=False):
        self.ensure_one()
        if key:
            chunk = data.get(key)

        if self.engine == 'json':
            return self._render_json(data)
        elif self.engine == 'lxml_etree':
            return self._render_lxml_etree(chunk)
        elif self.engine == 'qweb':
            return self._render_qweb(data, metadata)
        return False

    def _render_json(self, data, indent=None):
        indent = None
        if self.pretty_print:
            indent = 4
        return self.render_json(data, indent=indent)

    @api.model
    def render_json(self, data, indent=None):
        return json.dumps(data, indent=indent)

    def _render_lxml_etree(self, items):
        self.ensure_one()
        if not isinstance(items, list):
            return False

        tag = self.lxml_root if self.lxml_root else "root"
        # TODO: attrs
        root = etree.Element(tag)
        for item in items:
            element = self._lxml_etree_create_element(item)
            if element is not None:
                root.append(element)
        return self._serialize_xml(root)

    def _serialize_xml(self, root):
        self.ensure_one()
        return etree.tostring(
            root, encoding='utf-8', xml_declaration=True,
            pretty_print=self.pretty_print
        )

    @api.model
    def _lxml_etree_create_element(self, data):
        if not isinstance(data, dict):
            _logger.error(
                "Serializer lxml etree wants a dict, got this: {}".format(str(data)))
            return None
        tag, attrs = data.get('tag'), data.get('attrs', {})
        if not tag:
            return False
        element = etree.Element(tag, **attrs)
        if data.get('text'):
            element.text = data['text']
        if data.get('children'):
            for child in data['children']:
                child_elem = self._lxml_etree_create_element(child)
                if child_elem is not None:
                    element.append(child_elem)
        return element

    def _render_qweb(self, data):
        if self.qweb_template:
            qweb = self.env['ir.qweb']
            try:
                return qweb._render(self.qweb_template, data)
            except Exception as e:
                _logger.error(e)
                return str(e)
        msg = "No Qweb template specified"
        _logger.error(msg)
        return msg

    def rearrange(self, items, metadata={}):
        # TODO: iterate over rules
        self.ensure_one()
        if not isinstance(items, list):
            _logger.warning("Items has to be a list, got this: {}".format(items))
            return False
        expressions = self.jmespath_line_ids
        if not expressions:
            return items
        expr_generators = expressions.get_jmespath_generators()
        items_new = []
        for vals in items:
            for expr in expr_generators:
                vals_new = expr({'vals': vals, 'metadata': metadata})
                if not vals_new:
                    continue
                items_new.append(vals_new)
        return items_new


class ExternalDataJMESPathLine(models.Model):
    _name = 'external.data.jmespath.line'
    _description = "External Data JMESPath Expression"
    _order = 'sequence'

    name = fields.Char(default="JEMSPath expression")
    sequence = fields.Integer(default=10)
    serializer_id = fields.Many2one(
        'external.data.serializer',
        string="Serializer",
        required=True,
        ondelete='cascade',
    )
    jmespath_expr = fields.Text("JMESPath expression")
    update = fields.Boolean()
    bypass = fields.Boolean()

    def get_jmespath_generators(self):
        generators = []
        for record in self.filtered(lambda r: not r.bypass):
            try:
                expr = jmespath.compile(record.jmespath_expr)
            except jmespath.exceptions.ParseError as e:
                _logger.error(e)
                continue

            def expression_closure(data):
                try:
                    # TODO: optionally update vals instead of return new
                    return expr.search(data, options=jmespath_options)
                except jmespath.exceptions.JMESPathTypeError as e:
                    _logger.error(e)
                    return data
            generators.append(expression_closure)
        return generators


class ExternalDataParserLine(models.Model):
    _name = 'external.data.parser.line'
    _description = "External Data Parser Directive"
    _order = 'sequence'

    name = fields.Char(compute='_compute_name')
    active = fields.Boolean(default=True)
    sequence = fields.Integer(default=1)
    serializer_id = fields.Many2one(
        'external.data.serializer',
        string="Serializer",
        required=True,
        ondelete='cascade',
    )
    engine = fields.Selection(related='serializer_id.engine')
    parent_id = fields.Many2one(
        'external.data.parser.line',
        string="Parent",
    )
    child_ids = fields.One2many(
        'external.data.parser.line',
        inverse_name='parent_id',
        string="Children",
    )
    foreign_type_id = fields.Many2one(
        'external.data.type',
        string="Foreign type",
        required=True,
        ondelete='restrict',
    )
    foreign_field_id = fields.Many2one(
        'external.data.type.field',
        string="Foreign field",
    )
    path_type = fields.Selection(
        string="Type",
        selection=[
            ('xpath', "XPath"),
            ('elementpath', "ElementPath"),
            ('css_find', "CSS selector"),
            ('css_findall', "CSS selector (multi)"),
            ('open_graph', "Open Graph"),
            ('schema_org', "Schema.org"),
            ('find', "find"),
            ('findall', "findall"),
            ('children', "children"),
            ('prev', "previous"),
            ('next', "next"),
            ('custom', "custom"),
        ],
        required=True,
    )
    path = fields.Char(required=True)
    extract_method = fields.Selection(
        string="Extract method",
        selection=[
            ('text', "text"),
            ('tag', "tag"),
            ('attr', "attribute"),
            ('tostring', "tostring"),
            ('list', "list"),
            ('index', "index (nth of)"),
        ],
    )
    extract_param = fields.Char("Extract param")

    @api.depends()
    def _compute_name(self):
        for record in self:
            field_name = record.foreign_type_id.name
            if record.foreign_field_id.name:
                field_name = '.'.join([
                    field_name,
                    record.foreign_field_id.name,
                ])
            record.name = "{}: {}".format(field_name, record.path)

    def objects(self, raw_data):
        "Returns a dict of object data generators"
        if not self:
            raise UserError("No parser directives defined")
        objects = {}

        # assuming that all rules use the same engine
        # TODO: prepare only if one engine found
        data_prep = self.prepare(raw_data, self[0].engine)
        foreign_type_ids = self.mapped('foreign_type_id').ids
        for foreign_type_id in foreign_type_ids:
            # getting toplevel rules for foreign type
            rules = self.filtered(
                lambda r:
                r.active and
                not r.parent_id and
                r.foreign_type_id.id == foreign_type_id
            )
            if not rules:
                continue  # TODO: log, exception

            # gettimg jmespath expression generator from serializer
            expressions = self.serializer_id.jmespath_line_ids
            jmespath_expr = expressions.get_jmespath_generators()

            # setting up generator
            objects[foreign_type_id] = self.object_data_generator(
                rules, data_prep, vals={}, jmespath_expr=jmespath_expr)
        return objects

    @api.model
    def object_data_generator(self, rules, data, vals={}, jmespath_expr=[]):
        foreign_type_id = rules.foreign_type_id.id
        vals, generator, gen_rule_id = rules.get_object_data(
            foreign_type_id, data, vals=vals,
        )
        if generator is not None and gen_rule_id:
            child_rules = self.search([
                ('parent_id', '=', gen_rule_id),
                ('active', '=', True),
            ])
            for child_data in generator:
                gen = self.object_data_generator(
                    child_rules, child_data, vals=vals,
                    jmespath_expr=jmespath_expr,
                )
                for child_vals in gen:
                    yield child_vals
        else:
            # rearrange by starting with an empty new dict
            vals_parsed = {}
            for expr in jmespath_expr:
                parsed = expr(vals)
                if parsed:
                    vals_parsed.update(parsed)
            if vals_parsed:
                yield vals_parsed
            else:
                yield vals

    def get_object_data(self, foreign_type_id, data,
                        vals={}, generator=None, gen_rule_id=False):

        for rule in self:
            if rule.foreign_type_id.id != foreign_type_id:
                continue

            new_data = rule.execute(data)
            if new_data is None:
                msg = "Parse rule (ID {}) execution returned no data".format(rule.id)
                _logger.debug(msg)
                continue

            if rule.is_generator():
                if generator is not None:
                    _logger.warning(
                        "Multiple generator found for an object type "
                        "at the same level. Returning the last one only."
                    )
                generator = new_data
                gen_rule_id = rule.id
            elif rule.child_ids:
                child_rules = rule.child_ids
                if rule.foreign_field_id and len(child_rules) > 1:
                    _logger.warning(
                        "Field rule ID {} has multiple children!".format(rule.id) +
                        "Processing the first one only."
                    )
                    child_rules = child_rules[0]
                vals, generator, gen_rule_id = rule.child_ids.get_object_data(
                    foreign_type_id, new_data, vals,
                )
            elif rule.foreign_field_id and rule.extract_method:
                field_id = rule.foreign_field_id.name
                vals[field_id] = new_data
        return vals, generator, gen_rule_id

    def is_generator(self):
        self.ensure_one()
        # TODO: can be different with different engines
        if self.path_type in ['elementpath', 'findall', 'css_findall']:
            return True
        return False

    def execute(self, data):
        self.ensure_one()
        if self.engine == 'lxml_etree':
            data_prep = self._prepare_lxml_etree(data)
            return self._execute_lxml_etree(data_prep)
        if self.engine == 'bs':
            data_prep = self._prepare_bs(data)
            return self._execute_bs(data_prep)
        else:
            raise ValidationError("Engine is not supported yet")

    def _execute_lxml_etree(self, data):
        if data is None:
            return None
        self.ensure_one()
        # ignore namespace
        # TODO: add option to it
        path = "{*}" + self.path
        if self.path_type == 'xpath':
            chunk = data.xpath(path)
        elif self.path_type == 'elementpath':
            chunk = data.iterfind(path)
        elif self.path_type == 'find':
            chunk = data.find(path)
        elif self.path_type == 'findall':
            chunk = data.iter(path)
        else:
            return None

        if chunk is None:
            return None

        if self.extract_method == 'attr' and self.extract_param:
            return chunk.get(self.extract_param)

        if self.extract_param:
            chunk = chunk.find(self.extract_param)

        if self.extract_method == 'text':
            return chunk.text
        elif self.extract_method == 'tag':
            return chunk.tag
        elif self.extract_method == 'tostring':
            return etree.tostring(chunk)
        else:
            return chunk

    def _execute_bs(self, data):
        if data is None:
            return None
        self.ensure_one()

        name = False

        attrs = dict(parse_qsl(self.path))
        if not attrs:
            name = self.path
            attrs = {}

        attrs_not = {}
        for key in attrs.copy().keys():
            if key[0] == '-':
                attrs_not[key[1:]] = attrs.pop(key)

        recursive = self.path_type == 'children'
        index = start = end = None
        if self.extract_method == 'index':
            index_str = self.extract_param
            index_split = index_str.split(':')
            if len(index_split) == 2:
                start = bs.get_index(index_split[0])
                end = bs.get_index(index_split[1])
            index = bs.get_index(index_str)

        if self.path_type == 'find':
            if index:
                try:
                    chunk = data.find_all(name=name, attrs=attrs)[index]
                except IndexError:
                    return None
            else:
                chunk = data.find(name=name, attrs=attrs)
        elif self.path_type in ['next', 'prev']:
            direction = self.path_type
            gen = bs.findall(data, name, attrs, attrs_not,
                             direction=direction)
            try:
                chunk = next(gen)
            except StopIteration:
                return None
        elif self.path_type in ['children', 'findall']:
            chunk = bs.findall(data, name, attrs, attrs_not,
                               recursive=recursive, start=start, end=end)
        elif self.path_type == 'css_find':
            if index:
                chunk = data.select(self.path)[index]
            else:
                chunk = data.select_one(self.path)
        elif self.path_type == 'css_findall':
            if start or end:
                chunk = data.select(self.path)[start:end]
            else:
                chunk = data.select(self.path)
            if not chunk:
                return None
            chunk = (e for e in chunk)
        else:
            return None

        if attrs_not and isinstance(chunk, bs_element.Tag):
            if not bs.compute_conditions(chunk, attrs_not=attrs_not):
                return None
        if chunk is None:
            return None

        if self.extract_method == 'attr' and self.extract_param:
            return chunk.get(self.extract_param)

        if self.extract_param:
            chunk = chunk.find(self.extract_param)

        if self.extract_method == 'text':
            return chunk.string.strip()
        elif self.extract_method == 'tag':
            return chunk.name
        elif self.extract_method == 'tostring':
            return str(chunk)
        elif self.extract_method == 'list':
            if isinstance(chunk, (int, str)):
                return [chunk]
            elif isinstance(chunk, list):
                return chunk
            else:
                return None
        else:
            return chunk

    @api.model
    def prepare(self, data, engine):
        if engine == 'lxml_etree':
            return self._prepare_lxml_etree(data)
        if engine == 'bs':
            return self._prepare_bs(data)
        else:
            raise ValidationError("Engine is not supported yet")

    @api.model
    def _prepare_lxml_etree(self, data):
        if isinstance(data, etree._Element):
            return data
        elif isinstance(data, BufferedReader):
            data.seek(0)
            data = data.read()

        if isinstance(data, (str, bytes)):
            try:
                return etree.fromstring(data)
            except Exception as e:
                _logger.error(e)
                return None
        return None

    @api.model
    def _prepare_bs(self, data):
        if isinstance(data, (BeautifulSoup, bs_element.Tag)):
            return data
        elif isinstance(data, BufferedReader):
            data.seek(0)
            data = data.read()

        if isinstance(data, (str, bytes)):
            try:
                return BeautifulSoup(data, features="lxml")
            except Exception as e:
                _logger.error(e)
                return None
        return None
