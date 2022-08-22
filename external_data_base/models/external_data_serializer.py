# coding: utf-8

from io import BufferedReader

from odoo import api, fields, models
from odoo.exceptions import UserError, ValidationError
from odoo.tools import etree
from bs4 import BeautifulSoup
from bs4 import element as bs_element
from urllib.parse import parse_qsl

from ..tools import bs

import logging
_logger = logging.getLogger(__name__)


class ExternalDataSerializer(models.Model):
    _name = 'external.data.serializer'
    _description = "External Data Serializer"

    name = fields.Char(required=True)
    engine = fields.Selection(
        selection=[
            ('bs', "BeautifulSoup"),
            ('json', "JSON"),
            ('lxml_etree', "lxml.etree"),
            ('orm', "Odoo ORM"),
            ('custom', "custom"),
        ],
        required=True,
    )
    custom_name = fields.Char("Custom method name")
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
        return data

    def parse(self, data):
        """Returns a dict of generator objects providing object data"""
        self.ensure_one()
        return self.parser_line_ids.objects(data)


class ExternalDataParserLine(models.Model):
    _name = 'external.data.parser.line'
    _description = "External Data Parser Directive"
    _order = 'sequence'

    name = fields.Char(compute='_compute_name')
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
            ('css', "CSS selector"),
            ('open_graph', "Open Graph"),
            ('schema_org', "Schema.org"),
            ('find', "find"),
            ('findall', "findall"),
            ('children', "children"),
            ('prev', "previous"),
            ('next', "next"),
            ('jmespath', "JMESPath"),
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
            record.name = f"{field_name}:{record.path}"

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
            # getting toplpevel rules for foreign type
            rules = self.filtered(
                lambda r:
                not r.parent_id and
                r.foreign_type_id.id == foreign_type_id
            )
            if not rules:
                continue  # TODO: log, exception

            # setting up generator
            objects[foreign_type_id] = self.object_data_generator(
                rules, data_prep, vals={})
        return objects

    @api.model
    def object_data_generator(self, rules, data, vals={}):
        foreign_type_id = rules.foreign_type_id.id
        vals, generator, gen_rule_id = rules.get_object_data(
            foreign_type_id, data, vals=vals,
        )
        if generator is not None and gen_rule_id:
            child_rules = self.search([('parent_id', '=', gen_rule_id)])
            for child_data in generator:
                gen = self.object_data_generator(
                    child_rules, child_data, vals=vals)
                for child_vals in gen:
                    yield child_vals
        else:
            yield vals

    def get_object_data(self, foreign_type_id, data,
                        vals={}, generator=None, gen_rule_id=False):

        for rule in self:
            if rule.foreign_type_id.id != foreign_type_id:
                continue

            new_data = rule.execute(data)
            if new_data is None:
                msg = f"Parse rule (ID {rule.id}) execution returned no data"
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
                        f"Field rule ID {rule.id} has multiple children!"
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
        if self.path_type in ['elementpath', 'findall']:
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
