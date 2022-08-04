# coding: utf-8

import logging
import re
import requests
from base64 import b64encode
from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command
from odoo.exceptions import MissingError, ValidationError

_logger = logging.getLogger(__name__)


class WebscrapeSite(models.Model):
    _name = 'webscrape.site'  # TODO: generic.scraper.data.source
    _description = "Webscraper Sites"

    name = fields.Char(required=True)
    base_url = fields.Char("Base URL", required=True)  # TODO: to scraper
    scraper_model = fields.Char("Scraper model")
    scraper_id = fields.Many2oneReference(
        model_field='scraper_model',
        string="Scraper",
    )
    batch_size = fields.Integer("Batch size", default=10)
    last_fetch = fields.Datetime("Last fetched")
    type_mapping_line_ids = fields.One2many(
        'webscrape.type.mapping.line',
        inverse_name='site_id',
        string="Content type",
    )
    page_ids = fields.One2many(
        'webscrape.page',
        inverse_name='site_id',
        string="Pages",
    )

    def _scraper(self):
        scraper = self.env[self.scraper_model].browse(self.scraper_id).exists()
        if scraper:
            return scraper
        raise MissingError(
            f"Scraper not found '{self.scraper_model},{self.scraper_id}'"
        )

    def test(self):
        return self._scraper()._test()

    def _test(self):
        return "Hello from site!"

    def process_sitemap(self):
        self.ensure_one()

        _logger.info(f"Fetching sitemap from {self.base_url}")
        sitemap = self._scraper().process_sitemap()
        for d in sitemap:
            d.update(site_id=self.id)
        if not self.last_fetch:
            _logger.info(f"Creating page objects for site {self.base_url}")
            pages = self.env['webscrape.page'].create(sitemap)
            self.last_fetch = datetime.now()
            return sitemap, pages.ids

        _logger.info("Syncing page data...")
        for page_data in sitemap:
            page_data.update(site_id=self.id)
            url = page_data['url']
            page = self.page_ids.filtered(lambda p: p.url == url)
            if len(page) > 1:
                _logger.warning(
                    f"Multiple pages {page.ids} has the same url: {url}, "
                    "selecting the first."
                )
                page = page[0]
            if page:
                page.write(page_data)
            else:
                self.page_ids = [(Command.CREATE, 0, page_data)]
        return sitemap

    def batch_scrape(self, type_mapping_line_id=False,
                     sync=False, prune=False):
        pages = self.page_ids.filtered(
            lambda r: (
                not r.last_scrape or
                (r.last_mod and r.last_scrape and r.last_mod > r.last_scrape)
                # TODO: or (not r.last_mod and r.last_scrape + week < now)
            )
        )
        if type_mapping_line_id:
            pages = pages.filtered(
                lambda p: type_mapping_line_id in p.type_mapping_line_ids
            )
        return pages.batch_scrape(
            batch_size=self.batch_size,
            sync=sync, prune=prune,
        )

    def _scrape_page(self, url):
        self.ensure_one()
        return self._scraper().scrape_page(url)


class WebscrapeTypeMappingLine(models.Model):
    _name = 'webscrape.type.mapping.line'
    _description = "Webscraper Content Types"
    _order = 'sequence'

    name = fields.Char(required=True)
    sequence = fields.Integer(
        required=True,
        default=1,
    )
    content_type = fields.Char(
        "Content Type",
        required=True,
    )
    model_id = fields.Many2one(
        'ir.model',
        ondelete='cascade',
        string="Model",
        required=True,
    )
    source_id_key = fields.Char(
        "Source ID key",
        required=True,
        help="The ID parameter key in the input data structure."
    )
    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='cascade',
        string="Site",
        required=True,
    )
    field_mapping_line_ids = fields.One2many(
        'webscrape.field.mapping.line',
        inverse_name='type_mapping_line_id',
        string="Field mapping line",
    )
    rule_ids = fields.One2many(
        'webscrape.rule',
        inverse_name='type_mapping_line_id',
        string="Rules",
    )

    def process_values(self, vals):
        self.ensure_one()
        for mapping in self.field_mapping_line_ids:
            key = mapping.target_field.name
            if key in vals.keys():
                continue
            vals.update({key: vals.get(mapping.source_key)})
        for rule in self.rule_ids.filtered(lambda r: r.pre_post == 'pre'):
            vals_mod = rule.process_rule(vals)
            vals.update(vals_mod)
        return True

    def sanitize_values(self, vals):
        model = self.env[self.model_id.model]
        fields_data = model.fields_get()
        defaults = model.default_get(fields_data.keys())
        enough_to_create = True
        for name, field in fields_data.items():
            if name not in vals.keys():
                continue
            value = vals[name]
            # if values is a recordset, get ids/id
            if isinstance(value, models.Model):
                if not bool(value):
                    value = False
                elif len(value) > 1:
                    value = value.ids
                else:
                    value = value.id
            # check required fields
            if (
                    not value and field.get('required') and
                    name not in defaults and
                    field.get('type') not in ['one2many', 'many2many']
            ):
                _logger.warning(
                    "Missing required field of model {}: {}".format(
                        model._name, name
                    )
                )
                enough_to_create = False
            # check value by type
            if field.get('type') == 'binary':
                if isinstance(value, str):
                    data = self._fetch_binary_data(vals[name])
                    if data:
                        vals[name] = data
                    else:
                        vals.pop(name)
                elif value is not False:
                    vals.pop(name)
            elif field.get('type') == 'many2one':
                if isinstance(value, str):
                    try:
                        vals[name] = int(value)
                    except Exception as e:
                        _logger.error(e)
                        vals.pop(name)
                elif isinstance(value, list) and value:
                    vals[name] = value[0]
                elif not (isinstance(value, int) or value is False):
                    vals.pop(name)
            elif field.get('type') in ['one2many', 'many2many']:
                # only clear and link is supported
                if isinstance(value, int):
                    value = [value]
                elif isinstance(value, str):
                    try:
                        value = [
                            int(i) for i in
                            re.sub(' ', '', value).split(',')
                        ]
                    except Exception as e:
                        _logger.error(e)

                if isinstance(value, list):
                    vals[name] = [
                        Command.link(i) for i in value
                        if isinstance(i, int)
                    ]
                elif value is False:
                    vals[name] = [Command.clear()]
                else:
                    vals.pop(name)

        # drop irrelevant items
        irrelevant_keys = set(vals.keys()) - set(fields_data.keys())
        for key in irrelevant_keys:
            vals.pop(key)
        return enough_to_create

    @api.model
    def _fetch_binary_data(self, url):
        if not isinstance(url, str):
            _logger.error(f"Invalid URL: {url}")
            return False
        try:
            res = requests.get(url)
        except Exception as e:
            _logger.error(e)
            return False
        if isinstance(res.content, bytes):
            return b64encode(res.content)
        return False


class WebscrapeFieldMappingLine(models.Model):
    _name = 'webscrape.field.mapping.line'
    _description = "Webscraper Field Mapping Line"

    name = fields.Char(compute="_compute_name")
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='cascade',
        string="Type mapping",
        required=True,
    )
    source_key = fields.Char("Source key", required=True)
    target_model = fields.Char(related='type_mapping_line_id.model_id.model')
    target_field = fields.Many2one(
        'ir.model.fields',
        ondelete='cascade',
        string="Target field",
        required=True,
    )

    @api.depends(
        'type_mapping_line_id',
        'source_key',
        'target_field',
    )
    def _compute_name(self):
        for record in self:
            src = '.'.join([
                record.type_mapping_line_id.content_type,
                record.source_key,
            ])
            dst = f"{record.target_field.model}.{record.target_field.name}"
            record.name = f"{src} > {dst}"


class WebscrapePage(models.Model):
    _name = 'webscrape.page'
    _description = "Webscraper Pages"
    _rec_name = 'url'
    _order = 'url'

    url = fields.Char("URL", required=True)
    level = fields.Integer("Tree level", default=0)
    changefreq = fields.Char("Change frequency")
    priority = fields.Float("Priority")
    last_mod = fields.Datetime("Last modification time")
    last_scrape = fields.Datetime("Last scrape time")
    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='cascade',
        string="Site",
        required=True,
    )
    type_mapping_line_ids = fields.Many2many(
        comodel_name='webscrape.type.mapping.line',
        string="Type mappings",
    )
    object_ids = fields.Many2many(
        comodel_name='webscrape.object',
        string="Related records",
    )
    # TODO: Language

    def batch_scrape(self, batch_size=1, sync=False, prune=False):
        res = []
        i = 0
        for page in self:
            if i == batch_size:
                return res
            try:
                res.append(page.scrape_page(sync=sync, prune=prune))
            except Exception as e:
                _logger.error(e)
            i += 1

    def scrape_page(self, sync=False, prune=False):
        self.ensure_one()
        _logger.info(f"Scraping page {self.url}")
        dataset = self.site_id._scrape_page(self.url)

        # find content_types
        found_content_types = set([
            data['content_type']
            for data in dataset if data.get('content_type')
        ])
        type_mapping_lines = self.type_mapping_line_ids.search([
            ('site_id', '=', self.site_id.id),
            ('content_type', 'in', list(found_content_types)),
        ])
        if type_mapping_lines:
            self.type_mapping_line_ids = [Command.set(type_mapping_lines.ids)]
            if sync:
                self.sync_related_objects(dataset)
                self.last_scrape = datetime.now()
            if prune:
                self.prune_related_objects(dataset)

        return dataset

    def prune_related_objects(self, dataset):
        self.ensure_one()
        source_ids = [
            data['source_id']
            for data in dataset if data.get('source_id')
        ]
        unrelated_objects = self.object_ids.filtered(
            lambda r: r.source_id not in source_ids
        )
        # TODO: delete orphan objects
        self.object_ids = [Command.unlink(i) for i in unrelated_objects.ids]

    def sync_related_objects(self, dataset):
        self.ensure_one()
        if not self.type_mapping_line_ids:
            raise MissingError(f"No type mappings for page ID {self.id}")
        for data in dataset:
            assert({'vals', 'content_type'}.issubset(data.keys()))
            type_mappings = self.type_mapping_line_ids.filtered(
                lambda t: t.content_type == data['content_type']
            )
            if not type_mappings:
                _logger.warning(
                    "No type mapping found: page {}, content_type '{}'".format(
                        self.id, data['content_type']
                    )
                )

            for type_mapping in type_mappings:
                vals = data['vals'].copy()
                type_mapping.process_values(vals)

                source_id = data["vals"].get(type_mapping.source_id_key)
                self.object_ids.sync(
                    source_id, type_mapping.id, vals,
                    page_id=self.id
                )


class WebscrapeObject(models.Model):
    _name = 'webscrape.object'
    _description = "Webscraper Scraped Object Relations"

    name = fields.Char(required=True)
    source_id = fields.Char(
        "Source ID",
        help="A unique identifier that helps the CRUD methods "
        "to match the source object with the targeted record.",
        required=True,
    )
    model_id = fields.Many2one(
        'ir.model',
        string="Model",
        ondelete='cascade',
        required=True,
    )
    model_model = fields.Char(
        string="Model",
        related='model_id.model',
        store=True,
    )
    related_record = fields.Many2oneReference(
        "Related record",
        model_field='model_model',
        required=True,
    )
    type_mapping_line_ids = fields.Many2many(
        comodel_name='webscrape.type.mapping.line',
        string="Type mappings",
    )
    page_ids = fields.Many2many(
        comodel_name='webscrape.page',
        string="Related pages",
    )
    priority = fields.Integer("Priority", default=100)
    rule_ids = fields.One2many(
        'webscrape.rule',
        inverse_name='object_id',
        string="Rules",
    )

    @api.model
    def sync(self, source_id, type_mapping_id, vals, page_id=False):
        # update/create record
        type_mapping = self.type_mapping_line_ids.browse(type_mapping_id)
        if not type_mapping.exists():
            _logger.error(
                f"Couldn't find type mapping by ID {type_mapping_id}"
            )
            return False
        object_relation = self.search([
            ('source_id', '=', source_id),
            ('type_mapping_line_ids', '=', [type_mapping_id]),
            ('page_ids', '=', [page_id]),
        ], limit=1)
        if object_relation:
            object_rules = object_relation.rule_ids.filtered(
                lambda r: r.pre_post == 'pre'
            )
            for rule in object_rules:
                vals_mod = rule.process_rule(vals)
                vals.update(vals_mod)
            type_mapping.sanitize_values(vals)
            record_id = object_relation.related_record
            record_model = object_relation.model_model
            record = self.env[record_model].browse(record_id)
            assert(record.exists())
            record.write(vals)
        elif type_mapping.sanitize_values(vals):
            record = self.env[type_mapping.model_id.model].create(vals)
            self.create({
                "name": record.name if record.name else source_id,
                "source_id": source_id,
                "model_id": type_mapping.model_id.id,
                "related_record": record.id,
                "type_mapping_line_ids": [Command.link(type_mapping.id)],
                "page_ids": [Command.link(page_id)] if page_id else None,
            })
        else:
            _logger.error(
                "Sanity check failed for object: "
                f"type ID '{type_mapping_id}', ID '{source_id}'"
            )
            return False

        # post process type and object rules on record
        type_rules = type_mapping.rule_ids.filtered(
            lambda r: r.pre_post == 'post'
        )
        for rule in type_rules:
            vals_mod = rule.process_rule(vals)
            vals.update(vals_mod)
        if object_relation:
            object_rules = object_relation.rule_ids.filtered(
                lambda r: r.pre_post == 'post'
            )
            for rule in object_rules:
                vals_mod = rule.process_rule(vals)
                vals.update(vals_mod)
        return True


class WebscrapeRule(models.Model):
    _name = 'webscrape.rule'
    _description = "Webscrape processing rules"
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
    operation = fields.Selection(
        string="Operation",
        selection=[
            ('exclude', "exclude"),
            ('clear', "clear"),
            ('replace', "regexp replace"),
            ('lambda', "lambda"),
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
    param1 = fields.Char()
    param2 = fields.Char()
    param3 = fields.Char()
    sub_pattern = fields.Char()
    sub_repl = fields.Char()
    sub_count = fields.Integer()
    lambda_str = fields.Char("lambda v:")
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
        "Reference to vals data by keys can be injected in '{}' brackets."
    )
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='set null',
        string="Type mapping",
    )
    object_id = fields.Many2one(
        'webscrape.object',
        ondelete='set null',
        string="Scraped object",
    )

    @api.depends('operation')
    @api.onchange('operation')
    def _compute_help(self):
        for record in self:
            record.operation_help = record.operation

    def process_rule(self, data, vals={}):
        if isinstance(data, dict):
            if not vals:
                vals = data.copy()
            value = vals.get(self.key)
        elif isinstance(data, models.Model) and hasattr(data, self.key):
            value = data[self.key]
        else:
            value = None

        if self.condition:
            condition = self._eval_condition(vals)
            if not condition:
                return vals

        result = None
        if self.operation == 'exclude':
            if self.key in vals.keys():
                vals.pop(self.key)
        elif self.operation == 'clear':
            result = False
        elif self.operation == 'replace':
            result = self._regexp_replace(value, vals)
        elif self.operation == 'lambda' and self.lambda_str:
            if self.lambda_str:
                lambda_str = f"lambda v: {self.lambda_str}"
                f = self._get_lambda(lambda_str, vals)
                if f:
                    result = f(value)
        elif self.operation == 'orm_ref' and self.orm_ref:
            record = self.env.ref(self.orm_ref)
            if record:
                result = record.id
        elif self.operation == 'orm_expr':
            result = self._orm_expr(value, vals)

        if isinstance(result, type(None)):
            return vals
        elif isinstance(data, dict):
            vals[self.key] = result
        elif isinstance(data, models.Model) and hasattr(data, self.key):
            data[self.key] = result

        return vals

    def _eval_condition(self, vals):
        expr = self.condition.format(**vals)
        try:
            condition = bool(eval(expr))
        except SyntaxError:
            _logger.error(f"Failed to evaluate expression: {expr}")
            condition = True
        return condition

    def _regexp_replace(self, value, vals):
        if not value:
            return None
        pattern = re.compile(self.sub_pattern) if self.sub_pattern else '.*'
        repl = self.sub_repl.format(**vals) if self.sub_repl else ''
        count = int(self.sub_count)  # converts False to 0
        return re.sub(pattern, repl, value, count=count)

    def _orm_expr(self, value, vals):
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
