# coding: utf-8

import re
from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command


class WebscrapeSite(models.Model):
    _name = 'webscrape.site'
    _description = "Webscraper Sites"

    name = fields.Char(required=True)
    base_url = fields.Char("Base URL", required=True)
    scraper_model_id = fields.Many2one(
        'ir.model',
        ondelete='cascade',
        string="Scraper model",
        required=True,
    )
    type_mapping_line_ids = fields.One2many(
        'webscrape.type.mapping.line',
        inverse_name='site_id',
        string="Content type",
    )
    vendor_id = fields.Many2one(
        'res.partner',
        ondelete='set null',
        string="Vendor",
    )
    batch_size = fields.Integer("Batch size", default=10)
    last_fetch = fields.Datetime("Last fetched")
    page_ids = fields.One2many(
        'webscrape.page',
        inverse_name='site_id',
        string="Pages",
    )

    def process_sitemap(self):
        self.ensure_one()
        scraper = self.env[self.scraper_model_id.model]
        # TODO: check scraper model method availability
        sitemap = scraper.process_sitemap(
            base_url=self.base_url,
            not_before=self.last_fetch,
        )
        if not self.last_fetch:
            pages = self.env['webscrape.page'].create(sitemap)
            self.last_fetch = datetime.now()
            return sitemap, pages.ids

        for page_data in sitemap:
            page = self.page_ids.filtered(lambda p: p.url == page_data['url'])
            if page:
                page.write(page_data)
            else:
                self.write({
                    "page_ids": (Command.CREATE, 0, page_data)
                })
        return sitemap

    def batch_scrape(self, type_mapping_line_id=False,
                     sync=False, prune=False):
        pages = self.page_ids
        if type_mapping_line_id:
            pages = pages.filtered(
                lambda p: type_mapping_line_id in p.type_mapping_line_ids
            )
        return pages.batch_scrape(
            batch_size=self.batch_size,
            sync=sync, prune=prune,
        )


class WebscrapeTypeMappingLine(models.Model):
    _name = 'webscrape.type.mapping.line'
    _description = "Webscraper Content Types"

    name = fields.Char(required=True)
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

    def process_values(self, data):
        self.ensure_one()
        vals = {}
        for mapping in self.field_mapping_line_ids:
            key = mapping.target_field.name
            value = data["vals"].get(mapping.source_key)
            if mapping.target_field.ttype == 'binary':
                value = self._fetch_binary_value(value)
            vals.update({key: value})

        # TODO: pre process type rules
        return vals

    def sanitize_values(self, vals):
        # TODO: drop unrelevant items
        # TODO: check required fields
        # TODO: check recordset values, get ids/id
        return True

    @api.model
    def _fetch_binary_value(self, url):
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
        ondelete='set null',
        string="Site",
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
            # TODO: log info
            res.append(page.scrape_page(sync=sync, prune=prune))
            i += 1

    def scrape_page(self, sync=False, prune=False):
        self.ensure_one()
        scraper = self.env[self.site_id.scraper_model_id.model]
        # TODO: check scraper model method availability
        dataset = scraper.scrape_page(
            self.url,
            vendor_id=self.site_id.vendor_id.id
        )

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
        for data in dataset:
            # get field mapping and create 'vals' dictionary
            try:
                type_mapping = self.type_mapping_line_ids.filtered(
                    lambda t: t.content_type == data['content_type']
                )
                if not type_mapping:
                    raise ValueError(
                        "Couldn't find type mapping for content type '%s'!" %
                        data['content_type']
                    )
                type_mapping = type_mapping[0]
            except KeyError as e:
                raise e
            vals = type_mapping.process_values(data)

            # try to find record by source_id
            source_id = data["vals"].get(type_mapping.source_id_key)
            object_relation = self.object_ids.filtered(
                lambda r: r.source_id == source_id
            )

            # update/create record
            if object_relation:
                # TODO: pre process object rules
                # TODO: sanitize values
                record_id = object_relation[0].related_record
                record_model = object_relation[0].model_model
                record = self.env[record_model].browse(record_id)
                record.write(vals)
            else:  # TODO: elif sanitize vals
                record = self.env[type_mapping.model_id.model].create(vals)
                self.object_ids = [Command.create({
                    "name": record.name if record.name else source_id,
                    "source_id": source_id,
                    "model_id": type_mapping.model_id.id,
                    "related_record": record.id,
                    "type_mapping_line_ids": [Command.link(type_mapping.id)],
                    "page_ids": [Command.link(self.id)],
                })]

            # TODO: post process type and object rules on record


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
            ('clear', "clear"),
            ('replace', "regexp replace(pattern[, repl, count])"),
            ('lambda', "lambda"),
            ('orm_ref', "ORM external ID"),
            ('orm_search', "ORM search(model, domain, limit)"),
            ('orm_records', "ORM records(filtered, mapped)"),
        ],
        default='default',
        required=True,
    )
    param1 = fields.Char()
    param2 = fields.Char()
    param3 = fields.Char()
    is_default = fields.Boolean(
        "Default",
        help="Applied only when value not found."
    )
    bypass = fields.Boolean()
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

    def process_rule(self, data, vals=False):
        if isinstance(data, dict):
            value = data.get(self.key)
            if not vals:
                vals = data.copy()
        elif isinstance(data, models.Model) and hasattr(data, self.key):
            value = data[self.key]
            if not vals:
                vals = {}
        else:
            value = False

        if self.operation == 'exclude':
            if self.key in vals.keys():
                vals.pop(self.key)

        if value and self.is_default:
            return vals

        if self.operation == 'clear':
            value = False
        elif self.operation == 'replace':
            result = self._regexp_replace(value, vals)
            if result:
                value = result
        elif self.operation == 'lambda' and self.param1:
            f = self._get_lambda(self.param1, vals)
            if f:
                value = f(value)
        elif self.operation == 'orm_ref' and self.param1:
            record = self.env.ref(self.param1)
            if record:
                value = record.id
        elif self.operation == 'orm_search':
            records = self._orm_search(vals)
            if not isinstance(records, type(None)):
                value = records
        elif self.operation == 'orm_records':
            records = self._orm_filter_map(value, vals)
            if not isinstance(records, type(None)):
                value = records

        if isinstance(data, dict):
            vals[self.key] = value
        elif isinstance(data, models.Model) and hasattr(data, self.key):
            data[self.key] = value

        return vals

    def _regexp_replace(self, value, vals):
        if not self.param1 or not value:
            return False
        pattern = re.compile(self.param1) if self.param1 else '.*'
        repl = self.param2.format(**vals) if self.param2 else ''
        count = int(self.param3)
        return re.sub(pattern, repl, value, count=count)

    def _orm_search(self, vals):
        if self.env['ir.model'].search([('model', '=', self.param1)]):
            model = self.env[self.param1]
        else:
            return None
        domain = []
        domain_match = (
            re.search(r'\[.*\]', self.param2.format(**vals)) if self.param2
            else False
        )
        if domain_match:
            domain_list = eval(domain_match.group())
            if isinstance(domain_list, list):
                domain = domain_list
        limit = int(self.param3) if self.param3 else None
        return model.search(domain, limit=limit)

    def _orm_filter_map(self, records, vals):
        if isinstance(records, models.Model):
            f_filter = self._get_lambda(self.param1, vals)
            f_map = self._get_lambda(self.param2, vals)
            if f_filter:
                records = records.filtered(f_filter)
            if f_map:
                records = records.mapped(f_map)
            return records
        else:
            return None

    @api.model
    def _get_lambda(self, lambda_str, vals={}):
        if not isinstance(lambda_str, str):
            return False
        match = re.search(r'lambda [a-z]+:.*', lambda_str.format(**vals))
        if match:
            f = eval(match.group())
            is_callable = callable(f)
            return f if is_callable else is_callable
        return False
