# coding: utf-8

from datetime import datetime

from odoo import api, fields, models
from odoo.fields import Command


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
    type_mapping_line_ids = fields.Many2many(
        'webscrape.type.mapping.line',
        string="Content types",
    )
    relation_ids = fields.One2many(
        'webscrape.relation',
        inverse_name='page_id',
        string="Related records",
    )
    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='set null',
        string="Site",
    )

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
        type_mapping_line_ids = self.site_id.type_mapping_line_ids.filtered(
            lambda t: t.name in found_content_types
        )
        if type_mapping_line_ids:
            self.type_mapping_line_ids = type_mapping_line_ids
            if sync:
                self.sync_related_records(dataset)
                self.last_scraped = datetime.now()
            if prune:
                self.prune_related_records(dataset)

        return dataset

    def sync_related_records(self, dataset):
        self.ensure_one()
        type_mapping_lines = self.site_id.type_mapping_line_ids
        field_mapping_lines_all = self.site_id.field_mapping_line_ids
        for data in dataset:
            # get field mapping and create 'vals' dictionary
            try:
                type_mapping = type_mapping_lines.filtered(
                    lambda t: t.name == data['content_type']
                )
                if not type_mapping:
                    raise ValueError(
                        "Couldn't find type mapping for content type '%s'!" %
                        data['content_type']
                    )
                field_mapping_lines = field_mapping_lines_all.filtered(
                    lambda f: f.type_mapping_line_id == type_mapping.id
                )
            except KeyError as e:
                raise e

            vals = {}
            for mapping in field_mapping_lines:
                if mapping.is_relation:
                    rel_data = data["relations"].get(mapping.source_key)
                    parent_relation = self.relation_ids.filtered(
                        lambda r: (
                            r.source_id == rel_data["source_id"] and
                            r.type_mapping_id.name == rel_data["content_type"]
                        )
                    )
                    if parent_relation:
                        related_record = parent_relation[0].related_record
                        vals.update({
                            mapping.target_field.name: related_record.id,
                        })

                else:
                    vals.update({
                        mapping.target_field.name:
                        data["vals"].get(mapping.source_key)
                    })

            # try to find record by source_id
            source_id = data["vals"].get(type_mapping.source_id_key)
            relation = self.relation_ids.filtered(
                lambda r: r.source_id == source_id
            )

            # update/create record
            if relation:
                record = relation[0].related_record
                record.write(vals)
            else:
                record = self.env[type_mapping.model_id.model].create(vals)
                self.relation_ids.create({
                    "page_id": self.id,
                    "type_mapping_line_id": type_mapping.id,
                    "related_record": record.id,
                    "source_id": source_id,
                })

    def prune_related_records(self, dataset):
        self.ensure_one()
        source_ids = [
            data['source_id']
            for data in dataset if data.get('source_id')
        ]
        unrelated_records = self.relation_ids.filtered(
            lambda r: r.source_id not in source_ids
        )
        return unrelated_records.unlink()


class WebscrapeRelation(models.Model):
    _name = 'webscrape.relation'
    _description = "Webscraper Page/Object Relations"

    page_id = fields.Many2one(
        'webscrape.page',
        ondelete='cascade',
        string="Page",
        required=True,
    )
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='restrict',
        string="Content type",
        required=True,
    )
    related_model = fields.Char(
        related='type_mapping_line_id.model_id.model',
        store=True,
    )
    related_record = fields.Many2oneReference(
        "Related record",
        model_field='related_model',
        required=True,
    )
    source_id = fields.Char(
        "Source ID",
        help="A unique identifier that helps the CRUD methods "
        "to match the source object with the targeted record.",
        required=True,
    )
    priority = fields.Integer("Priority", default=100)


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
    field_mapping_line_ids = fields.One2many(
        'webscrape.field.mapping.line',
        inverse_name='site_id',
        string="Mapping line",
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

    def process_sitemap(self):
        self.ensure_one()
        scraper = self.env[self.scraper_model_id.model]
        # TODO: check scraper model method availability
        sitemap = scraper.process_sitemap(
            base_url=self.base_url,
            site_id=self.id,
            not_before=self.last_fetch,
        )
        if not self.last_fetch:
            pages = self.env['webscrape.page'].create(sitemap)
            self.last_fetch = datetime.now()
            return sitemap, pages.ids

        page_ids = []
        for page_data in sitemap:
            page = self.env['webscrape.page'].search(
                [('url', '=', page_data['url'])],
                limit=1,
            )
            if page:
                if page.write(page_data):
                    page_ids.append(page.id)
            else:
                page_ids.append(page.create(page_data).id)
        return sitemap, page_ids

    def batch_scrape(self, type_mapping_id=False, sync=False, prune=False):
        domain = [('site_id', '=', self.id)]
        if type_mapping_id:
            domain.append(('type_mapping_line_ids', '=', type_mapping_id))

        scrape_res = []
        pages = env['webscrape.page'].search(domain).filtered(
            lambda p:
            not p.last_scrape or not p.last_mod or p.last_scrape < p.last_mod
        )
        i = 0
        for page in pages:
            if i == self.batch_size:
                return scrape_res
            # TODO: log info
            scrape_res.append(page.scrape_page(sync=sync, prune=prune))
            i += 1


class WebscrapeTypeMappingLine(models.Model):
    _name = 'webscrape.type.mapping.line'
    _description = "Webscraper Content Types"

    name = fields.Char(required=True)
    model_id = fields.Many2one(
        'ir.model',
        ondelete='cascade',
        string="Model",
        required=True,
    )
    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='cascade',
        string="Site",
        required=True,
    )
    source_id_key = fields.Char(
        "Source ID key",
        required=True,
    )

    def batch_scrape(self, sync=False, prune=False):
        return self.site_id.batch_scrape(
            type_mapping_id=self.id,
            sync=sync, prune=prune,
        )


class WebscrapeFieldMappingLine(models.Model):
    _name = 'webscrape.field.mapping.line'
    _description = "Webscraper Field Mapping Line"
    _rec_name = 'source_key'

    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='cascade',
        string="Site",
        required=True,
    )
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='cascade',
        string="Content type",
        required=True,
    )
    type_mapping_model_id = fields.Integer(
        related="type_mapping_line_id.model_id.id",
    )
    source_key = fields.Char("Source key", required=True)
    target_field = fields.Many2one(
        'ir.model.fields',
        ondelete='set null',
        string="Target field",
    )
    is_relation = fields.Boolean("Relational")
