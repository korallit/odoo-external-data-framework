# coding: utf-8

from odoo import models, fields


class WebscrapeSite(models.Model):
    _name = 'webscrape.site'

    url = fields.Char("URL", required=True)
    name = fields.Char("Name", required=True)
    level = fields.Integer("Tree level", required=True, default=0)
    changefreq = fields.Char("Change frequency")
    priority = fields.Float("Priority")
    last_mod = fields.Datetime("Last modification time")
    last_scrape = fields.Datetime("Last scrape time")
    content_type = fields.Char("Content type")
    relation_ids = fields.One2many(
        'webscrape.relation',
        inverse_name='site_id',
        string="Related records",
    )
    scraper_id = fields.Many2one(
        'webscrape.scraper',
        ondelete='set null',
        string="Scraper",
    )

    def scrape_site(self, sync=False, prune=False):
        self.ensure_one()
        # TODO: check scraper model method availability
        dataset = self.scraper_id.scraper_model_id.scrape_site(self.url)
        if sync:
            self.sync_related_records(dataset)
        if prune:
            self.prune_related_records(dataset)
        return dataset

    def sync_related_records(self, dataset):
        self.ensure_one()
        type_mapping_lines = self.scraper_id.type_mapping_line_ids
        field_mapping_lines_all = self.scraper_id.field_mapping_line_ids
        for data in dataset:
            # get source id, raise exception if missing
            try:
                source_id = data['source_id']
            except KeyError as e:
                raise e

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

            vals = {
                mapping.target_field.name: data.get(mapping.source_key)
                for mapping in field_mapping_lines
            }

            # try to find record by source_id
            relation = self.relation_ids.filtered(
                lambda r: r.source_id == source_id
            )

            # update/create record
            if relation:
                record = relation[0].related_record
                record.write(vals)
            else:
                record = type_mapping.model_id.create(vals)
                self.relation_ids.create({
                    "site_id": self.id,
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

    site_id = fields.Many2one(
        'webscrape.site',
        ondelete='cascade',
        string="Site",
        required=True,
    )
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='restrict',
        string="Content type",
        required=True,
    )
    related_model = fields.Char(
        related='type_mapping_line_id.model_id._name',
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


class WebscrapeScraper(models.Model):
    _name = 'webscrape.scraper'

    name = fields.Char(required=True)
    base_url = fields.Char("Base URL", required=True)
    scraper_model_id = fields.Many2one(
        'ir.model.model',
        ondelete='cascade',
        string="Scraper model",
        required=True,
    )
    field_mapping_line_ids = fields.One2many(
        'webscrape.field.mapping.line',
        inverse_name='scraper_id',
        string="Mapping line",
    )
    type_mapping_line_ids = fields.One2many(
        'webscrape.type.mapping.line',
        inverse_name='scraper_id',
        string="Content type",
    )
    vendor_id = fields.Many2one(
        'res.partner',
        ondelete='set null',
        string="Vendor",
    )
    batch_size = fields.Integer("Batch size", default=10)

    def process_sitemap(self):
        self.ensure_one()
        # TODO: check scraper model method availability
        return self.scraper_model_id.process_sitemap(
            base_url=self.base_url,
            scraper_id=self.id,
        )


class WebscrapeTypeMappingLine(models.Model):
    _name = 'webscrape.type.mapping.line'

    name = fields.Char(required=True)
    model_id = fields.Many2one(
        'ir.model.model',
        ondelete='cascade',
        string="Model",
        required=True,
    )
    scraper_id = fields.Many2one(
        'webscrape.scraper',
        ondelete='cascade',
        string="Scraper",
        required=True,
    )


class WebscrapeFieldMappingLine(models.Model):
    _name = 'webscrape.field.mapping.line'

    scraper_id = fields.Many2one(
        'webscrape.scraper',
        ondelete='cascade',
        string="Scraper",
        required=True,
    )
    source_key = fields.Char("Source key", required=True)
    target_field = fields.Many2one(
        'ir.model.field',
        ondelete='set null',
        string="Target field",
        required=True,
    )
    type_mapping_line_id = fields.Many2one(
        'webscrape.type.mapping.line',
        ondelete='set null',
        string="Content type",
    )
