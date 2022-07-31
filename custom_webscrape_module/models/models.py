# -*- coding: utf-8 -*-

import base64
import requests as req
from bs4 import BeautifulSoup as bs

from odoo import models, api, fields
from odoo.exceptions import ValidationError


# default Image if no image is found
# urltemp = 'https://beresbence.cdn.shoprenter.hu/Custom/beresbence/image/cache/w345h435wt1/no_image.jpg?lastmod=0.1570780153'


class OdooWebscrape(models.Model):
    _name = 'update.product.price'
    base_url = 'https://www.meleget.hu/sitemap.xml'
    exception = ['https://www.meleget.hu', 'Kezdőlap']

    @api.depends('update_all')
    def _get_total_products(self):
        xmldata = bs(req.get(self.base_url).text)
        all_url_tag = xmldata.find_all('url')
        self.total_number_of_batch = len(all_url_tag)

    update_all = fields.Boolean('Update All')
    update_products = fields.Many2many('product.template')
    total_number_of_batch = fields.Integer(
        string="Total Number of Products Found",
        compute=_get_total_products
    )
    start_batch_size = fields.Integer('Start From', default=0)
    end_batch_size = fields.Integer('End To', default=0)

    def product_details(self, productpageurl, search_related=True):
        page_data = bs(req.get(productpageurl).text)
        all_category = bs(
            str(page_data.find_all('div', {'class': 'pathway_inner'}))
        ).find_all('span', {'itemprop': 'name'})
        if all_category:
            cat_list = []
            for cat in all_category[1:-1]:
                cat_list.append(cat.text)
            product_name = all_category[-1].text
        product_price = page_data.find('meta', {'itemprop': 'price'})
        if product_price:
            product_price = page_data.find(
                'meta', {'itemprop': 'price'}
            )['content']
        else:
            return
        try:
            product_price = float(product_price)
            description = page_data.find('div', {'id': 'productdescription'})
            media_images = page_data.find_all(
                'div', {'class': 'productimages'}
            )
            media_images = bs(str(media_images)).find_all('a')
            image_links = []
            for i in media_images:
                image_links.append(i['href'])
        except Exception:
            raise ValidationError("string cannot be type of product")

        return {
            'category': cat_list or [],
            'name': product_name,
            'price': product_price,
            'description': str(description),
            'image_url': page_data.find(
                'img',
                {'id': 'image', 'itemprop': 'image', 'class': 'product-image-img'}
            ).get('src', False),
            'image_links': image_links,
            'related_product': self._sub_product_url(page_data) if search_related else False
        }

    def _sub_product_url(self, page_data):
        url_set = set()
        try:
            for i in page_data.find('table', {'class': 'product_collateral list_with_tables'}).find_all('a'):
                url_set.add(i['href'])
        except Exception as e:
            return list(url_set)
        return list(url_set)

    def _create_cat_on_demand(self, cat_list):
        if not cat_list:
            return
        cat_id = self.env['product.category'].search(
            [('name', '=', cat_list[-1])], limit=1)
        if cat_id:
            return cat_id.id
        else:
            new_cat = self.env['product.category'].create({
                'name': cat_list[-1],
                'parent_id': self._create_cat_on_demand(cat_list[:-1])
            })
            return new_cat.id

    def _create_public_cat_on_demand(self, cat_list):
        if not cat_list:
            return
        public_cat_id = self.env['product.public.category'].search(
            [('name', '=', cat_list[-1])], limit=1)
        if public_cat_id:
            return public_cat_id.id
        else:
            new_public_cat = self.env['product.public.category'].create({
                'name': cat_list[-1],
                'parent_id': self._create_public_cat_on_demand(cat_list[:-1])
            })
            return new_public_cat.id

    def _ready_public_categ_ids(self, cat_list=[]):
        public_cat_id = self._create_public_cat_on_demand(cat_list)
        public_cat_id = self.env['product.public.category'].search(
            [('id', '=', public_cat_id)])
        return [(4, public_cat_id.id)]

    def _create_related_product_on_demand(self, url_list=[]):
        related_product_ids = []
        for url in url_list:
            url_in_db = self.env['product.template'].sudo().search(
                [('product_url', '=', url)], limit=1)
            if url_in_db:
                related_product_ids.append(url_in_db.id)
            else:
                is_product = self.product_details(url, search_related=False)
                if is_product:
                    is_already_in_db = self.env['product.template'].search(
                        [('product_url', '=', url)])
                    if is_already_in_db:
                        related_product_ids.append(is_already_in_db.id)
                        continue
                    cat = self._create_cat_on_demand(is_product['category'])
                    cat_id = self.env['product.category'].search(
                        [('id', '=', cat)])
                    public_categ_ids = self._ready_public_categ_ids(is_product['category'])
                    data = {
                        'name': is_product['name'],
                        'categ_id': cat_id.id,
                        'public_categ_ids': public_categ_ids,
                        'list_price': is_product['price'],
                        'product_url': url,
                        'last_sync': fields.Datetime.now(),
                        'desciption': is_product['description'],
                    }
                    if is_product.get('image_url'):
                        data['image_1920'] = base64.b64encode(req.get(is_product['image_url']).content)

                    product = self.env['product.template'].create(data)
                    self.env['product.pricelist.item'].sudo().create({
                        'product_tmpl_id': product.id,
                        'applied_on': '0_product_variant',
                        'product_id': self.env['product.product'].search([('product_tmpl_id', '=', product.id)]).id,
                        'min_quantity': 1,
                        'fixed_price': data['list_price'],
                        'company_id': self.env.company.id
                    })
                    product_image = self.env['product.image']
                    for index, medial_url in enumerate(is_product['image_links']):
                        product_image.create({
                            'name': 'Image_' + str(index + 1),
                            'image_1920': base64.b64encode(req.get(medial_url).content),
                            'product_tmpl_id': product.id
                        })
                    related_product_ids.append(product.id)
        if related_product_ids:
            return [(4, x) for x in related_product_ids]
        return related_product_ids

    def browse_url(self):
        xmldata = bs(req.get(self.base_url).text)
        all_url_tag = xmldata.find_all('url')
        start_index = self.start_batch_size
        end_index = len(all_url_tag)
        if self.end_batch_size:
            end_index = self.end_batch_size
        for url in all_url_tag[start_index:end_index]:
            if url in self.exception:
                continue
            url_in_db = self.env['product.template'].sudo().search([('product_url', '=', url.loc.text)], limit=1)
            if url_in_db:
                continue
            got_product = self.product_details(url.loc.text)
            if got_product:
                has_record = self.env['product.template'].search([('name', '=', got_product['name'])])
                if has_record:
                    continue
                cat = self._create_cat_on_demand(got_product['category'])
                cat_id = self.env['product.category'].search([('id', '=', cat)])
                public_categ_ids = self._ready_public_categ_ids(got_product['category'])
                alternative_prodcut_ids = self._create_related_product_on_demand(got_product['related_product'])

                data = {
                    'name': got_product['name'],
                    'categ_id': cat_id.id,
                    'public_categ_ids': public_categ_ids,
                    'list_price': got_product['price'],
                    'product_url': url.loc.text,
                    'last_sync': fields.Datetime.now(),
                    'desciption': got_product['description'],
                }
                if alternative_prodcut_ids:
                    data['alternative_product_ids'] = alternative_prodcut_ids
                if got_product.get('image_url'):
                    data['image_1920'] = base64.b64encode(req.get(got_product['image_url']).content)

                product = self.env['product.template'].create(data)
                self.env['product.pricelist.item'].sudo().create({
                    'product_tmpl_id': product.id,
                    'applied_on': '0_product_variant',
                    'product_id': self.env['product.product'].search([('product_tmpl_id', '=', product.id)]).id,
                    'min_quantity': 1,
                    'fixed_price': data['list_price'],
                    'company_id': self.env.company.id
                })
                product_image = self.env['product.image']
                for index, medial_url in enumerate(got_product['image_links']):
                    product_image.create({
                        'name': 'Image_' + str(index + 1),
                        'image_1920': base64.b64encode(req.get(medial_url).content),
                        'product_tmpl_id': product.id
                    })

    def update_product_price(self):
        self.browse_url()
        # self._create_cat_on_demand([chr(i) for i in range(65,70)])


class ProductTemplateInherit(models.Model):
    _inherit = 'product.template'

    product_url = fields.Char("Product URL")
    last_sync = fields.Datetime("Last Sync")
    desciption = fields.Html("Description")

# from bs4 import BeautifulSoup as bs
# import requests as req
#
# base_url = 'https://www.meleget.hu/sitemap.xml'
# exception = ['https://www.meleget.hu', 'Kezdőlap']
# def productDetails(productpageurl):
#
#     page_data = bs(req.get(productpageurl).text)
#     all_category = bs(str(page_data.find_all('div', {'class': 'pathway_inner'}))).find_all('span',
#                                                                                            {'itemprop': 'name'})
#     if all_category:
#         cat_list = []
#         for cat in all_category[1:-1]:
#             cat_list.append(cat.text)
#         product_name = all_category[-1].text
#     product_price = page_data.find('meta', {'itemprop': 'price'})
#     if product_price:
#         product_price = page_data.find('meta', {'itemprop': 'price'})['content']
#     else:
#         return
#     try:
#         product_price = float(product_price)
#     except:
#         raise ValidationError("string cannot be type of product")
#
#     return {
#         'category': cat_list or [],
#         'name': product_name,
#         'price': product_price or 0.0
#     }
#
# def browseURL():
#     xmldata = bs(req.get(base_url).text)
#     all_url_tag = xmldata.find_all('url')
#     i = 0
#     for url in all_url_tag:
#         if url in exception:
#             continue
#         print(url.loc.text)
#         val = productDetails(url.loc.text)
#         if val:
#             print(val)
