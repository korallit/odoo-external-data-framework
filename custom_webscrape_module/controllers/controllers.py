# -*- coding: utf-8 -*-
# from odoo import http


# class OdooWebscrapeModule(http.Controller):
#     @http.route('/odoo_webscrape_module/odoo_webscrape_module/', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/odoo_webscrape_module/odoo_webscrape_module/objects/', auth='public')
#     def list(self, **kw):
#         return http.request.render('odoo_webscrape_module.listing', {
#             'root': '/odoo_webscrape_module/odoo_webscrape_module',
#             'objects': http.request.env['odoo_webscrape_module.odoo_webscrape_module'].search([]),
#         })

#     @http.route('/odoo_webscrape_module/odoo_webscrape_module/objects/<model("odoo_webscrape_module.odoo_webscrape_module"):obj>/', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('odoo_webscrape_module.object', {
#             'object': obj
#         })
