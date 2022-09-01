# -*- coding: utf-8 -*-
# from odoo import http


# class MelegetHu(http.Controller):
#     @http.route('/meleget_hu/meleget_hu', auth='public')
#     def index(self, **kw):
#         return "Hello, world"

#     @http.route('/meleget_hu/meleget_hu/objects', auth='public')
#     def list(self, **kw):
#         return http.request.render('meleget_hu.listing', {
#             'root': '/meleget_hu/meleget_hu',
#             'objects': http.request.env['meleget_hu.meleget_hu'].search([]),
#         })

#     @http.route('/meleget_hu/meleget_hu/objects/<model("meleget_hu.meleget_hu"):obj>', auth='public')
#     def object(self, obj, **kw):
#         return http.request.render('meleget_hu.object', {
#             'object': obj
#         })
