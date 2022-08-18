# -*- coding: utf-8 -*-
{
    'name': "odoo_webscrape_module",

    'summary': """
        Short (1 phrase/line) summary of the module's purpose, used as
        subtitle on modules listing or apps.openerp.com""",

    'description': """
        Long description of module's purpose
    """,

    'author': "My Company",
    'website': "http://www.yourcompany.com",

    'category': 'Uncategorized',
    'version': '0.1',

    # any module necessary for this one to work correctly
    'depends': ['base', 'external_data_base', 'stock'],

    # always loaded
    'data': [
        # 'security/ir.model.access.csv',
        # 'views/views.xml',
        # 'views/templates.xml',
        # 'views/webscrape.xml',
        # 'actions/webscrape.xml',
        'data/meleget_hu.xml',
        'data/foreign_data_types.xml',
        'data/parsers.xml',
        'data/data_source.xml',
        'data/field_mappings.xml',
    ],
}
