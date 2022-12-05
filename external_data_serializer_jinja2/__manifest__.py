# coding: utf-8
{
    'name': "External Data Serializer Jinja2",

    'summary': """
    Jinja2 serializer for External Data Framework.""",

    'description': """
    Jinja2 serializer for External Data Framework.
    """,

    'author': "grzs",
    'website': "https://korallit.com",

    'category': 'Technical',
    'version': '12.0.9',

    # any module necessary for this one to work correctly
    'depends': ['external_data_base'],

    # always loaded
    'data': [
        'views/external_data_serializer.xml',
    ],
}
