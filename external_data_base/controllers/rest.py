# coding: utf-8

from odoo.http import Controller, request, route
from odoo.exceptions import MissingError, UserError


class ExternalDataController(Controller):

    paths = [
        '/external-data/',
        '/external-data/<string:resource>',
        '/external-data/<string:resource>/<int:res_id>',
        '/external-data/strategy/<int:strategy_id>',
        '/external-data/strategy/<int:strategy_id>/<string:resource>',
        '/external-data/strategy/<int:strategy_id>/<string:resource>/'  # >
        '<int:res_id>',
        '/external-data/mapping/<int:mapping_id>',
        '/external-data/mapping/<int:mapping_id>/<string:resource>',
        '/external-data/mapping/<int:mapping_id>/<string:resource>'  # >
        '<int:res_id>',
    ]

    @route(paths, type='json', auth='api_key')
    def strategies(self, **params):

        method = request.httprequest.method
        # path = request.httprequest.path
        params.update(request.httprequest.args)
        env = request.env
        res_id = params.get('res_id')
        strategy_id = params.get('strategy_id')
        mapping_id = params.get('mapping_id')
        resource = params.get('resource', 'info')

        # pagination & search
        limit = params.get('page_size')
        page = params.get('page', 0)
        offset = page * limit if limit else 0
        domain = []
        if not params.get('show_all'):
            domain.append(('operation', '=', 'rest'))

        strategies = env['external.data.strategy'].search(
            domain, limit=limit, offset=offset,
        )
        strategy = strategies.browse(strategy_id).exists()
        mapping = strategy.field_mapping_ids.filtered(
            lambda r: r.id == mapping_id
        )

        res = {'resource': resource}
        if strategy_id:
            res['strategy_id'] = strategy_id
        if mapping_id:
            res['mapping_id'] = mapping_id
        if strategy:
            fields = [
                'id', 'name',
                'batch_size', 'prune_vals',
                'field_mapping_ids',
                'operation',
            ]
            res['strategy'] = strategy.read(fields)[0]
        if mapping:
            res['mapping'] = mapping.read(['id', 'name'])[0]
            res['mapping'].update({
                'odoo_model': mapping.model_model,
                'foreign_type': mapping.foreign_type_id.name,
            })

        if method == 'GET':
            if not strategy:
                fields = ['id', 'name', 'operation']
                res['strategies'] = strategies.read(fields)
                return res
            elif resource == 'info':
                return res
            elif resource == 'mappings':
                res.update({
                    'mappings':
                    [{
                        'id': m.id,
                        'name': m.name,
                        'odoo_model': m.model_model,
                        'foreign_type': m.foreign_type_id.name,
                    } for m in strategy.field_mapping_ids]
                })
                return res
            elif resource == 'items' or (resource == 'item' and res_id):
                if not strategy.operation == 'rest':
                    raise MissingError("Please select a valid REST strategy!")
                metadata = {}
                data = [
                    vals for vals in
                    strategy._gather_items(metadata, mapping_id, res_id)
                ]
                if res_id and data:
                    data = data[0]
                res['data'] = data
                if params.get('include_metadata'):
                    res['metadata'] = metadata
                return res

        raise UserError(f"Invalid method: {method}")
