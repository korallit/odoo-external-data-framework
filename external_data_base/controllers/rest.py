# coding: utf-8

from odoo.http import Controller, request, route
from odoo.exceptions import MissingError, UserError


class ExternalDataController(Controller):

    paths = [
        '/external-data/',
        '/external-data/strategy/<int:strategy_id>',
        '/external-data/strategy/<int:strategy_id>/<string:resource>',
        '/external-data/strategy/<int:strategy_id>/<string:resource>/'  # >
        '<int:res_id>',
        '/external-data/strategy/<string:strategy_slug>',
        '/external-data/strategy/<string:strategy_slug>/<string:resource>',
        '/external-data/strategy/<string:strategy_slug>/<string:resource>/' # >
        '<int:res_id>',
    ]

    @route(paths, type='json', auth='api_key')
    def external_data(self, **params):

        env = request.env
        # path = request.httprequest.path
        method = request.httprequest.method

        # merge params from query string, path and body
        params.update(request.httprequest.args)

        # strategy & resource params
        strategy_id = params.get('strategy_id')
        strategy_type = params.get('strategy_type')
        strategy_slug = params.get('strategy_slug')
        resource = params.get('resource', 'info')
        res_id = params.get('res_id')

        # pagination & search
        limit = params.get('page_size')
        page = params.get('page', 0)
        offset = page * limit if limit else 0

        # find strategy
        result = {'resource': resource}
        domain = [('exposed', '=', True)]
        if strategy_type:
            domain.append(('operation', '=', strategy_type))
        strategies = env['external.data.strategy'].search(
            domain, limit=limit, offset=offset,
        )
        strategy = False
        if type(strategy_id) == int:
            result['strategy_id'] = strategy_id
            domain_new = domain + [('id', '=', strategy_id)]
            strategy = strategies.search(domain_new, limit=1)
        if not strategy and strategy_slug:
            result['strategy_slug'] = strategy_slug
            domain_new = domain + [('slug', '=', strategy_slug)]
            strategy = strategies.search(domain_new, limit=1)
        if strategy:
            fields = [
                'id', 'name', 'slug',
                'operation',
                'batch_size', 'prune_vals',
            ]
            result['strategy'] = strategy.read(fields)[0]
            result['strategy'].update({
                'mappings':
                [{
                    'id': m.id,
                    'name': m.name,
                    'model': m.model_model,
                    'foreign_type': m.foreign_type_id.name,
                } for m in strategy.field_mapping_ids]
            })

        if method == 'GET':
            if not strategy:
                fields = ['id', 'name', 'slug', 'operation']
                result['strategies'] = strategies.read(fields)
                return result
            elif resource == 'info':
                return result
            elif resource == 'items' or (resource == 'item' and res_id):
                metadata = {}
                data = [
                    vals for vals in
                    strategy._gather_items(
                        metadata, res_id,
                        prune_implicit=params.get('prune_implicit'),
                    )
                ]
                if res_id and data:
                    data = data[0]
                result['data'] = data
                if params.get('include_metadata'):
                    result['metadata'] = metadata
                return result
            else:
                raise UserError(f"Invalid resource: {resource}")
        raise UserError(f"Invalid method: {method}")
