# coding: utf-8

from odoo.http import Controller, request, route
from odoo.exceptions import MissingError, UserError


class ExternalDataController(Controller):

    json_paths = [
        '/external-data/json',
        '/external-data/json/<int:strategy_id>',
        '/external-data/json/<int:strategy_id>/<string:resource>',
        '/external-data/json/<int:strategy_id>/<string:resource>/'  # >
        '<int:res_id>',
        '/external-data/json/<string:strategy_slug>',
        '/external-data/json/<string:strategy_slug>/<string:resource>',
        '/external-data/json/<string:strategy_slug>/<string:resource>/' # >
        '<int:res_id>',
    ]

    @route(json_paths, type='json', auth='api_key')
    def external_data_json(self, **params):

        # merge params from query string, path and body
        params.update(request.httprequest.args)
        self.params = params

        # init result dict
        resource = params.get('resource', 'info')
        self.result = {
            'input': {'resource': resource},
        }

        # find strategy
        self._set_strategy()

        # process request
        # path = request.httprequest.path
        method = request.httprequest.method
        if method == 'GET':
            if not self.strategy or resource == 'strategies':
                self._set_strategies()
            elif resource == 'info':
                self._get_info()
            elif resource == 'resources':
                self._get_resources()
            elif resource == 'items' or (resource == 'item' and res_id):
                self._get_items()
            else:
                raise UserError(f"Invalid resource: {resource}")
            return self.result

        raise UserError(f"Invalid method: {method}")

    def _get_pagination(self):
        page = int(self.params.get('page', 0))
        limit = int(self.params.get('page_size', 10))
        offset = limit * page
        self.result['pagination'] = {
            'page_size': limit,
            'requested_page': offset,
        }
        return limit, offset

    def _get_strategy_domain(self):
        domain = [('exposed', '=', True)]
        str_type = self.params.get('strategy_type')
        if str_type:
            self.result['input'].update(strategy_type=str_type)
            domain.append(('operation', '=', str_type))
        return domain

    def _set_strategies(self):
        domain = self._get_strategy_domain()
        fields = ['id', 'name', 'slug', 'operation']
        self.result['strategies'] = self.strategy.search(domain).read(fields)

    def _set_strategy(self):
        str_id = self.params.get('strategy_id')
        str_slug = self.params.get('strategy_slug')

        domain = self._get_strategy_domain()
        strategy = request.env['external.data.strategy']
        if type(str_id) == int:
            self.result['input'].update(strategy_id=str_id)
            strategy = strategy.search(domain).filtered(
                lambda str: str.id == str_id)
        elif str_slug:
            self.result['input'].update(strategy_slug=str_slug)
            strategy = strategy.search(domain).filtered(
                lambda str: str.slug == str_slug)
        if strategy:
            strategy = strategy[0]
            fields = [
                'name', 'slug', 'operation',
                'batch_size', 'prune_vals',
            ]
            self.result['strategy'] = {
                'id': strategy.id,
                'slug': strategy.slug,
                'details': strategy.read(fields)[0],
            }
        self.strategy = strategy

    def _get_info(self):
        page_size, _ = self._get_pagination()
        info = self.result['strategy']['details']
        info['data_source'] = {
            'id': self.strategy.data_source_id.id,
            'name': self.strategy.data_source_id.name,
        }
        item_count = 0
        mappings = self.strategy.field_mapping_ids
        if mappings:
            info['mappings'] = [
                {
                    'id': m.id,
                    'name': m.name,
                    'model': m.model_model,
                    'foreign_type': m.foreign_type_id.name,
                } for m in mappings
            ]
            item_count = mappings[0].record_count
            total_pages = int(item_count / page_size) + 1 if item_count else 0
            info['items'] = {
                'total': item_count,
                'total_pages': total_pages,
            }
            res_count = len(self.strategy.data_source_id.resource_ids)
            total_pages = int(res_count / page_size) + 1 if item_count else 0
            info['resources'] = {
                'total': res_count,
                'total_pages': total_pages,
            }
        
    def _get_items(self):
        res_id = self.params.get('res_id')
        limit, offset = self._get_pagination()
        metadata = {}
        self.result['strategy']['items'] = [
            vals for vals in
            self.strategy._gather_items(
                metadata, res_id=res_id, limit=limit, offset=offset,
                prune_implicit=self.params.get('prune_implicit'),
            )
        ]
        if self.params.get('include_metadata'):
            self.result['metadata'] = metadata

    def _get_resources(self):
        domain = [
            ('data_source_id', '=', self.strategy.data_source_id.id),
        ]
        limit, offset = self._get_pagination()
        resources = request.env['external.data.resource'].search(
            domain, limit=limit, offset=offset)
        self.result['strategy']['resources'] = [
            {
                'id': res.id,
                'name': res.name,
                'url': res.url,
                'external_objects': len(res.object_ids),
            } for res in resources
        ]
