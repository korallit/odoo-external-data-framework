# coding: utf-8

from odoo.http import Controller, request, route
from odoo.exceptions import UserError

import logging
_logger = logging.getLogger(__name__)


class ExternalDataController(Controller):

    valid_resources = [
        'strategies', 'data-sources', 'info',
        'items', 'resources',
        'item', 'resource',
    ]
    json_paths = [
        '/external-data/json',
        '/external-data/json/<string:resource>',
        '/external-data/json/<string:resource>/<int:res_id>',
    ]
    http_paths = [
        '/external-data',
        # all slugs
        '/external-data/<string:data_source>',
        '/external-data/<string:data_source>/<string:strategy>',
        '/external-data/<string:data_source>/<string:strategy>/'
        '<string:resource>',
        '/external-data/<string:data_source>/<string:strategy>/'
        '<string:resource>/<int:res_id>',
        # strategy by ID
        '/external-data/<string:data_source>/strategy/<int:strategy_id>',
        '/external-data/<string:data_source>/strategy/<int:strategy_id>/'
        '<string:resource>',
        '/external-data/<string:data_source>/strategy/<int:strategy_id>/'
        '<string:resource>/<int:res_id>',
        # data source by ID
        '/external-data/data-source/<int:data_source_id>',
        '/external-data/data-source/<int:data_source_id>/<string:strategy>',
        '/external-data/data-source/<int:data_source_id>/<string:strategy>/'
        '<string:resource>',
        '/external-data/data-source/<int:data_source_id>/<string:strategy>/'
        '<string:resource>/<int:res_id>',
        # all IDs
        '/external-data/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>',
        '/external-data/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>/<string:resource>',
        '/external-data/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>/<string:resource>/<int:res_id>',
    ]
    web_paths = [
        '/external-data/web',
        # all slugs
        '/external-data/web/<string:data_source>',
        '/external-data/web/<string:data_source>/<string:strategy>',
        '/external-data/web/<string:data_source>/<string:strategy>/'
        '<string:resource>',
        '/external-data/web/<string:data_source>/<string:strategy>/'
        '<string:resource>/<int:res_id>',
        '/external-data/web/<string:data_source>/<string:strategy>/'
        '<string:resource>/<string:export_filename>',
        # strategy by ID
        '/external-data/web/<string:data_source>/strategy/<int:strategy_id>',
        '/external-data/web/<string:data_source>/strategy/<int:strategy_id>/'
        '<string:resource>',
        '/external-data/web/<string:data_source>/strategy/<int:strategy_id>/'
        '<string:resource>/<int:res_id>',
        # data source by ID
        '/external-data/web/data-source/<int:data_source_id>',
        '/external-data/web/data-source/<int:data_source_id>/'
        '<string:strategy>',
        '/external-data/web/data-source/<int:data_source_id>/'
        '<string:strategy>/<string:resource>',
        '/external-data/web/data-source/<int:data_source_id>/'
        '<string:strategy>/<string:resource>/<int:res_id>',
        # all IDs
        '/external-data/web/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>',
        '/external-data/web/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>/<string:resource>',
        '/external-data/web/data-source/<int:data_source_id>/strategy/'
        '<int:strategy_id>/<string:resource>/<int:res_id>',
    ]

    @route(json_paths, type='json', auth='api_key')
    def external_data_json(self, **params):
        # merge params from query string, path and body
        params.update(request.httprequest.args)
        self.params = params
        metadata = {}
        self._process_request(metadata)
        return self.result

    @route(http_paths, type='http', auth='api_key', csrf=False)
    def external_data_http(self, **params):
        # merge params from query string, path and body
        self.params = params
        metadata = {}
        self._process_request(metadata)
        result_str = self._serialize_result(metadata)
        return request.make_response(result_str)

    @route(web_paths, type='http', auth='user')
    def external_data_web(self, **params):
        # merge params from query string, path and body
        self.params = params
        metadata = {}
        self._process_request(metadata)
        result_str = self._serialize_result(metadata)
        return request.make_response(result_str)

    def _serialize_result(self, metadata={}):
        renderer = self.strategy.serializer_id
        data = False
        if renderer:
            data = renderer.render(self.result, metadata, key="items")
        else:  # fallback to json
            data = renderer.render_json(self.result)

        if data:
            return data
        msg = "No data produced"
        _logger.error(msg)
        return msg

    def _process_request(self, metadata={}):
        # get resource label from params
        resource = self.params.get('resource')
        self.result = {}
        self.result['input'] = {'resource': resource}

        # find strategy and set resource if not found it healthy enough
        free_resources = ['list', 'data-sources', 'strategies']
        self._set_strategy()
        if not self.strategy and (resource not in free_resources):
            resource = 'list'
            self.result['message'] = "No strategy found, fallback to 'list'"
        elif self.strategy and (resource not in self.valid_resources):
            self.result['message'] = (
                "No valid resource found, fallback to 'info'. \n"
                f"Valid resource types: {', '.join(self.valid_resources)}"
            )
            resource = 'info'
        self.result['resource'] = resource

        # path = request.httprequest.path
        method = request.httprequest.method
        if method in ['GET', 'POST']:
            if resource == 'list':
                self._get_data_sources()
                self._get_strategies()
            elif resource == 'strategies':
                self._get_strategies()
            elif resource == 'data-sources':
                self._get_data_sources()
            elif resource == 'info':
                self._get_info()
            elif resource == 'resources':
                self._get_resources()
            elif (resource == 'items' or
                  (resource == 'item' and self.params.get('res_id'))):
                self._get_items(metadata)
            else:
                raise UserError(f"Invalid resource: {resource}")
        else:
            raise UserError(f"Invalid method: {method}")

    def _get_pagination(self):
        page = int(self.params.get('page', 0))
        limit = int(self.params.get('page_size', 10))
        offset = limit * page
        self.result['pagination'] = {
            'page_size': limit,
            'requested_page': page,
        }
        return limit, offset

    def _get_strategy_domain(self):
        domain = [('exposed', '=', True)]
        str_type = self.params.get('strategy_type')
        data_source_id = self.params.get('data_source_id')
        data_source_slug = self.params.get('data_source')
        # TODO: data_source slug
        if str_type:
            self.result['input'].update(strategy_type=str_type)
            domain.append(('operation', '=', str_type))
        if data_source_id:
            self.result['input'].update(data_source_id=data_source_id)
            domain.append(('data_source_id', '=', data_source_id))
        elif data_source_slug:
            self.result['input'].update(data_source_slug=data_source_slug)
            domain.append(('data_source_id.slug', '=', data_source_slug))
        return domain

    def _get_strategies(self):
        domain = self._get_strategy_domain()
        fields = ['id', 'name', 'slug', 'operation']
        self.result['strategies'] = self.strategy.search(domain).read(fields)

    def _get_data_sources(self):
        self.result['data_sources'] = [
            {
                'id': ds.id,
                'name': ds.name,
            }
            for ds in request.env['external.data.source'].search([])
        ]

    def _get_data_source(self):
        ds_id = self.params.get('data_source_id')
        ds_slug = self.params.get('data_source')
        ds = request.env['external.data.source']
        if ds_id:
            return ds.browse(ds_id).exists()
        elif ds_slug:
            return ds.search([('slug', '=', ds_slug)], limit=1)

    def _set_strategy(self):
        str_id = self.params.get('strategy_id')
        str_slug = self.params.get('strategy')

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
                'name', 'slug', 'operation', 'batch_size',
            ]
            self.result['strategy'] = strategy.read(fields)[0]
        self.strategy = strategy

    def _get_info(self):
        page_size, _ = self._get_pagination()
        node = self.result['strategy']
        node['Data_source'] = {
            'id': self.strategy.data_source_id.id,
            'name': self.strategy.data_source_id.name,
            'slug': self.strategy.data_source_id.slug,
        }
        item_count = 0
        mappings = self.strategy.field_mapping_ids
        if mappings:
            node['mappings'] = [
                {
                    'id': m.id,
                    'name': m.name,
                    'model': m.model_model,
                    'foreign_type': m.foreign_type_id.name,
                    'prune_vals': m.prune_vals,
                } for m in mappings
            ]
            item_count = mappings[0].record_count
            total_pages = int(item_count / page_size) + 1 if item_count else 0
            node['items'] = {
                'total': item_count,
                'total_pages': total_pages,
            }
            res_count = len(self.strategy.data_source_id.resource_ids)
            total_pages = int(res_count / page_size) + 1 if item_count else 0
            node['resources'] = {
                'total': res_count,
                'total_pages': total_pages,
            }

    def _get_items(self, metadata={}):
        res_id = self.params.get('res_id')
        limit, offset = self._get_pagination()
        items = [
            vals for vals in
            self.strategy._gather_items(
                metadata, res_id=res_id, limit=limit, offset=offset,
                prune_implicit=self.params.get('prune_implicit'),
            )
        ]
        renderer = self.strategy.serializer_id
        if renderer:
            items_new = renderer.rearrange(items, metadata)
            if items_new:
                items = items_new
        self.result['items'] = items
        if self.params.get('include_metadata'):
            self.result['metadata'] = metadata

    def _get_resources(self):
        domain = [
            ('data_source_id', '=', self.strategy.data_source_id.id),
        ]
        limit, offset = self._get_pagination()
        resources = request.env['external.data.resource'].search(
            domain, limit=limit, offset=offset)
        self.result['resources'] = [
            {
                'id': res.id,
                'name': res.name,
                'url': res.url,
                'external_objects': len(res.object_ids),
            } for res in resources
        ]
