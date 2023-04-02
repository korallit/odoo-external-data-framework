from odoo import api, fields, models
from odoo.exceptions import MissingError, UserError, ValidationError


class ExternalDataWorker(models.AbstractModel):
    _name = 'external.data.worker'
    _description = "External Data Worker Template"

    name = fields.Char(required=True)
    forward_type = fields.Selection(
        "Forward type",
        selection=[
            ('simple', "simple"),
            ('spltter', "splitter"),
            ('funnel', "funnel"),
        ],
        default='simple',
        required=True,
    )
    batch_size = fields.Integer("Batch size", default=0)
    signature = fields.Char(compute='_compute_signature')

    def _compute_signature(self):
        for record in self:
            record.signature = f"{record._name}:{record.id}"

    def process(self, data=None, metadata={}):
        for worker in self:
            processed_data = worker._process(data, metadata)
            if worker.forward_type == 'funnel':
                return worker._process_funnel(processed_data)
            return processed_data

    def _process_funnel(self, data):
        self.ensure_one()
        funnel = self.env['external.data.funnel']
        if funnel.count_items(self.signature) < self.batch_size - 1:
            if not isinstance(data, str) and hasattr(self, '_data_to_str'):
                data = self._data_to_str(data)
            funnel.add(self.signature, data)
            return None
        else:
            dataset = funnel.release(self.signature)
            if hasattr(self, '_data_from_str'):
                dataset = [
                    self._data_from_str(data_str)
                    for data_str in dataset
                ]
            dataset.append(data)
            if hasattr(self, '_compile_dataset'):
                dataset = self._compile_dataset(dataset)
            return dataset

    def _process(self, data, metadata):
        self.ensure_one()
        # worker specific logic
        return data

    # def _data_to_str(self, data):
    #     self.ensure_one()
    #     # worker specific logic
    #     return str(data)

    # def _data_from_str(self, data):
    #     self.ensure_one()
    #     # worker specific logic
    #     return data

    # def _compile_dataset(self, dataset):
    #     self.ensure_one()
    #     # worker specific logic
    #     return dataset


class ExternalDataFunnel(models.TransientModel):
    _name = 'external.data.funnel'
    _description = "Worker Funnel"

    worker_signature = fields.Integer(required=True)
    data = fields.Text(required=True)

    @api.model
    def count_items(model, signature):
        return model.search_count([('worker_signature', '=', signature)])

    @api.model
    def add(model, signature, data):
        if not isinstance(data, str):
            raise ValidationError(
                f"Only string can be added to funnel, got {type(data)}."
                "worker._data_to_str(data) should be implemented!"
            )
        model.create([{
            'worker_signature': signature,
            'data': data,
        }])

    @api.model
    def release(model, signature):
        records = model.search([('worker_signature', '=', signature)])
        dataset = [r.data for r in records]
        records.unlink()
        return dataset


class ExternalDataSubscription(models.Model):
    _name = 'external.data.subscription'
    _description = 'Worker Subscriptions'

    # consumer fields
    consumer_model = fields.Char("Consumer model", required=True)
    consumer_id = fields.Many2oneReference(
        "Consumer", required=True,
        model_field='consumer_model',
    )
    consumer_signature = fields.Char(compute='_compute_signatures', store=True)

    # producer fields
    producer_model = fields.Char("Producer model", required=True)
    producer_id = fields.Many2oneReference(
        "Producer", required=True,
        model_field='producer_model',
    )
    producer_signature = fields.Char(compute='_compute_signatures', store=True)

    # other fields
    data_source_id = fields.Many2one(
        name="Data source", comodel_name='external.data.source',
        required=True, ondelete='cascade',
    )
    direction = fields.Selection(
        "Direction",
        selection=[('pull', "push"), ('push', "push")],
        default='pull',
    )
    sync = fields.Boolean()
    active = fields.Boolean(default=True)
    name = fields.Char(compute='_compute_name')
    is_terminated = fields.Boolean(compute='_compute_terminated')

    @api.depends(
        'producer_model', 'producer_id',
        'consumer_model', 'consumer_id',
    )
    def _compute_signatures(self):
        for r in self:
            r.consumer_signature = f"{r.consumer_model}:{r.consumer_id}"
            r.producer_signature = f"{r.producer_model}:{r.producer_id}"

    @api.depends('producer_signature', 'consumer_signature')
    def _compute_name(self):
        for r in self:
            r.name = '|'.join([r.producer_signature, r.consumer_signature])

    def _compute_terminated(self):
        for r in self:
            r.is_terminated = bool(
                self._get_subscriptions(r.consumer_signature))

    @api.model
    def _get_worker(model, signature):
        worker_model, worker_id = signature.split(':')
        return model.env[worker_model].browse(worker_id)

    @api.model
    def _get_subscriptions(model, signature):
        return model.search([('producer_signature', '=', signature)])

    @api.model
    def process_and_forward(model, signature, data=None, metadata={}):
        data, forward_type = model._process(signature, data, metadata)
        if data is None:  # maybe it's a waiting funnel
            return
        subscriptions = model._get_subscriptions(signature)
        if forward_type == 'spliter' and hasattr(data, '__iter__'):
            for d in data:
                subscriptions._forward(d, metadata.copy())
        else:
            subscriptions._forward(data, metadata.copy())

    @api.model
    def _process(model, signature, data, metadata):
        worker = model._get_worker(signature)
        if not worker.exists():
            raise MissingError(f"No worker found for signature '{signature}'")
        return worker.process(data, metadata), worker.forward_type

    def _forward(self, data, metadata):
        for subscription in self:
            if subscription.sync:
                subscription._sync_forward(data, metadata)
            else:
                subscription.submit(data, metadata)

    def _sync_forward(self, data, metadata):
        self.ensure_one()
        if self.is_terminated:
            self._process(self.consumer_signature, data, metadata)
        self.process_and_forward(self.consumer_signature, data, metadata)

    def submit(self, data, metadata):
        self.ensure_one()
        raise UserError("No queue module installed!")
