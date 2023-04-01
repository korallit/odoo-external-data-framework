from odoo import api, fields, models
from odoo.exceptions import MissingError, UserError


class ExternalDataWorker(models.AbstractModel):
    _name = 'external.data.worker'
    _description = "External Data Worker Template"

    name = fields.Char(required=True)
    batch_size = fields.Integer("Batch size", default=0)
    signature = fields.Char(compute='_compute_signature')

    def _compute_signature(self):
        for record in self:
            record.signature = f"{record._name}:{record.id}"

    def process(self, data, metadata={}):
        return data


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
        data = model._process(signature, data, metadata)
        for subscription in model._get_subscriptions(signature):
            metadata_copy = metadata.copy()
            if subscription.sync:
                subscription._sync_forward(data, metadata_copy)
            subscription.submit(data, metadata_copy)

    @api.model
    def _process(model, signature, data, metadata):
        worker = model._get_worker(signature)
        if not worker.exists():
            raise MissingError(f"No worker found for signature '{signature}'")
        return worker.process(data, metadata)

    def _sync_forward(self, data, metadata):
        self.ensure_one()
        if self.is_terminated:
            self._process(self.consumer_signature, data, metadata)
        self.process_and_forward(self.consumer_signature, data, metadata)

    def submit(self, data, metadata):
        self.ensure_one()
        raise UserError("No queue module installed!")
