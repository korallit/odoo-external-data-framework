from odoo import api, fields, models


class ExternalDataSubscription(models.Model):
    _inherit = 'external.data.subscription'

    def submit(self, data, metadata):
        self.ensure_one()
        # TODO


class ExternalDataQueue(models.TransientModel):
    _inherit = 'external.data.queue'
    _description = "Worker Queue Odoo"
    _transient_max_count = 0
    _transient_max_hours = 24.0
    _order = 'create_date'
    _rec_name = 'id'

    worker_signature = fields.Char("Worker", required=True)
    history = fields.Json(default=[])
    index = fields.Integer(default=0)
    metadata = fields.Json(required=True)
    payload_type = fields.Char(
        "Type",
        selection=[
            ('json', "JSON"),
            ('str', "String"),
            ('bin', "Binary"),
        ],
        default='json',
    )
    payload_json = fields.Json("Payload")
    payload_str = fields.Text()
    payload_bin = fields.Binary(attachment=False)
