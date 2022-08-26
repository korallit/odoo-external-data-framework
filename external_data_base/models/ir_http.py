# coding: utf-8

from werkzeug.exceptions import BadRequest

from odoo.models import AbstractModel
from odoo.http import request


class IrHttp(AbstractModel):
    _inherit = "ir.http"

    @classmethod
    def _auth_method_api_key(cls):
        api_key = request.httprequest.headers.get("Authorization")
        if not api_key:
            raise BadRequest("Authorization header with API key missing")

        user_id = request.env["res.users.apikeys"]._check_credentials(
            scope="rpc", key=api_key
        )
        if not user_id:
            raise BadRequest("API key invalid")

        request.uid = user_id
