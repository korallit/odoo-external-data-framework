# coding: utf-8

from requests import Request, Session
from odoo import fields, models
from odoo.exceptions import UserError
from http.cookiejar import MozillaCookieJar
import tempfile
from os import path

import logging
import warnings
from cryptography.utils import CryptographyDeprecationWarning

SESSIONS = {}

# Ignoring pyOpenSSL warnings
warnings.simplefilter('ignore', category=CryptographyDeprecationWarning)
_logger = logging.getLogger(__name__)


class ExternalDataCredential(models.Model):
    _name = 'external.data.credential'
    _description = "External Data Credential"

    data_source_id = fields.Many2one(
        'external.data.source',
        required=True,
    )
    name = fields.Char(required=True)
    key = fields.Char(required=True)
    value = fields.Char()
    document = fields.Text()
    file = fields.Binary()
    is_secret = fields.Boolean()
    transporter_ids = fields.Many2many(
        'external.data.transporter',
        string="Transporters",
    )


class ExternalDataTransporter(models.Model):
    _name = 'external.data.transporter'
    _description = "External Data Transporter"

    name = fields.Char(required=True)
    protocol = fields.Selection(
        selection=[
            ('local_fs', 'local filesystem'),
            ('http', "HTTP"),
            ('ftp', "FTP"),
            ('s3', "S3"),
            ('orm', "Odoo ORM"),
        ],
        required=True,
    )
    auth_method = fields.Selection(  # TODO: don't need this
        string="Auth method",
        selection=[
            ('sudo', "sudo"),
            ('userpass', "user/password"),
            ('tls', "TLS cert"),
            ('http_login', "HTTP web login"),
            ('http_basic', "HTTP Basic"),
            ('http_bearer', "HTTP Bearer"),
            ('http_proxy_basic', "HTTP Basic (proxy)"),
            ('http_proxy_bearer', "HTTP Bearer (proxy)"),
            ('http_header', "HTTP custom headers"),
            ('http_cookie', "HTTP custom cookies"),
        ],
    )
    credential_ids = fields.Many2many(
        'external.data.credential',
        string="Credentials",
    )
    http_credential_ids_headers = fields.Many2many(
        'external.data.credential',
        relation='external_data_credential_transporter_http_header_rel',
        string="Headers",
    )
    http_credential_ids_cookies = fields.Many2many(
        'external.data.credential',
        relation='external_data_credential_transporter_http_cookie_rel',
        string="Cookies",
    )
    http_cookiejar = fields.Many2one(
        comodel_name='ir.attachment',
        string="Cookiejar",
    )
    http_request_method = fields.Selection(
        string="Method",
        selection=[
            ('GET', "GET"),
            ('POST', "POST"),
        ],
        default='GET',
    )
    content_type = fields.Selection(
        string="Content type",
        selection=[
            ('binary', "binary"),
            ('text', "text"),
        ],
        default='binary',
    )

    def fetch(self, resource_id):
        self.ensure_one()
        resource = self.env['external.data.resource'].browse(resource_id)
        if not resource.exists():
            return False

        method_name = '_fetch_' + self.protocol
        try:
            fetcher = getattr(self, method_name)
            return fetcher(resource)
        except (AttributeError, TypeError) as e:
            _logger.error(e)
            return False

    def deliver(self, resource_id):
        self.ensure_one()
        resource = self.env['external.data.resource'].browse(resource_id)
        if not resource.exists():
            return False

        method_name = '_deliver_' + self.protocol
        try:
            fetcher = getattr(self, method_name)
            return fetcher(resource)
        except (AttributeError, TypeError) as e:
            _logger.error(e)
            return False

    def _fetch_http(self, resource):
        return self._http_request(resource, 'pull')

    def _deliver_http(self, resource):
        return self._http_request(resource, 'push')

    def _http_request(self, resource, direction):
        self.ensure_one()
        ses = self._http_create_session()
        req = Request(self.http_request_method, resource.url)
        req_prepped = ses.prepare_request(req)
        res = ses.send(req_prepped)
        if res.status_code == 200:
            if self.content_type == 'binary':
                return res.content
            elif self.content_type == 'text':
                return res.text
        else:
            _logger.error("HTTP response code is " + res.status_code)
        return False

    def _http_get_cookiejar_path(self):
        self.ensure_one()
        tempdir = tempfile.tempdir or '/tmp'
        return path.join(tempdir, f'cookiejar-{self.id}.txt')

    def _http_create_session(self):
        self.ensure_one()
        ses = SESSIONS.get(self.id)
        if not isinstance(ses, Session):
            ses = Session()
            SESSIONS.update({self.id: ses})
            ses.cookies = MozillaCookieJar(self._http_get_cookiejar_path())
            _logger.info(f"HTTP session created for transporter ID {self.id}")
        return ses

    def http_cookiejar_load(self):
        self.ensure_one()
        if not self.http_cookiejar:
            return False
        ses = self._http_create_session()
        with open(self._http_get_cookiejar_path(), 'w') as cf:
            cf.write(self.http_cookiejar.raw)
        ses.cookies.load(ignore_expires=True)

    def http_cookiejar_save(self):
        self.ensure_one()
        ses = SESSIONS.get(self.id)
        if not isinstance(ses, Session) or not self.http_cookiejar:
            msg = (
                "No session created yet for transporter "
                f"'{self.name}' (ID {self.id})"
            )
            raise UserError(msg)

        ses.cookies.save(ignore_expires=True)
        with open(self._http_get_cookiejar_path(), 'r') as cf:
            self.http_cookiejar.raw = cf.read()

    def _fetch_local_fs(self, resource):
        self.ensure_one()
        # TODO: check whether file or directory
        if self.content_type == 'binary':
            mode = 'rb'
        elif self.content_type == 'text':
            mode = 'r'
        try:
            reader = open(resource.url, mode)
            return reader.read()
        except Exception as e:
            _logger.error(e)
