"""
Copyright 2014 INTEL RESEARCH AND INNOVATION IRELAND LIMITED

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import json
import logging
import re
import socket
import uuid

from heat.common.i18n import _
from heat.engine import constraints, properties, resource

try:
    from oslo.config import cfg
except:
    from oslo_config import cfg

import pika


VERSION = "1.3"

LOGGER = logging.getLogger(__name__)


r_conf = cfg.CONF
rabbbit_userid = None
rabbit_password = None
rabbit_hosts = None

try:
    rpc_backend = r_conf.rpc_backend
    if hasattr(r_conf, 'oslo_messaging_rabbit'):
        rabbbit_userid = r_conf.oslo_messaging_rabbit.rabbit_userid if hasattr(
            r_conf.oslo_messaging_rabbit, 'rabbit_userid'
        ) else "guest"
        rabbit_password = (
            r_conf.oslo_messaging_rabbit.rabbit_password if hasattr(
                r_conf.oslo_messaging_rabbit, 'rabbit_password'
            ) else "guest"
        )
        rabbit_hosts = r_conf.oslo_messaging_rabbit.rabbit_hosts if hasattr(
            r_conf.oslo_messaging_rabbit, 'rabbit_hosts'
        ) else [r_conf.oslo_messaging_rabbit.rabbit_host]
    else:
        rabbbit_userid = r_conf.rabbit_userid if hasattr(
            r_conf, 'rabbit_userid'
        ) else "guest"
        rabbit_password = r_conf.rabbit_password if hasattr(
            r_conf, 'rabbit_password'
        ) else "guest"
        rabbit_hosts = r_conf.rabbit_hosts if hasattr(
            r_conf, 'rabbit_hosts'
        ) else [r_conf.rabbit_host]

except Exception, err:
    LOGGER.info('Exception: {0}'.format(err))


class AEMessenger:

    @staticmethod
    def send(msg_type, msg_data, resource_id):
        global rabbbit_userid
        global rabbit_password
        global rabbit_hosts
        global received_body

        LOGGER.debug('adaptation send')
        reg_host = re.compile('^.*(?=:\d{1,4})')  # match only pre-port
        host_match = re.search(reg_host, rabbit_hosts[0])
        _host = None
        if host_match:
            _host = host_match.group()
        if not _host:  # if no match then just use the whole thing
            _host = rabbit_hosts[0]

        _port = 5672
        _username = rabbbit_userid
        _password = rabbit_password
        _credentials = pika.PlainCredentials(_username, _password)
        _exchange = 'AdaptationEngine'
        _queue = 'adaptationengine.{0}'.format(uuid.uuid4().hex)
        _routing_key_out = 'adaptationengine'

        _msg_id = uuid.uuid4().hex
        _routing_key_in = 'aemessenger.{0}'.format(resource_id)

        _message = {
            'heat': {
                'id': _msg_id,
                'type': msg_type,
                'data': msg_data
            }
        }

        try:
            payload = json.dumps(_message)
        except Exception:
            error_msg = "tried to send [{}]".format(_message)
            raise Exception(error_msg)

        LOGGER.debug('adaptation send_create payload {0}'.format(payload))
        LOGGER.debug('adaptation send connecting')

        # connect
        LOGGER.debug("host {}, port {}".format(_host, _port))
        try:
            real_host = socket.gethostbyname(_host)
            LOGGER.debug("DNS was working so the host is {}".format(real_host))
        except Exception:
            real_host = '192.168.17.101'
            LOGGER.debug(
                "DNS wasn't working so hardcoded the host to {}".format(
                    real_host
                )
            )

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=real_host,
                port=_port,
                credentials=_credentials,
                connection_attempts=5,
                retry_delay=1,
                socket_timeout=5
            )
        )
        channel = connection.channel()
        channel.exchange_declare(exchange=_exchange, type='topic')
        channel.queue_declare(queue=_queue, auto_delete=True)
        channel.queue_bind(
            exchange=_exchange, queue=_queue, routing_key=_routing_key_in
        )
        LOGGER.debug('adaptation send connected')

        # publish
        LOGGER.debug('adaptation send publishing')
        channel.basic_publish(
            exchange=_exchange,
            routing_key=_routing_key_out,
            body=payload
        )
        LOGGER.debug('adaptation send published to {}'.format(_routing_key_out))

        # listen
        received_body = None

        def callback(ch, method, properties, body):
            global received_body
            LOGGER.debug('adaptation send got message {0}'.format(body))
            received_body = body
            disconnect()

        def timeout():
            LOGGER.error('adaptation send timed out')
            bad_disconnect()

        def disconnect():
            LOGGER.debug('adaptation send closing')
            channel.stop_consuming()
            channel.close()
            connection.close()
            LOGGER.debug('adaptation send closed')

        def bad_disconnect():
            LOGGER.debug('adaptation bad disconnect')
            disconnect()
            return None

        LOGGER.debug('adaptation send listening on {}'.format(_routing_key_in))
        connection.add_timeout(30, timeout)
        channel.basic_consume(callback, queue=_queue, no_ack=True)
        channel.start_consuming()

        LOGGER.debug('received_body: {0}'.format(received_body))

        try:
            json_body = json.loads(received_body)
        except Exception:
            error_msg = "did not get good json back [{}]".format(received_body)
            raise Exception(error_msg)

        return json_body

    @staticmethod
    def send_create(
            name,
            actions,
            stack_id,
            agreement_id,
            default_weighting,
            weightings,
            grouping,
            blacklist,
            horizontal_scale_out,
            embargo
    ):
        LOGGER.debug('adaptation send_create')
        resource_id = uuid.uuid4().hex
        msg_data = {
            'name': name,
            'stack_id': stack_id,
            'agreement_id': agreement_id,
            'actions': actions,
            'resource_id': resource_id,
            'default_weighting': default_weighting,
            'weightings': weightings,
            'grouping': grouping,
            'blacklist': blacklist,
            'embargo': embargo,
            'horizontal_scale_out': horizontal_scale_out,
        }
        LOGGER.debug('adaptation msg_data {0}'.format(msg_data))
        return AEMessenger.send('heat_create', msg_data, resource_id)

    @staticmethod
    def send_check_create_complete(resource_id):
        LOGGER.debug('adaptation send_check_create_complete')
        msg_data = {
            'resource_id': resource_id
        }
        LOGGER.debug('adaptation msg_data {0}'.format(msg_data))
        return AEMessenger.send(
            'heat_check_create_complete', msg_data, resource_id
        )

    @staticmethod
    def send_delete(resource_id):
        LOGGER.debug('adaptation send_delete')
        msg_data = {
            'resource_id': resource_id,
        }
        LOGGER.debug('adaptation msg_data {0}'.format(msg_data))
        return AEMessenger.send('heat_delete', msg_data, resource_id)


class AdaptationResponse(resource.Resource):

    plugin_schema = {
        'default_weighting': properties.Schema(
            properties.Schema.NUMBER,
            _('Default weighting value for plugins'),
            required=False,
            default=1,
        ),
        'weightings': properties.Schema(
            properties.Schema.LIST,
            _('Assigned weighting values'),
            required=False,
            default=None,
            schema=properties.Schema(
                properties.Schema.MAP,
                _('Plugin grouping'),
                required=True,
                default=None,
                schema={
                    'name': properties.Schema(
                        properties.Schema.STRING,
                        _('Plugin name'),
                        required=True,
                        default=None,
                    ),

                    'weight': properties.Schema(
                        properties.Schema.NUMBER,
                        _('Plugin weight'),
                        required=True,
                        default=None,
                    ),
                },
            )
        ),
        'grouping': properties.Schema(
            properties.Schema.LIST,
            _('Grouping plugins and execution order'),
            required=False,
            default=None,
            schema=properties.Schema(
                properties.Schema.LIST,
                _('Plugin grouping'),
                required=True,
                default=None,
                schema=properties.Schema(
                    properties.Schema.STRING,
                    _('Plugin name'),
                    required=True,
                    default=None,
                ),
            ),
        ),
        'blacklist': properties.Schema(
            properties.Schema.LIST,
            _('Blacklist plugins from default plugin grouping'),
            required=False,
            default=None
        ),
    }

    properties_schema = {
        'name': properties.Schema(
            properties.Schema.STRING,
            _('Name of enactment point'),
            required=True,
            default=None,
        ),
        'agreement_id': properties.Schema(
            properties.Schema.STRING,
            _('ID of SLA agreement'),
            required=False,
            default=None,
        ),
        'allowed_actions': properties.Schema(
            properties.Schema.LIST,
            _('allowed_actions'),
            required=True,
            default=None,
            constraints=[constraints.AllowedValues(
                [
                    'NoAction',
                    'MigrateAction',
                    'VerticalScaleAction',
                    'HorizontalScaleAction',
                    'DeveloperAction',
                    'StartAction',
                    'StopAction',
                    'LowPowerAction'
                ]
            )],
        ),
        'plugins': properties.Schema(
            properties.Schema.MAP,
            _('Plugin config for this stack'),
            required=False,
            default=None,
            schema=plugin_schema,
        ),
        'extend_embargo': properties.Schema(
            properties.Schema.NUMBER,
            (
                'number of seconds to extend the adaptation '
                'embargo by on top of how long the adaptation '
                'actually takes'
            ),
            required=False,
            default=None,
        ),
        'horizontal_scale_out': properties.Schema(
            properties.Schema.MAP,
            _('Describe resource to be created on scale out'),
            required=False,
            default=None,
            schema={
                'name_prefix': properties.Schema(
                    properties.Schema.STRING,
                    (
                        'prefix of name of new instance '
                        '(will have random id appended)'
                    ),
                    required=True,
                    default=None,
                ),
                'key_name': properties.Schema(
                    properties.Schema.STRING,
                    _('name of key'),
                    required=True,
                    default=None,
                ),
                'image': properties.Schema(
                    properties.Schema.STRING,
                    _('name of image'),
                    required=True,
                    default=None,
                ),
                'flavor': properties.Schema(
                    properties.Schema.STRING,
                    _('name of flavor'),
                    required=True,
                    default=None,
                ),
                'network_id': properties.Schema(
                    properties.Schema.STRING,
                    _('name or id of network'),
                    required=True,
                    default=None,
                ),
                'extend_embargo': properties.Schema(
                    properties.Schema.NUMBER,
                    (
                        'number of seconds to extend the adaptation '
                        'embargo by on top of how long the adaptation '
                        'actually takes'
                    ),
                    required=False,
                    default=None,
                ),
            },
        ),
    }

    def handle_create(self):
        try:
            LOGGER.debug('adaptation stack_id: {0}'.format(self.stack.id))
            LOGGER.debug('name {0}'.format(self.properties['name']))
            LOGGER.debug(
                'allowed actions {0}'.format(
                    self.properties['allowed_actions']
                )
            )
            LOGGER.debug(
                'plugin config {}'.format(
                    self.properties.get('plugins', 'Not found')
                )
            )
            LOGGER.debug(
                'scale config {}'.format(
                    self.properties.get('horizontal_scale_out', 'Not found')
                )
            )
            LOGGER.debug(
                'agreement id {}'.format(
                    self.properties.get('agreement_id', 'Not found')
                )
            )

            plugin_config = self.properties.get('plugins', {})
            scale_out = self.properties.get('horizontal_scale_out', {})
            if scale_out:
                embargo = scale_out.get('extend_embargo')
            else:
                embargo = self.properties.get('extend_embargo', 0)

            if plugin_config:
                plugin_default_weighting = plugin_config.get(
                    'default_weighting',
                    1,
                )
                plugin_weightings = plugin_config.get('weightings', [])
                plugin_grouping = plugin_config.get('grouping', [])
                plugin_blacklist = plugin_config.get('blacklist', [])
            else:
                plugin_default_weighting = 1
                plugin_weightings = []
                plugin_grouping = []
                plugin_blacklist = []

            response = AEMessenger.send_create(
                name=self.properties['name'],
                actions=self.properties['allowed_actions'],
                stack_id=self.stack.id,
                agreement_id=self.properties.get('agreement_id'),
                default_weighting=plugin_default_weighting,
                weightings=plugin_weightings,
                grouping=plugin_grouping,
                blacklist=plugin_blacklist,
                horizontal_scale_out=self.properties.get(
                    'horizontal_scale_out',
                    None,
                ),
                embargo=embargo
            )
            LOGGER.debug('adaptation response [{0}]'.format(response))
            LOGGER.debug('adaptation type [{0}]'.format(type(response)))
            token = response['response']
            LOGGER.debug('adaptation token [{0}]'.format(token))
        except KeyError, err:
            LOGGER.error(
                'malformed response from adaptationengine: {0}'.format(err)
            )
            raise Exception(err)
        except Exception, err:
            LOGGER.exception(err)
            LOGGER.error('adaptation error [{0}]'.format(err))
            raise Exception(err)
        else:
            self.resource_id_set(token)
            return token

    def check_create_complete(self, order_href):
        LOGGER.debug('check_create_complete start')
        try:
            LOGGER.debug(
                'check_create_complete sending message [%s]', str(order_href)
            )
            response = AEMessenger.send_check_create_complete(
                resource_id=self.resource_id
            )
            LOGGER.debug('check_create_complete response [%s]', str(response))
            return response['response']
        except Exception, err:
            raise Exception("check_create_complete failed [{}]".format(err))
        LOGGER.debug('check_create_complete end')

    def handle_delete(self):
        try:
            response = AEMessenger.send_delete(
                resource_id=self.resource_id,
            )
            return response['response']
        except Exception, err:
            LOGGER.debug(
                'handle_delete failed, returning true anyway [%s]', str(err)
            )
            return True


def resource_mapping():
    return {
        'AdaptationEngine::Heat::AdaptationResponse': AdaptationResponse
    }
