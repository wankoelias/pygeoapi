# =================================================================
#
# Authors: Bernhard Mallinger <bernhard.mallinger@eox.at>
#
# Copyright (c) 2020 Bernhard Mallinger
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging

from pygeoapi.process.base import BaseProcessor

LOGGER = logging.getLogger(__name__)

#: Process metadata and description
PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'notebook',
    'title': 'Process notebooks on kubernetes with papermill',
    'description': '',
    'keywords': ['notebook'],
    'links': [{
        'type': 'text/html',
        'rel': 'canonical',
        'title': 'eurodatacube',
        'href': 'https://eurodatacube.com',
        'hreflang': 'en-US'
    }],
    'inputs': [{
        'id': 'notebook',
        'title': 'notebook file',
        'abstract': 'notebook file',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 1,
        'maxOccurs': 1,
        'metadata': None,  # TODO how to use?
        'keywords': ['notebook']
    }, {
        'id': 'parameters',
        'title': 'parameters',
        'abstract': 'parameters for notebook execution.',
        'input': {
            'literalDataDomain': {
                'dataType': 'string',
                'valueDefinition': {
                    'anyValue': True
                }
            }
        },
        'minOccurs': 0,
        'maxOccurs': 1,
        'metadata': None,
        'keywords': ['message']
    }],
    'outputs': [{
        'id': 'link_to_result_notebook',
        'title': 'Link to result notebook',
        'description': 'Link to result notebook',
        'output': {
            'formats': [{
                'mimeType': 'text/plain'
            }]
        }
    }],
    'example': {},
}


class PapermillNotebookKubernetesProcessor(BaseProcessor):

    def __init__(self, processor_def):
        super().__init__(processor_def, PROCESS_METADATA)

    def execute(self, data):
        value = 'papermill {}!'.format("yeah")
        outputs = [{
            'id': 'echo',
            'value': value
        }]

        return outputs

    def __repr__(self):
        return '<PapermillNotebookKubernetesProcessor> {}'.format(self.name)
