# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from apiclient.discovery import build
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

import time


class GoogleCloudEngineHook(GoogleCloudBaseHook):
    """
    Interact with Google Cloud Engine. This hook uses the Google Cloud Platform
    connection.
    """

    def __init__(self,
                 conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudEngineHook, self).__init__(conn_id, delegate_to)

    def get_conn(self):
        """
        Returns a Google Cloud Storage service object.
        """
        http_authorized = self._authorize()
        return build('compute', 'v1', http=http_authorized)

    def _wait_for_operation(self, project, zone, operation):
        compute = self.get_conn()
        while True:
            result = compute.zoneOperations().get(
                project=project,
                zone=zone,
                operation=operation).execute()

            if result['status'] == 'DONE':
                print("done.")
                if 'error' in result:
                    raise Exception(result['error'])
                return result

            time.sleep(1)

    def delete_instance(self, project, zone, name, execute_sync=True):
        compute = self.get_conn()
        operation = compute.instances().delete(
            project=project,
            zone=zone,
            instance=name).execute()
        if execute_sync:
            self._wait_for_operation(project, zone, operation['name'])
        return operation

    def create_instance(self, project, zone, instance_name,
                    image_project='debian-cloud', 
                    image_family='debian-8', 
                    instance_type='n1-standard-1',
                    execute_sync=True):
        compute = self.get_conn()
        # Get the latest Debian Jessie image.
        image_response = compute.images().getFromFamily(
            project=image_project, family=image_family).execute()
        source_disk_image = image_response['selfLink']

        # Configure the machine
        machine_type = "zones/%s/machineTypes/%s" % (zone, instance_type)

        config = {
            'name': instance_name,
            'machineType': machine_type,

            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': source_disk_image,
                    }
                }
            ],

            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }]
        }

        operation = compute.instances().insert(
            project=project,
            zone=zone,
            body=config).execute()
        if execute_sync:
            self._wait_for_operation(project, zone, operation['name'])
        return operation
