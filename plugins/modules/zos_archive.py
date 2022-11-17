#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (c) IBM Corporation 2020, 2022
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: zos_archive
version_added: "1.6.0"
author:
  - Oscar Fernando Flores Garcia (@fernandofloresg)
short_description: Archive a dataset on z/OS.



description:
  - Creates or extends an archive.
  - The source and archive are on the remote host,
    and the archive is not copied to the local host.

options:
  path:
    description:
      - Remote absolute path, glob, or list of paths or globs for the file or files to compress or archive.
    type: list
    required: true
    elements: path
    alias: src
  format:
    description:
      - The type of compression to use.
    type: str
    choices: [ bz2, gz, tar, zip ]
    default: gz
    required: false
  dest:
    description: The file name of the destination archive.
    type: str
    required: false
  exclude_path:
    description:
        - Remote absolute path, glob, or list of paths or globs for the file or files to exclude
           from path list and glob expansion.
    type: list
    required: false
    elements: path
  force_archive:
    description:
      - Allows you to force the module to treat this as an archive even if only a single file is specified.
      - By default when a single file is specified it is compressed only (not archived).
    type: bool
    required: false
    default: false
  group:
    description:
      - Name of the group that should own the filesystem object, as would be fed to chown.
      - When left unspecified, it uses the current group of the current user unless you are root,
        in which case it can preserve the previous ownership.
    type: str
    required: false
  mode:
    description:
      - The permissions the resulting filesystem object should have.
    type: str
    required: false
  owner:
    description:
      - Name of the user that should own the filesystem object, as would be fed to chown.
      - When left unspecified, it uses the current user unless you are root, in which case it can preserve the previous ownership.
    type: str
    required: false
  remove:
    description:
      - Remove any added source files and trees after adding to archive.
    type: bool
    required: false
    default: false
  replace_dest:
    description:
      - Replace the existing archive C(dest).
    type: bool
    default: false
    required: false
    alias: force
  list:
    description:
      - List the names of the archive contents
    type: bool
    default: false
    required: false
  tmp_hlq:
    description:
      - High Level Qualifier used for temporary datasets.
    type: str
    required: false
'''

EXAMPLES = r'''
# Pass in a message
- name: Test with a message
  my_namespace.my_collection.my_test:
    name: hello world

# pass in a message and have changed true
- name: Test with a message and changed output
  my_namespace.my_collection.my_test:
    name: hello world
    new: true

# fail the module
- name: Test failure of the module
  my_namespace.my_collection.my_test:
    name: fail me
'''

RETURN = r'''
state:
    description:
        The state of the input C(path).
    type: str
    returned: always
dest_state:
    description:
      - The state of the I(dest) file.
      - C(absent) when the file does not exist.
      - C(archive) when the file is an archive.
      - C(compress) when the file is compressed, but not an archive.
      - C(incomplete) when the file is an archive, but some files under I(path) were not found.
    type: str
    returned: success
    version_added: 3.4.0
missing:
    description: Any files that were missing from the source.
    type: list
    returned: success
archived:
    description: Any files that were compressed or added to the archive.
    type: list
    returned: success
arcroot:
    description: The archive root.
    type: str
    returned: always
expanded_paths:
    description: The list of matching paths from paths argument.
    type: list
    returned: always
expanded_exclude_paths:
    description: The list of matching exclude paths from the exclude_path argument.
    type: list
    returned: always
'''

from ansible.module_utils.basic import AnsibleModule
from ansible_collections.ibm.ibm_zos_core.plugins.module_utils import (
    better_arg_parser)


def run_module():
    module = AnsibleModule(
        argument_spec=dict(
            path=dict(type='list', elements='path', required=True, alias='src'),
            dest=dict(type='str'),
            exclude_path=dict(type='list', elements='path', default=[]),
            # Q1 I think we should use force in here instead of down, and change that one to replace.
            force_archive=dict(type='bool', default=False),
            format=dict(type='str', default='gz', choices=['bz2', 'gz', 'tar', 'zip']),
            group=dict(type='str', default=''),
            mode=dict(type='str', default=''),
            owner=dict(type='str', default=''),
            remove=dict(type='bool', default=False),
            # Q1 I think this parameter name should be replace.
            replace_dest=dict(type='bool', default=False, alias='force'),
            list=dict(type='bool', default=False),
            tmp_hlq=dict(type='str', default=''),
        ),
        supports_check_mode=True,
    )

    arg_defs = dict(
        path=dict(type='list', elements='path', required=True, alias='src'),
        dest=dict(type='str', required=False),
        exclude_path=dict(type='list', elements='path', default=[]),
        # Q1 I think we should use force in here instead of down, and change that one to replace.
        force_archive=dict(type='bool', default=False),
        format=dict(type='str', default='gz', choices=['bz2', 'gz', 'tar', 'zip']),
        group=dict(type='str', default=''),
        mode=dict(type='str', default=''),
        owner=dict(type='str', default=''),
        remove=dict(type='bool', default=False),
        # Q1 I think this parameter name should be replace.
        replace_dest=dict(type='bool', default=False, alias='force'),
        list=dict(type='bool', default=False),
        tmp_hlq=dict(type='qualifier_or_empty', default=''),
    )
    # seed the result dict in the object
    # we primarily care about changed and state
    # changed is if this module effectively modified the target
    # state will include any data that you want your module to pass back
    # for consumption, for example, in a subsequent task
    result = dict(
        changed=False,
        original_message='',
        message=''
    )

    try:
        parser = better_arg_parser.BetterArgParser(arg_defs)
        parsed_args = parser.parse_args(module.params)
    except ValueError as err:
        module.fail_json(msg="Parameter verification failed", stderr=str(err))

    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)
    module.exit_json(**result)


def main():
    run_module()


if __name__ == '__main__':
    main()
