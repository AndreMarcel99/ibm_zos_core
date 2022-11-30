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
  exclusion_patterns:
    description:
      - Glob style patterns to exclude files or directories from the resulting archive.
      - This differs from I(exclude_path) which applies only to the source paths from I(path).
    type: list
    elements: path
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
from ansible.module_utils.common.text.converters import to_bytes, to_native
import glob
import re
import os
import abc
import zipfile
import tarfile
# import io
from ansible_collections.ibm.ibm_zos_core.plugins.module_utils.data_set import DataSet
from fnmatch import fnmatch

try:
    from zoautil_py import datasets
except Exception:
    Datasets = MissingZOAUImport()

STATE_ABSENT = 'absent'
STATE_ARCHIVED = 'archive'
STATE_COMPRESSED = 'compress'
STATE_INCOMPLETE = 'incomplete'


def _to_bytes(s):
    return to_bytes(s, errors='surrogate_or_strict')

def _to_native(s):
    return to_native(s, errors='surrogate_or_strict')

def _to_native_ascii(s):
    return to_native(s, errors='surrogate_or_strict', encoding='ascii')

def matches_exclusion_patterns(path, exclusion_patterns):
    return any(fnmatch(path, p) for p in exclusion_patterns)


def get_archive(module):
    """
    Return the proper archive handler based on archive format.
    Arguments:
        format: {str}
    Returns:
        Archive: {Archive}


    """

    """
    TODO Come up with rules to decide based on src, dest and format
    which archive handler to use.
    """
    format = module.params.get("format")
    if format in ["tar"]:
        return TarArchive(module)
    elif format in ["terse", "xmit"]:
        return MVSArchive(module)
    return ZipArchive(module)


def common_path(paths):
    empty = b'' if paths else ''

    return os.path.join(
        os.path.dirname(os.path.commonprefix([os.path.join(os.path.dirname(p), empty) for p in paths])), empty
    )


def expand_paths(paths):
    expanded_path = []
    is_globby = False
    for path in paths:
        b_path = _to_bytes(path)
        if b'*' in b_path or b'?' in b_path:
            e_paths = glob.glob(b_path)
            is_globby = True
        else:
            e_paths = [b_path]
        expanded_path.extend(e_paths)
    return expanded_path, is_globby


def strip_prefix(prefix, string):
    return string[len(prefix):] if string.startswith(prefix) else string


class Archive(abc.ABC):
    def __init__(self, module):
        self.module = module
        self.destination = module.params['dest']
        self.exclusion_patterns = module.params['exclusion_patterns'] or []
        self.format = module.params['format']
        self.must_archive = module.params['force_archive']
        self.remove = module.params['remove']

        self.changed = False
        self.destination_state = STATE_ABSENT
        self.errors = []
        self.file = None
        self.successes = []
        self.targets = []
        self.not_found = []

        paths = module.params['path']

        self.expanded_paths, has_globs = expand_paths(paths)
        self.expanded_exclude_paths = expand_paths(module.params['exclude_path'])[0]

        self.paths = sorted(set(self.expanded_paths) - set(self.expanded_exclude_paths))

        if not self.paths:
            module.fail_json(
                path=', '.join(paths),
                # expanded_paths=_to_native(b', '.join(self.expanded_paths)),
                # expanded_exclude_paths=_to_native(b', '.join(self.expanded_exclude_paths)),
                msg='Error, no source paths were found'
            )

        self.root = common_path(self.paths)

        self.original_checksums = self.destination_checksums()
        self.original_size = self.destination_size()

    def add(self, path, archive_name):
        try:
            self._add(_to_native_ascii(path), _to_native(archive_name))
            if self.contains(_to_native(archive_name)):
                self.successes.append(path)
        except Exception as e:
            self.errors.append('%s: %s' % (_to_native_ascii(path), _to_native(e)))

    def add_targets(self):
        """
        Add targets invokes the add abstract methods, each Archive handler
        will implement it differently.
        """
        self.open()

        try:
            for target in self.targets:
                if os.path.isdir(target):
                    for directory_path, directory_names, file_names in os.walk(target, topdown=True):
                        for directory_name in directory_names:
                            full_path = os.path.join(directory_path, directory_name)
                            self.add(full_path, strip_prefix(self.root, full_path))

                        for file_name in file_names:
                            full_path = os.path.join(directory_path, file_name)
                            self.add(full_path, strip_prefix(self.root, full_path))
                else:
                    self.add(target, strip_prefix(self.root, target))
        except Exception as e:
            if self.format in ('zip', 'tar'):
                archive_format = self.format
            else:
                archive_format = 'tar.' + self.format
            self.module.fail_json(
                msg='Error when writing %s archive at %s: %s' % (
                    archive_format, self.destination, e
                ),
                exception=e
            )
        self.close()

        if self.errors:
            self.module.fail_json(
                msg='Errors when writing archive at %s: %s' % (self.destination, '; '.join(self.errors))
            )

    def find_targets(self):
        """
        Find USS targets, this is the default behaviour, MVS handlers will override it.
        """
        for path in self.paths:
            if not os.path.lexists(path):
                self.not_found.append(path)
            else:
                self.targets.append(path)

    def is_different_from_original(self):
        if self.original_checksums is None:
            return self.original_size != self.destination_size()
        else:
            return self.original_checksums != self.destination_checksums()

    def destination_checksums(self):
        if self.destination_exists() and self.destination_readable():
            return self._get_checksums(self.destination)
        return None

    def destination_exists(self):
        return self.destination and os.path.exists(self.destination)

    def destination_readable(self):
        return self.destination and os.access(self.destination, os.R_OK)

    def destination_size(self):
        return os.path.getsize(self.destination) if self.destination_exists() else 0

    def remove_targets(self):
        for path in self.successes:
            if os.path.exists(path):
                try:
                    if os.path.isdir(path):
                        # remove tree
                        None
                    else:
                        os.remove(path)
                except OSError:
                    self.errors.append(path)
        for path in self.paths:
            try:
                if os.path.isdir(path):
                    # remove tree
                    None
            except OSError:
                self.errors.append(path)

        if self.errors:
            self.module.fail_json(
                dest=self.destination, msg='Error deleting some source files: ', files=self.errors
            )

    def is_archive(path):
        return re.search(br'\.(tar|tar\.(gz|bz2|xz)|tgz|tbz2|zip)$', os.path.basename(path), re.IGNORECASE)

    def has_targets(self):
        return bool(self.targets)

    def has_unfound_targets(self):
        return bool(self.not_found)

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def contains(self, name):
        pass

    @abc.abstractmethod
    def open(self):
        pass

    @abc.abstractmethod
    def _add(self, path, archive_name):
        pass

    @abc.abstractmethod
    def _get_checksums(self, path):
        pass

    @abc.abstractmethod
    def _list_targets(self):
        pass

    @property
    def result(self):
        return {
            'archived': [p for p in self.successes],
            'dest': self.destination,
            'dest_state': self.destination_state,
            'changed': self.changed,
            'arcroot': self.root,
            'missing': [p for p in self.not_found],
            'expanded_paths': [p for p in self.expanded_paths],
            'expanded_exclude_paths': [p for p in self.expanded_exclude_paths],
            # tmp debug variables
            'tmp_debug' : None,
            'targets' : self.targets,
        }


class TarArchive(Archive):
    def __init__(self, module):
        super(TarArchive, self).__init__(module)
        self.fileIO = None

    def open(self):
        if self.format in ('gz', 'bz2'):
            self.file = tarfile.open(_to_native_ascii(self.destination), 'w|' + self.format)
        # python3 tarfile module allows xz format but for python2 we have to create the tarfile
        # in memory and then compress it with lzma.
        # elif self.format == 'xz':
        #     self.fileIO = io.BytesIO()
        #     self.file = tarfile.open(fileobj=self.fileIO, mode='w')
        elif self.format == 'tar':
            self.file = tarfile.open(_to_native_ascii(self.destination), 'w')
        else:
            self.module.fail_json(msg="%s is not a valid archive format" % self.format)

    def close(self):
        self.file.close()
        # if self.format == 'xz':
        #     with lzma.open(_to_native(self.destination), 'wb') as f:
        #         f.write(self.fileIO.getvalue())
        #     self.fileIO.close()

    def _add(self, path, archive_name):
        if not matches_exclusion_patterns(path, self.exclusion_patterns):
            self.file.write(_to_bytes(path), archive_name)
    
    def contains(self, name):
        try:
            self.file.getmember(name)
        except KeyError:
            return False
        return True

    def _add(self, path, archive_name):
        def py26_filter(path):
            return matches_exclusion_patterns(path, self.exclusion_patterns)

        self.file.add(path, archive_name, recursive=False, exclude=py26_filter)

    def _get_checksums(self, path):
        pass

    def _list_targets(self):
         pass

class ZipArchive(Archive):
    def __init__(self, module):
        super(ZipArchive, self).__init__(module)

    def open(self):
        self.file = zipfile.ZipFile(self.destination, 'w', zipfile.ZIP_DEFLATED, True)

    def close(self):
        self.file.close()

    def _add(self, path, archive_name):
        if not matches_exclusion_patterns(path, self.exclusion_patterns):
            self.file.write(_to_bytes(path), archive_name)

    def contains(self, name):
        try:
            self.file.getinfo(name)
        except KeyError:
            return False
        return True

    def _get_checksums(self, path):
        try:
            archive = zipfile.ZipFile(path, 'r')
            checksums = set((info.filename, info.CRC) for info in archive.infolist())
            archive.close()
        except zipfile.BadZipFile:
            checksums = set()
        return checksums

    def _list_targets(self):
         pass

class MVSArchive():
    def __init__(self, module):
        super(MVSArchive, self).__init__(module)

    def open(self):
        pass

    def close(self):
        pass

    def find_targets(self):
        # do a dls to get the file ?
        for path in self.paths:
            if DataSet.data_set_exists(path):
                self.targets.append(path)
            else:
                self.not_found.append(path)

    def _add(self, path, archive_name):
        if not matches_exclusion_patterns(path, self.exclusion_patterns):
            return_content = datasets.zip(path, archive_name)
            stdout = return_content.stdout_response
            stderr = return_content.stderr_response
            rc = return_content.rc
            if rc != 0:
                self.module.fail_json(msg="Error creating MVS archive", stderr=str(stderr))

    def _list_targets(self):
         pass

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
            exclusion_patterns=dict(type='list', elements='path'),
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
        exclusion_patterns=dict(type='list', elements='path'),
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
        # TODO Is is ok to override module.params with parsed_args ?
        module.params = parsed_args
    except ValueError as err:
        module.fail_json(msg="Parameter verification failed", stderr=str(err))

    # Get the proper archive handler based on src and dest type.
    archive = get_archive(module)
    # Find the targets
    archive.find_targets()
    if archive.has_targets():
        #if archive.must_archive:
        archive.add_targets()
        archive.destination_state = STATE_INCOMPLETE if archive.has_unfound_targets() else STATE_ARCHIVED
        archive.changed = archive.is_different_from_original()
        if archive.remove:
            archive.remove_targets()
    else:
        if archive.destination_exists():
            # If destination exists then we verify is an archive.
            archive.destination_state = STATE_ARCHIVED if archive.is_archive(archive.destination) else STATE_COMPRESSED

    if archive.destination_exists():
        None
        # archive.update_permissions()
    # if the user is working with this module in only check mode we do not
    # want to make any changes to the environment, just return the current
    # state with no modifications
    if module.check_mode:
        module.exit_json(**result)
    module.exit_json(**archive.result)


def main():
    run_module()


if __name__ == '__main__':
    main()
