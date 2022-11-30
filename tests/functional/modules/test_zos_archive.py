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


from __future__ import absolute_import, division, print_function

import pytest
import os
import shutil
import re
import tempfile
from tempfile import mkstemp

__metaclass__ = type

SHELL_EXECUTABLE = "/bin/sh"
USS_TEST_FILES = {  "foo.txt" : "foo sample content",
                    "bar.txt": "bar sample content", 
                    "empty.txt":""}
USS_TEMP_DIR = "/tmp/archive"
TEST_PS = "USER.PRIVATE.TESTDS"
USS_DEST_ARCHIVE = "testarchive.dzp"

STATE_ARCHIVED = "archived"
STATE_COMPRESSED = "compressed"

def set_uss_test_env(ansible_zos_module, test_files):
    temp_dir = USS_TEMP_DIR
    for key, value in test_files.items():
        ansible_zos_module.all.shell(
            cmd=f"echo \"{value}\" > \"{temp_dir}/{key}\"",
            executable=SHELL_EXECUTABLE,
        )

# Core functionality tests
@pytest.mark.parametrize(
    "format", [
    "tar",
    "zip",
    # "gz",
    # "bz2",
    # "pax",
])
def test_uss_archive(ansible_zos_module, format):
    try:
        hosts = ansible_zos_module
        expected_state = STATE_ARCHIVED if format in ['tar', 'zip'] else STATE_COMPRESSED
        hosts.all.file(path=USS_TEMP_DIR, state="directory")
        set_uss_test_env(hosts, USS_TEST_FILES)
        # set test env
        archive_result = hosts.all.zos_archive( path=f"{USS_TEMP_DIR}/*.txt",
                                        dest=f"{USS_TEMP_DIR}/archive.{format}",
                                        format=format)
        for result in archive_result.contacted.values():
            assert result.get("changed") is True
            assert result.get("state") == expected_state
            # TODO assert that the file with expected extension exists.
            # cmd_result = hosts.all.shell(cmd="")
    finally:
        hosts.all.file(path=f"{USS_TEMP_DIR}", state="absent")


pytest.mark.parametrize(
    "format", [
        "terse",
        # "xmit",
        ])
def test_mvs_archive(ansible_zos_module, format):
    try:
        hosts = ansible_zos_module
        hosts.all.file(path=USS_TEMP_DIR, state="directory")
        # create a sammple ds with content
        archive_result = hosts.all.zos_archive( path=TEST_PS,
                                                dest=f"{USS_TEMP_DIR}/{USS_DEST_ARCHIVE}",
                                                format=format)
        for result in archive_result.contacted.values():
            assert result.get("changed") is True
            assert result.get("state")
    finally:
        hosts.all.file(path=f"{USS_TEMP_DIR}/{USS_DEST_ARCHIVE}", state="absent")
        hosts.all.zos_data_set(name=TEST_PS, state="absent")