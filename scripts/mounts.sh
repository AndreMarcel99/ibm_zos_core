# ==============================================================================
# Copyright (c) IBM Corporation 2023
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
# ==============================================================================
# KSH (Korn Shell) Array of mounts index delimited by " ", etries delimited by ":"
# More on ksh arrays: https://docstore.mik.ua/orelly/unix3/korn/ch06_04.htm
# This `mounts.sh` is sourced by serveral other files, only these lists needs to
# be maintained.
# ==============================================================================

# ------------------------------------------------------------------------------
# zoau_mount_list[0]="<index>:<version>:<mount>:<data_set><space>"
#   e.g: zoau_mount_list[0]="1:v1.2.0:/zoau/v1.2.0:IMSTESTU.ZOAU.V120.ZFS "
# Format:
#   index   - used by the generated profile so a user can select an option
#   version    - describes the option a user can select
#   mount - the mount point path the data set will be mounted to
#   data_set - the z/OS data set containing the binaries to mount
#   space - must be a space before the closing quote
# ------------------------------------------------------------------------------
zoau_mount_list_str="1:1.2.0:/zoau/v1.2.0:IMSTESTU.ZOAU.V120.ZFS "\
"2:1.0.0-ga:/zoau/v1.0.0-ga:IMSTESTU.ZOAU.V100.GA.ZFS "\
"3:1.0.1-ga:/zoau/v1.0.1-ga:IMSTESTU.ZOAU.V101.GA.ZFS "\
"6:1.0.2-ga:/zoau/v1.0.2-ga:IMSTESTU.ZOAU.V102.GA.ZFS "\
"7:1.0.3-ga5:/zoau/v1.0.3-ga5:IMSTESTU.ZOAU.V103.GA5.ZFS "\
"8:1.0.3-ptf2:/zoau/v1.0.3-ptf2:IMSTESTU.ZOAU.V103.PTF2.ZFS "\
"12:1.1.0-ga:/zoau/v1.1.0-ga:IMSTESTU.ZOAU.V110.GA.ZFS "\
"13:1.1.1-ptf1:/zoau/v1.1.1-ptf1:IMSTESTU.ZOAU.V111.PTF1.ZFS "\
"15:1.2.1:/zoau/v1.2.1:IMSTESTU.ZOAU.V121.ZFS "\
"19:1.2.2:/zoau/v1.2.2:IMSTESTU.ZOAU.V122.ZFS "\
"20:latest:/zoau/latest:IMSTESTU.ZOAU.LATEST.ZFS "

# ------------------------------------------------------------------------------
# python_mount_list[0]="<mount>:<data_set><space>"
# python_mount_list[0]="/python2:IMSTESTU.PYZ.ROCKET.V362B.ZFS "
# Format:
#   mount - the mount point path the data set will be mounted to
#   data_set - the z/OS data set containing the binaries to mount
#   space - must be a space before the closing quote
# Mismarked: "/allpython/3.8.5:IMSTESTU.PYZ.V380.GA.ZFS "\
# ------------------------------------------------------------------------------
python_mount_list_str="1:3.8.2:/allpython/3.8.2/usr/lpp/IBM/cyp/v3r8:IMSTESTU.PYZ.ROCKET.V362B.ZFS "\
"2:3.8.3:/allpython/3.8.3/usr/lpp/IBM/cyp/v3r8:IMSTESTU.PYZ.V383PLUS.ZFS "\
"3:3.9:/allpython/3.9/usr/lpp/IBM/cyp/v3r9:IMSTESTU.PYZ.V380.GA.ZFS "\
"4:3.10:/allpython/3.10/usr/lpp/IBM/cyp/v3r10:IMSTESTU.PYZ.V3A0.ZFS "\
"5:3.11:/allpython/3.11/usr/lpp/IBM/cyp/v3r11:IMSTESTU.PYZ.V3B0.ZFS "\
"6:3.11-ga:/allpython/3.11-ga/usr/lpp/IBM/cyp/v3r11:IMSTESTU.PYZ.V311GA.ZFS "

# ------------------------------------------------------------------------------
# python_path_list[0]="<index>:<version>:<path><space>"
# python_path_list[0]="1:3.8:/python3/usr/lpp/IBM/cyp/v3r8/pyz "
# Format:
#   index   - used by the generated profile so a user can select an option
#   version    - describes the option a user can select
#   path - the path where a particular python can be found
#   space - must be a space before the closing quote
# ------------------------------------------------------------------------------
python_path_list_str="1:3.8.2:/allpython/3.8.2/usr/lpp/IBM/cyp/v3r8/pyz "\
"2:3.8.3:/allpython/3.8.3/usr/lpp/IBM/cyp/v3r8/pyz "\
"3:3.9:/allpython/3.9/usr/lpp/IBM/cyp/v3r9/pyz "\
"4:3.10:/allpython/3.10/usr/lpp/IBM/cyp/v3r10/pyz "\
"5:3.11:/allpython/3.11/usr/lpp/IBM/cyp/v3r11/pyz "\
"6:3.11:/allpython/3.11-ga/usr/lpp/IBM/cyp/v3r11/pyz "