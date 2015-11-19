#!/bin/bash
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# This script runs the impala shell from a dev environment.
. ${IMPALA_HOME}/bin/set-pythonpath.sh
SHELL_HOME=${IMPALA_SHELL_HOME:-${IMPALA_HOME}/shell}
exec impala-python ${SHELL_HOME}/impala_shell.py "$@"
