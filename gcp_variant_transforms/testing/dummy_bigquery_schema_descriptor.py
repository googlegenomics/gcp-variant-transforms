# Copyright 2018 Google Inc.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""A dummy version of SchemaDescriptor class for testing."""

from __future__ import absolute_import


class DummySchemaDescriptor(object):
  """Dummy schema descriptor that returns default value to an API call."""

  def has_simple_field(self, field_name):
    #pylint: disable=unused-argument
    return True

  def get_field_descriptor(self, field_name):
    #pylint: disable=unused-argument
    return None

  def get_record_schema_descriptor(self, record_name):
    #pylint: disable=unused-argument
    return self
