# Copyright 2017 Google Inc.  All Rights Reserved.
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

"""Util functions for working with Variant objects."""

__all__ = ['is_snp', 'is_non_variant', 'is_mnp']


def is_snp(v):
  return v.start + 1 == v.end


def is_non_variant(v):
  return v.start + len(v.reference_bases) < v.end


def is_mnp(v):
  return v.start + len(v.reference_bases) == v.end and v.start + 1 != v.end
