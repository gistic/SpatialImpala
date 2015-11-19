#!/usr/bin/env impala-python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

# Functions for generating test files with specific length, and ended with all
# permutation (with replacement) of items in suffix_list.

from shutil import rmtree
from optparse import OptionParser
from contextlib import contextmanager
from itertools import product
import os

parser = OptionParser()
parser.add_option("--table_dir", dest="table_dir", default=None)
parser.add_option("--only_newline", dest="only_newline", default=False, action="store_true")
parser.add_option("--file_len", dest="file_len", type="int")

def generate_testescape_files(table_location, only_newline, file_len):
  data = ''.join(["1234567890" for _ in xrange(1 + file_len / 10)])

  suffix_list = ["\\", ",", "a"]
  if only_newline:
    suffix_list.append("\n")
  else:
    suffix_list.append("\r\n")

  if os.path.exists(table_location):
    rmtree(table_location)

  os.mkdir(table_location)
  for count, p in enumerate(product(suffix_list, repeat=len(suffix_list))):
    ending = ''.join(p)
    content = data[:file_len - len(ending)] + ending
    with open(os.path.join(table_location, str(count)), 'w') as f:
      f.write(content)

if __name__ == "__main__":
  (options, args) = parser.parse_args()
  if not options.table_dir:
    parser.error("--table_dir option must be specified")

  generate_testescape_files(options.table_dir, options.only_newline, options.file_len)
