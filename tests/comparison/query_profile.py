# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

from logging import getLogger
from random import choice, randint, random

from tests.comparison.types import (
    Boolean,
    Char,
    Decimal,
    Float,
    Int,
    TYPES,
    Timestamp)
from tests.comparison.funcs import WindowBoundary
from tests.comparison.data_generator import RandomValGenerator

UNBOUNDED_PRECEDING = WindowBoundary.UNBOUNDED_PRECEDING
PRECEDING = WindowBoundary.PRECEDING
CURRENT_ROW = WindowBoundary.CURRENT_ROW
FOLLOWING = WindowBoundary.FOLLOWING
UNBOUNDED_FOLLOWING = WindowBoundary.UNBOUNDED_FOLLOWING

LOG = getLogger()

class DefaultProfile(object):

  def __init__(self):
    # Bounds are (min, max) values, the actual value used will be selected from the
    # bounds and each value within the range has an equal probability of being selected.
    self._bounds = {
        'MAX_NESTED_QUERY_COUNT': (0, 2),
        'MAX_NESTED_EXPR_COUNT': (0, 2),
        'SELECT_ITEM_COUNT': (1, 5),
        'WITH_TABLE_COUNT': (1, 3),
        'TABLE_COUNT': (1, 2),
        'ANALYTIC_LEAD_LAG_OFFSET': (1, 100),
        'ANALYTIC_WINDOW_OFFSET': (1, 100)}

    # Below are interdependent weights used to determine probabilities. The probability
    # of any item being selected should be (item weight) / sum(weights). A weight of
    # zero means the item will never be selected.
    self._weights = {
        'SELECT_ITEM_CATEGORY': {
            'AGG': 3,
            'ANALYTIC': 1,
            'BASIC': 10},
        'TYPES': {
            Boolean: 1,
            Char: 1,
            Decimal: 1,
            Float: 1,
            Int: 10,
            Timestamp: 1},
        'ANALYTIC_WINDOW': {
            ('ROWS', UNBOUNDED_PRECEDING, None): 1,
            ('ROWS', UNBOUNDED_PRECEDING, PRECEDING): 2,
            ('ROWS', UNBOUNDED_PRECEDING, CURRENT_ROW): 1,
            ('ROWS', UNBOUNDED_PRECEDING, FOLLOWING): 2,
            ('ROWS', UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING): 2,
            ('ROWS', PRECEDING, None): 1,
            ('ROWS', PRECEDING, PRECEDING): 2,
            ('ROWS', PRECEDING, CURRENT_ROW): 1,
            ('ROWS', PRECEDING, FOLLOWING): 2,
            ('ROWS', PRECEDING, UNBOUNDED_FOLLOWING): 2,
            ('ROWS', CURRENT_ROW, None): 1,
            ('ROWS', CURRENT_ROW, CURRENT_ROW): 1,
            ('ROWS', CURRENT_ROW, FOLLOWING): 2,
            ('ROWS', CURRENT_ROW, UNBOUNDED_FOLLOWING): 2,
            ('ROWS', FOLLOWING, FOLLOWING): 2,
            ('ROWS', FOLLOWING, UNBOUNDED_FOLLOWING): 2,
            # Ranges not yet supported
            ('RANGE', UNBOUNDED_PRECEDING, None): 0,
            ('RANGE', UNBOUNDED_PRECEDING, PRECEDING): 0,
            ('RANGE', UNBOUNDED_PRECEDING, CURRENT_ROW): 0,
            ('RANGE', UNBOUNDED_PRECEDING, FOLLOWING): 0,
            ('RANGE', UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING): 0,
            ('RANGE', PRECEDING, None): 0,
            ('RANGE', PRECEDING, PRECEDING): 0,
            ('RANGE', PRECEDING, CURRENT_ROW): 0,
            ('RANGE', PRECEDING, FOLLOWING): 0,
            ('RANGE', PRECEDING, UNBOUNDED_FOLLOWING): 0,
            ('RANGE', CURRENT_ROW, None): 0,
            ('RANGE', CURRENT_ROW, CURRENT_ROW): 0,
            ('RANGE', CURRENT_ROW, FOLLOWING): 0,
            ('RANGE', CURRENT_ROW, UNBOUNDED_FOLLOWING): 0,
            ('RANGE', FOLLOWING, FOLLOWING): 0,
            ('RANGE', FOLLOWING, UNBOUNDED_FOLLOWING): 0},
        'JOIN': {
            'INNER': 90,
            'LEFT': 30,
            'RIGHT': 10,
            'FULL_OUTER': 3,
            'CROSS': 1},
        'SUBQUERY_PREDICATE': {
            ('Exists', 'AGG', 'CORRELATED'): 0,   # Not supported
            ('Exists', 'AGG', 'UNCORRELATED'): 1,
            ('Exists', 'NON_AGG', 'CORRELATED'): 1,
            ('Exists', 'NON_AGG', 'UNCORRELATED'): 1,
            ('NotExists', 'AGG', 'CORRELATED'): 0,   # Not supported
            ('NotExists', 'AGG', 'UNCORRELATED'): 0,   # Not supported
            ('NotExists', 'NON_AGG', 'CORRELATED'): 1,
            ('NotExists', 'NON_AGG', 'UNCORRELATED'): 0,   # Not supported
            ('In', 'AGG', 'CORRELATED'): 0,   # Not supported
            ('In', 'AGG', 'UNCORRELATED'): 0,   # Not supported
            ('In', 'NON_AGG', 'CORRELATED'): 1,
            ('In', 'NON_AGG', 'UNCORRELATED'): 1,
            ('NotIn', 'AGG', 'CORRELATED'): 0,   # Not supported
            ('NotIn', 'AGG', 'UNCORRELATED'): 1,
            ('NotIn', 'NON_AGG', 'CORRELATED'): 1,
            ('NotIn', 'NON_AGG', 'UNCORRELATED'): 1,
            ('Scalar', 'AGG', 'CORRELATED'): 0,   # Not supported
            ('Scalar', 'AGG', 'UNCORRELATED'): 1,
            ('Scalar', 'NON_AGG', 'CORRELATED'): 0,   # Not supported
            ('Scalar', 'NON_AGG', 'UNCORRELATED'): 1},
        'QUERY_EXECUTION': {   # Used by the discrepancy searcher
            'CREATE_TABLE_AS': 1,
            'RAW': 10,
            'VIEW': 1}}

    # On/off switches
    self._flags = {
        'ANALYTIC_DESIGNS': {
            'TOP_LEVEL_QUERY_WITHOUT_LIMIT': True,
            'DETERMINISTIC_ORDER_BY': True,
            'NO_ORDER_BY': True,
            'ONLY_SELECT_ITEM': True,
            'UNBOUNDED_WINDOW': True,
            'RANK_FUNC': True}}

    # Independent probabilities where 1 means 100%. These values may be ignored depending
    # on the context. For example, GROUP_BY is almost always ignored and instead
    # determined by the SELECT item weights above, since mixing aggregate and
    # non-aggregate items requires the use of a GROUP BY. The GROUP_BY option below is
    # only applied if all of the SELECT items are non-aggregate.
    self._probabilities = {
        'OPTIONAL_QUERY_CLAUSES': {
            'WITH': 0.1,   # MAX_NESTED_QUERY_COUNT bounds take precedence
            'FROM': 1,
            'WHERE': 0.5,
            'GROUP_BY': 0.1,   # special case, doesn't really do much, see comment above
            'HAVING': 0.25,
            'UNION': 0.1,
            'ORDER_BY': 0.1},
        'OPTIONAL_ANALYTIC_CLAUSES': {
            'PARTITION_BY': 0.5,
            'ORDER_BY': 0.5,
            'WINDOW': 0.5},   # will only be used if ORDER BY is chosen
        'MISC': {
            'INLINE_VIEW': 0.1,   # MAX_NESTED_QUERY_COUNT bounds take precedence
            'SELECT_DISTINCT': 0.1,
            'SCALAR_SUBQUERY': 0.1,
            'UNION_ALL': 0.5}}   # Determines use of "ALL" but not "UNION"

    self.__type_weights = {}

    self.constant_generator = RandomValGenerator()

  def _get_config_value(self, start_config, *keys):
    value = start_config
    for key in keys:
      value = value[key]
    return value

  def weights(self, *keys):
    '''Convenience method for getting the values of named weights'''
    return self._get_config_value(self._weights, *keys)

  def bounds(self, *keys):
    '''Convenience method for getting the values of named bounds'''
    return self._get_config_value(self._bounds, *keys)

  def probability(self, *keys):
    '''Convenience method for getting the value of named probabilities'''
    return self._get_config_value(self._probabilities, *keys)

  def _choose_from_bounds(self, *bounds):
    '''Returns a value that is within the given bounds. Each value has an equal chance
       of being chosen.
    '''
    if isinstance(bounds[0], str):
      lower, upper = self.bounds(*bounds)
    else:
      lower, upper = bounds
    return randint(lower, upper)

  def _choose_from_weights(self, *weights):
    '''Returns a value that is selected from the keys of weights with the probability
       determined by the values of weights.
    '''
    if isinstance(weights[0], str):
      weights = self.weights(*weights)
    else:
      weights = weights[0]
    total_weight = sum(weights.itervalues())
    numeric_choice = randint(1, total_weight)
    for choice_, weight in weights.iteritems():
      if weight <= 0:
        continue
      if numeric_choice <= weight:
        return choice_
      numeric_choice -= weight

  def _choose_from_filtered_weights(self, filter, *weights):
    '''Convenience method, apply the given filter before choosing a value.'''
    if isinstance(weights[0], str):
      weights = self.weights(*weights)
    else:
      weights = weights[0]
    return self._choose_from_weights(dict(
      (choice_, weight) for choice_, weight in weights.iteritems() if filter(choice_)))

  def _decide_from_probability(self, *keys):
    return random() < self.probability(*keys)

  def get_max_nested_query_count(self):
    '''Return the maximum number of queries the top level query may contain.'''
    return self._choose_from_bounds('MAX_NESTED_QUERY_COUNT')

  def use_with_clause(self):
    return self._decide_from_probability('OPTIONAL_QUERY_CLAUSES', 'WITH')

  def get_with_clause_table_ref_count(self):
    '''Return the number of table ref entries a WITH clause should contain.'''
    return self._choose_from_bounds('WITH_TABLE_COUNT')

  def get_select_item_count(self):
    return self._choose_from_bounds('SELECT_ITEM_COUNT')

  def choose_nested_expr_count(self):
    return self._choose_from_bounds('MAX_NESTED_EXPR_COUNT')

  def allowed_analytic_designs(self):
    return [design for design, is_enabled in self._flags['ANALYTIC_DESIGNS'].iteritems()
            if is_enabled]

  def use_partition_by_clause_in_analytic(self):
    return self._decide_from_probability('OPTIONAL_ANALYTIC_CLAUSES', 'PARTITION_BY')

  def use_order_by_clause_in_analytic(self):
    return self._decide_from_probability('OPTIONAL_ANALYTIC_CLAUSES', 'ORDER_BY')

  def use_window_in_analytic(self):
    return self._decide_from_probability('OPTIONAL_ANALYTIC_CLAUSES', 'WINDOW')

  def choose_window_type(self):
    return self._choose_from_weights('ANALYTIC_WINDOW')

  def get_window_offset(self):
    return self._choose_from_bounds('ANALYTIC_WINDOW_OFFSET')

  def get_offset_for_analytic_lead_or_lag(self):
    return self._choose_from_bounds('ANALYTIC_LEAD_LAG_OFFSET')

  def get_table_count(self):
    return self._choose_from_bounds('TABLE_COUNT')

  def use_inline_view(self):
    return self._decide_from_probability('MISC', 'INLINE_VIEW')

  def choose_table(self, table_exprs):
    return choice(table_exprs)

  def choose_join_type(self, join_types):
    return self._choose_from_filtered_weights(
        lambda join_type: join_type in join_types, 'JOIN')

  def get_join_condition_count(self):
    return self._choose_from_bounds('MAX_NESTED_EXPR_COUNT')

  def use_where_clause(self):
    return self._decide_from_probability('OPTIONAL_QUERY_CLAUSES', 'WHERE')

  def use_scalar_subquery(self):
    return self._decide_from_probability('MISC', 'SCALAR_SUBQUERY')

  def choose_subquery_predicate_category(self, func_name, allow_correlated):
    weights = self.weights('SUBQUERY_PREDICATE')
    func_names = set(name for name, _, _ in weights.iterkeys())
    if func_name not in func_names:
      func_name = 'Scalar'
    allow_agg = self.weights('SELECT_ITEM_CATEGORY').get('AGG', 0)
    if allow_correlated and self.bounds('TABLE_COUNT')[1] == 0:
      allow_correlated = False
    weights = dict(((name, use_agg, use_correlated), weight)
                   for (name, use_agg, use_correlated), weight in weights.iteritems()
                   if name == func_name \
                       and (allow_agg or use_agg == 'NON_AGG') \
                       and weight)
    if weights:
      return self._choose_from_weights(weights)

  def use_distinct(self):
    return self._decide_from_probability('MISC', 'SELECT_DISTINCT')

  def use_distinct_in_func(self):
    return self._decide_from_probability('MISC', 'SELECT_DISTINCT')

  def use_group_by_clause(self):
    return self._decide_from_probability('OPTIONAL_QUERY_CLAUSES', 'GROUP_BY')

  def use_having_clause(self):
    return self._decide_from_probability('OPTIONAL_QUERY_CLAUSES', 'HAVING')

  def use_union_clause(self):
    return self._decide_from_probability('OPTIONAL_QUERY_CLAUSES', 'UNION')

  def use_union_all(self):
    return self._decide_from_probability('MISC', 'UNION_ALL')

  def get_query_execution(self):
    return self._choose_from_weights('QUERY_EXECUTION')

  def use_having_without_groupby(self):
    return True

  def use_nested_with(self):
    return True

  # Workaround for Hive null ordering differences, and lack of 'NULL FIRST', 'NULL LAST'
  # specifications. The ref db will order nulls as specified for ASC sorting to make it
  # identifical to Hive. Valid return values are: 'BEFORE', 'AFTER', or 'DEFAULT',
  # the latter means no specification needed.
  def nulls_order_asc(self):
    return 'DEFAULT'

  def choose_val_expr(self, val_exprs, types=TYPES):
    if not val_exprs:
      raise Exception('At least on value is required')
    if not types:
      raise Exception('At least one type is required')
    available_types = set(types) & set(val_exprs.by_type)
    if not available_types:
      raise Exception('None of the provided values return any of the required types')
    val_type = self.choose_type(available_types)
    return choice(val_exprs.by_type[val_type])

  def choose_constant(self, return_type=None, allow_null=True):
    if not return_type:
      return_type = self.choose_type()
    while True:
      val = self.constant_generator.generate_val(return_type)
      if val is None and not allow_null:
        continue
      return return_type(val)

  def choose_type(self, types=TYPES):
    type_weights = self.weights('TYPES')
    weights = dict((type_, type_weights[type_]) for type_ in types)
    if not weights:
      raise Exception('None of the requested types are enabled')
    return self._choose_from_weights(weights)

  def choose_func_signature(self, signatures):
    '''Return a signature chosen from "signatures".'''
    if not signatures:
      raise Exception('At least one signature is required')

    type_weights = self.weights('TYPES')
    # First a function will be chosen then a signature. This is done so that the number
    # of signatures a function has doesn't influence its likelihood of being chosen.
    # Functions will be weighted based on the weight of the types in their arguments.
    # The weights will be normalized by the number of arguments in the signature. The
    # weight of a function will be the maximum weight out of all of it's signatures.
    # If any signature has a type with a weight of zero, the signature will not be used.
    #
    # Example: type_weights = {Int: 10, Float: 1},
    #          funcs = [foo(Int), foo(Float), bar(Int, Float)]
    #
    #          max signature length = 2   # from bar(Int, Float)
    #          weight of foo(Int) = (10 * 2)
    #          weight of foo(Float) = (1 * 2)
    #          weight of bar(Int, Float) = ((10 + 1) * 1)
    #          func_weights = {foo: 20, bar: 11}
    #
    # Note that this only selects a function, the function signature will be selected
    # later. This is done to prevent function with a greater number of signatures from
    # being selected more frequently.
    func_weights = dict()
    # The length of the signature in func_weights
    signature_length_by_func = dict()
    for signature in signatures:
      signature_weight = type_weights[signature.return_type]
      signature_length = 1
      for arg in signature.args:
        if arg.is_subquery:
          for subtype in arg.type:
            signature_weight *= type_weights[subtype]
            signature_length += 1
        else:
          signature_weight *= type_weights[arg.type]
          signature_length += 1
      if not signature_weight:
        continue
      if not signature.func in func_weights \
          or signature_weight > func_weights[signature.func]:
        func_weights[signature.func] = signature_weight
        signature_length_by_func[signature.func] = signature_length
    if not func_weights:
      raise Exception('All functions disallowed based on signature types')
    distinct_signature_lengths = set(signature_length_by_func.values())
    for func, weight in func_weights.iteritems():
      signature_length = signature_length_by_func[func]
      func_weights[func] = reduce(
          lambda x, y: x * y,
          distinct_signature_lengths - set([signature_length]),
          func_weights[func])
    func = self._choose_from_weights(func_weights)

    # Same idea as above but for the signatures of the selected function.
    signature_weights = dict()
    signature_lengths = dict()
    for idx, signature in enumerate(func.signatures()):
      if signature not in signatures:
        continue
      signature_weight = type_weights[signature.return_type]
      signature_length = 1
      for arg in signature.args:
        if arg.is_subquery:
          for subtype in arg.type:
            signature_weight *= type_weights[subtype]
            signature_length += 1
        else:
          signature_weight *= type_weights[arg.type]
          signature_length += 1
      if signature_weight:
        signature_weights[idx] = signature_weight
        signature_lengths[idx] = signature_length
    distinct_signature_lengths = set(signature_lengths.values())
    for idx, weight in signature_weights.iteritems():
      signature_length = signature_lengths[idx]
      signature_weights[idx] = reduce(
          lambda x, y: x * y,
          distinct_signature_lengths - set([signature_length]),
          signature_weights[idx])
    idx = self._choose_from_weights(signature_weights)
    return func.signatures()[idx]

  def allow_func_signature(self, signature):
    weights = self.weights('TYPES')
    if not weights[signature.return_type]:
      return False
    for arg in signature.args:
      if arg.is_subquery:
        if not all(weights[subtype] for subtype in arg.type):
          return False
      elif not weights[arg.type]:
        return False
    return True


class HiveProfile(DefaultProfile):
  def __init__(self):
      super(HiveProfile, self).__init__()

  def use_having_without_groupby(self):
    return False

  def use_nested_with(self):
    return False

  def nulls_order_asc(self):
    return 'BEFORE'

  def allow_func_signature(self, signature):
    if signature.func._NAME.startswith('DateAdd'):
      return False
    if signature.func._NAME in ('Greatest', 'Least'):
      type = signature.return_type
      argtypes = [arg.type for arg in signature.args]
      for argtype in argtypes:
        if type is None:
          type = argtype
          continue
        else:
          if type != argtype:
            return False
    return DefaultProfile.allow_func_signature(self, signature)


PROFILES = [var for var in locals().values()
            if isinstance(var, type) and var.__name__.endswith('Profile')]
