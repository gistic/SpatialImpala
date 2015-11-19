// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/url-parser.h"
#include "runtime/string-value.inline.h"

#include "common/names.h"

namespace impala {

const StringValue UrlParser::url_authority(const_cast<char*>("AUTHORITY"), 9);
const StringValue UrlParser::url_file(const_cast<char*>("FILE"), 4);
const StringValue UrlParser::url_host(const_cast<char*>("HOST"), 4);
const StringValue UrlParser::url_path(const_cast<char*>("PATH"), 4);
const StringValue UrlParser::url_protocol(const_cast<char*>("PROTOCOL"), 8);
const StringValue UrlParser::url_query(const_cast<char*>("QUERY"), 5);
const StringValue UrlParser::url_ref(const_cast<char*>("REF"), 3);
const StringValue UrlParser::url_userinfo(const_cast<char*>("USERINFO"), 8);
const StringValue UrlParser::protocol(const_cast<char*>("://"), 3);
const StringValue UrlParser::at(const_cast<char*>("@"), 1);
const StringValue UrlParser::slash(const_cast<char*>("/"), 1);
const StringValue UrlParser::colon(const_cast<char*>(":"), 1);
const StringValue UrlParser::question(const_cast<char*>("?"), 1);
const StringValue UrlParser::hash(const_cast<char*>("#"), 1);
const StringSearch UrlParser::protocol_search(&protocol);
const StringSearch UrlParser::at_search(&at);
const StringSearch UrlParser::slash_search(&slash);
const StringSearch UrlParser::colon_search(&colon);
const StringSearch UrlParser::question_search(&question);
const StringSearch UrlParser::hash_search(&hash);

bool UrlParser::ParseUrl(const StringValue& url, UrlPart part, StringValue* result) {
  result->ptr = NULL;
  result->len = 0;
  // Remove leading and trailing spaces.
  StringValue trimmed_url = url.Trim();

  // All parts require checking for the protocol.
  int32_t protocol_pos = protocol_search.Search(&trimmed_url);
  if (protocol_pos < 0) return false;
  // Positioned to first char after '://'.
  StringValue protocol_end = trimmed_url.Substring(protocol_pos + protocol.len);

  switch(part) {
    case AUTHORITY: {
      // Find first '/'.
      int32_t end_pos = slash_search.Search(&protocol_end);
      *result = protocol_end.Substring(0, end_pos);
      break;
    }

    case FILE:
    case PATH: {
      // Find first '/'.
      int32_t start_pos = slash_search.Search(&protocol_end);
      if (start_pos < 0) {
        // Return empty string. This is what Hive does.
        return true;
      }
      StringValue path_start = protocol_end.Substring(start_pos);
      int32_t end_pos;
      if (part == FILE) {
        // End at '#'.
        end_pos = hash_search.Search(&path_start);
      } else {
        // End string at next '?' or '#'.
        end_pos = question_search.Search(&path_start);
        if (end_pos < 0) {
          // No '?' was found, look for '#'.
          end_pos = hash_search.Search(&path_start);
        }
      }
      *result = path_start.Substring(0, end_pos);
      break;
    }

    case HOST: {
      // Find '@'.
      int32_t start_pos = at_search.Search(&protocol_end);
      if (start_pos < 0) {
        // No '@' was found, i.e., no user:pass info was given, start after protocol.
        start_pos = 0;
      } else {
        // Skip '@'.
        start_pos += at.len;
      }
      StringValue host_start = protocol_end.Substring(start_pos);

      // Find the start of the query
      int32_t query_start_pos = question_search.Search(&host_start);
      StringValue url_only = host_start.Substring(0, query_start_pos);

      // Find the first '/' in url_only to determine host<:port>
      int32_t hostport_end_pos = slash_search.Search(&url_only);
      StringValue hostport = url_only.Substring(0, hostport_end_pos);

      // Find ':' to strip out port.
      int32_t end_pos = colon_search.Search(&hostport);
      *result = hostport.Substring(0, end_pos);
      break;
    }

    case PROTOCOL: {
      *result = trimmed_url.Substring(0, protocol_pos);
      break;
    }

    case QUERY: {
      // Find first '?'.
      int32_t start_pos = question_search.Search(&protocol_end);
      if (start_pos < 0) {
        // Indicate no query was found.
        return false;
      }
      StringValue query_start = protocol_end.Substring(start_pos + question.len);
      // End string at next '#'.
      int32_t end_pos = hash_search.Search(&query_start);
      *result = query_start.Substring(0, end_pos);
      break;
    }

    case REF: {
      // Find '#'.
      int32_t start_pos = hash_search.Search(&protocol_end);
      if (start_pos < 0) {
        // Indicate no user and pass were given.
        return false;
      }
      *result = protocol_end.Substring(start_pos + hash.len);
      break;
    }

    case USERINFO: {
      // Find '@'.
      int32_t end_pos = at_search.Search(&protocol_end);
      if (end_pos < 0) {
        // Indicate no user and pass were given.
        return false;
      }
      *result = protocol_end.Substring(0, end_pos);
      break;
    }

    case INVALID: return false;
  }
  return true;
}

bool UrlParser::ParseUrlKey(const StringValue& url, UrlPart part,
      const StringValue& key, StringValue* result) {
  // Part must be query to ask for a specific query key.
  if (part != QUERY) {
    return false;
  }
  // Remove leading and trailing spaces.
  StringValue trimmed_url = url.Trim();

  // Search for the key in the url, ignoring malformed URLs for now.
  StringSearch key_search(&key);
  while(trimmed_url.len > 0) {
    // Search for the key in the current substring.
    int32_t key_pos = key_search.Search(&trimmed_url);
    bool match = true;
    if (key_pos < 0) {
      return false;
    }
    // Key pos must be != 0 because it must be preceded by a '?' or a '&'.
    // Check that the char before key_pos is either '?' or '&'.
    if (key_pos == 0 ||
        (trimmed_url.ptr[key_pos - 1] != '?' && trimmed_url.ptr[key_pos - 1] != '&')) {
      match = false;
    }
    // Advance substring beyond matching key.
    trimmed_url = trimmed_url.Substring(key_pos + key.len);
    if (!match) {
      continue;
    }
    if (trimmed_url.len <= 0) {
      break;
    }
    // Next character must be '=', otherwise the match cannot be a key in the query part.
    if (trimmed_url.ptr[0] != '=') {
      continue;
    }
    int32_t pos = 1;
    // Find ending position of key's value by matching '#' or '&'.
    while(pos < trimmed_url.len) {
      switch(trimmed_url.ptr[pos]) {
        case '#':
        case '&':
          *result = trimmed_url.Substring(1, pos - 1);
          return true;
      }
      ++pos;
    }
    // Ending position is end of string.
    *result = trimmed_url.Substring(1);
    return true;
  }
  return false;
}

UrlParser::UrlPart UrlParser::GetUrlPart(const StringValue& part) {
  // Quick filter on requested URL part, based on first character.
  // Hive requires the requested URL part to be all upper case.
  switch(part.ptr[0]) {
    case 'A': {
      if (!part.Eq(url_authority)) return INVALID;
      return AUTHORITY;
    }
    case 'F': {
      if (!part.Eq(url_file)) return INVALID;
      return FILE;
    }
    case 'H': {
      if (!part.Eq(url_host)) return INVALID;
      return HOST;
    }
    case 'P': {
      if (part.Eq(url_path)) {
        return PATH;
      } else if (part.Eq(url_protocol)) {
        return PROTOCOL;
      } else {
        return INVALID;
      }
    }
    case 'Q': {
      if (!part.Eq(url_query)) return INVALID;
      return QUERY;
    }
    case 'R': {
      if (!part.Eq(url_ref)) return INVALID;
      return REF;
    }
    case 'U': {
      if (!part.Eq(url_userinfo)) return INVALID;
      return USERINFO;
    }
    default: return INVALID;
  }
}

}
