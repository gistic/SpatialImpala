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

#include "rpc/thrift-util.h"

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "util/hash-util.h"
#include "util/time.h"
#include "rpc/thrift-server.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Data_types.h"

// TCompactProtocol requires some #defines to work right.  They also define UNLIKELY
// so we need to undef this.
// TODO: is there a better include to use?
#ifdef UNLIKELY
#undef UNLIKELY
#endif
#define SIGNED_RIGHT_SHIFT_IS 1
#define ARITHMETIC_RIGHT_SHIFT 1

// Thrift does things like throw exception("some string " + int) which just returns
// garbage.
// TODO: get thrift to fix this.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wstring-plus-int"
#include <thrift/Thrift.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/protocol/TCompactProtocol.h>
#pragma clang diagnostic pop

#include "common/names.h"

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::concurrency;

// Thrift defines operator< but does not implement it. This is a stub
// implementation so we can link.
bool Apache::Hadoop::Hive::Partition::operator<(
    const Apache::Hadoop::Hive::Partition& x) const {
  DCHECK(false) << "This should not get called.";
  return false;
}

namespace impala {

ThriftSerializer::ThriftSerializer(bool compact, int initial_buffer_size) :
    mem_buffer_(new TMemoryBuffer(initial_buffer_size)) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> factory;
    protocol_ = factory.getProtocol(mem_buffer_);
  }
}

shared_ptr<TProtocol> CreateDeserializeProtocol(
    shared_ptr<TMemoryBuffer> mem, bool compact) {
  if (compact) {
    TCompactProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  } else {
    TBinaryProtocolFactoryT<TMemoryBuffer> tproto_factory;
    return tproto_factory.getProtocol(mem);
  }
}

// Comparator for THostPorts. Thrift declares this (in gen-cpp/Types_types.h) but
// never defines it.
bool TNetworkAddress::operator<(const TNetworkAddress& that) const {
  if (this->hostname < that.hostname) {
    return true;
  } else if ((this->hostname == that.hostname) && (this->port < that.port)) {
    return true;
  }
  return false;
};

// Comparator for TUniqueIds
bool TUniqueId::operator<(const TUniqueId& that) const {
  return (hi < that.hi) || (hi == that.hi &&  lo < that.lo);
}

bool TAccessEvent::operator<(const TAccessEvent& that) const {
  return this->name < that.name;
}

static void ThriftOutputFunction(const char* output) {
  VLOG_QUERY << output;
}

void InitThriftLogging() {
  GlobalOutput.setOutputFunction(ThriftOutputFunction);
}

Status WaitForLocalServer(const ThriftServer& server, int num_retries,
    int retry_interval_ms) {
  return WaitForServer("localhost", server.port(), num_retries, retry_interval_ms);
}

Status WaitForServer(const string& host, int port, int num_retries,
    int retry_interval_ms) {
  int retry_count = 0;
  while (retry_count < num_retries) {
    try {
      TSocket socket(host, port);
      // Timeout is in ms
      socket.setConnTimeout(500);
      socket.open();
      socket.close();
      return Status::OK();
    } catch (const TException& e) {
      VLOG_QUERY << "Connection failed: " << e.what();
    }
    ++retry_count;
    VLOG_QUERY << "Waiting " << retry_interval_ms << "ms for Thrift server at "
               << host << ":" << port
               << " to come up, failed attempt " << retry_count
               << " of " << num_retries;
    SleepForMs(retry_interval_ms);
  }
  return Status("Server did not come up");
}

std::ostream& operator<<(std::ostream& out, const TColumnValue& colval) {
  if (colval.__isset.bool_val) {
    out << ((colval.bool_val) ? "true" : "false");
  } else if (colval.__isset.double_val) {
    out << colval.double_val;
  } else if (colval.__isset.byte_val) {
    out << colval.byte_val;
  } else if (colval.__isset.short_val) {
    out << colval.short_val;
  } else if (colval.__isset.int_val) {
    out << colval.int_val;
  } else if (colval.__isset.long_val) {
    out << colval.long_val;
  } else if (colval.__isset.string_val) {
    out << colval.string_val;
  } else if (colval.__isset.binary_val) {
    out << colval.binary_val; // Stored as a std::string
  } else {
    out << "NULL";
  }
  return out;
}

bool TNetworkAddressComparator(const TNetworkAddress& a, const TNetworkAddress& b) {
  int cmp = a.hostname.compare(b.hostname);
  if (cmp < 0) return true;
  if (cmp == 0) return a.port < b.port;
  return false;
}

bool IsTimeoutTException(const TException& e) {
  // String taken from Thrift's TSocket.cpp
  return strstr(e.what(), "EAGAIN (timed out)") != NULL;
}

}
