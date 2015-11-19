// This file will be removed when the code is accepted into the Thrift library.
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _THRIFT_TRANSPORT_TSASL_H_
#define _THRIFT_TRANSPORT_TSASL_H_ 1

#include <string>
#include <map>
#include <stdint.h>
#include <stdexcept>

#ifdef _WIN32
#include <sasl.h>
#include <saslplug.h>
#include <saslutil.h>
#else /* _WIN32 */
#include <sasl/sasl.h>
#include <sasl/saslplug.h>
#include <sasl/saslutil.h>
#endif

#include <thrift/transport/TTransportException.h>

using namespace apache::thrift::transport;

namespace sasl {
class SaslException : public TTransportException {
  public:
    SaslException(const char* msg) : TTransportException(msg) {
  }
};

/**
 * These classes implement the Simple Authentication and Security Layer (SASL)
 * authentication mechanisms.  see: http://www.ietf.org/rfc/rfc2222.txt.
 * They are mostly wrappers for the cyrus-sasl library routines.
 */
class TSasl {
  public:
   virtual ~TSasl() { sasl_dispose(&conn); }

  /*
   * Called once per application to free resources.`
   * Note that there is no distinction in the sasl library between being done
   * with servers or done with clients.  Internally the library maintains a which
   * is being used.  A call to SaslDone should only happen after all clients
   * and servers are finished.
   */
  static void SaslDone() {
    sasl_done();
  }

  /* Evaluates the challenge or response data and generates a response. */
  virtual uint8_t* evaluateChallengeOrResponse(const uint8_t* challenge,
                                               uint32_t len, uint32_t* resLen) = 0;

  /* Determines whether the authentication exchange has completed. */
  bool isComplete() {
    return authComplete;
  }

  /*
   * Unwraps a received byte array.
   * Returns a buffer for unwrapped result, and sets
   * 'len' to the buffer's length. The buffer is only valid until the next call, or
   * until the client is closed.
   */
  uint8_t* unwrap(const uint8_t* incoming, const int offset,
                  const uint32_t len, uint32_t* outLen);

  /*
   * Wraps a byte array to be sent.
   * Returns a buffer of wrapped result, and sets
   * 'len' to the buffer's length. The buffer is only valid until the next call, or
   * until the client is closed.
   */
  uint8_t* wrap(const uint8_t* outgoing, int offset,
                  const uint32_t len, uint32_t* outLen);

  /* Returns the IANA-registered mechanism name. */
  virtual std::string getMechanismName() {  return NULL; }

  /* Determines whether this mechanism has an optional initial response. */
  virtual bool hasInitialResponse() { return false; }

  /* Returns the username from the underlying sasl connection. */
  std::string getUsername();

  protected:
   /* Authorization is complete. */
   bool authComplete;
   /* Sasl Connection. */
   sasl_conn_t* conn;
};

class SaslClientImplException : public SaslException {
  public:
    SaslClientImplException(const char* errMsg)
        : SaslException(errMsg) {
    }
};

/* Client sasl implementation class. */
class TSaslClient : public sasl::TSasl {
  public:
    TSaslClient(const std::string& mechanisms, const std::string& authorizationId,
                const std::string& protocol, const std::string& serverName,
                const std::map<std::string,std::string>& props,
                sasl_callback_t* callbacks);

    static void SaslInit(sasl_callback_t* callbacks) {
      int result = sasl_client_init(callbacks);
      if (result != SASL_OK)
        throw SaslClientImplException(sasl_errstring(result, NULL, NULL));
    }

    /* Evaluates the challenge data and generates a response. */
    uint8_t* evaluateChallengeOrResponse(const uint8_t* challenge,
                                         const uint32_t len, uint32_t* outLen);

    /* Returns the IANA-registered mechanism name of this SASL client. */
    virtual std::string getMechanismName();

    /* Retrieves the negotiated property */
    std::string     getNegotiatedProperty(const std::string& propName);

    /* Determines whether this mechanism has an optional initial response. */
    virtual bool hasInitialResponse();

  private :
   /* true if sasl_client_start has been called. */
   bool clientStarted;

   /* The chosen mechanism. */
   std::string chosenMech;

   /* List of possible mechanisms. */
   std::string mechList;
};

class SaslServerImplException : public SaslException {
  public:
    SaslServerImplException(const char* errMsg)
        : SaslException(errMsg) {
    }
};

/* Server sasl implementation class. */
class TSaslServer : public sasl::TSasl {
 public:
  TSaslServer(const std::string& service, const std::string& serverFQDN,
              const std::string& userRealm, unsigned flags, sasl_callback_t* callbacks);

  /*
   * This initializes the sasl server library and should be called onece per application
   */
  static void SaslInit(const sasl_callback_t* callbacks, const std::string& appname) {
    int result = sasl_server_init(callbacks, appname.c_str());
    if (result != SASL_OK) {
      throw SaslServerImplException(sasl_errstring(result, NULL, NULL));
    }
  }

  /* Evaluates the response data and generates a challenge. */
  virtual uint8_t* evaluateChallengeOrResponse(const uint8_t* challenge,
                                               const uint32_t len, uint32_t* resLen);
 private:
  /* true if sasl_server_start has been called. */
  bool serverStarted;
};
}
#endif /* _THRIFT_TRANSPORT_TSALS_H_ */
