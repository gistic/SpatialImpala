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

#include "testutil/in-process-servers.h"

#include "statestore/statestore.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "util/network-util.h"
#include "util/webserver.h"
#include "util/default-path-handlers.h"
#include "util/metrics.h"
#include "runtime/exec-env.h"
#include "service/impala-server.h"

#include "common/names.h"

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);

using namespace apache::thrift;
using namespace impala;

InProcessImpalaServer::InProcessImpalaServer(const string& hostname, int backend_port,
    int subscriber_port, int webserver_port, const string& statestore_host,
    int statestore_port)
  : hostname_(hostname), backend_port_(backend_port),
    impala_server_(NULL),
    exec_env_(new ExecEnv(hostname, backend_port, subscriber_port, webserver_port,
                          statestore_host, statestore_port)) {
}

void InProcessImpalaServer::SetCatalogInitialized() {
  DCHECK(impala_server_ != NULL) << "Call Start*() first.";
  exec_env_->frontend()->SetCatalogInitialized();
}

Status InProcessImpalaServer::StartWithClientServers(int beeswax_port, int hs2_port,
    bool use_statestore) {
  RETURN_IF_ERROR(exec_env_->StartServices());
  ThriftServer* be_server;
  ThriftServer* hs2_server;
  ThriftServer* beeswax_server;
  RETURN_IF_ERROR(CreateImpalaServer(exec_env_.get(), beeswax_port, hs2_port,
                                     backend_port_, &beeswax_server, &hs2_server,
                                     &be_server, &impala_server_));
  be_server_.reset(be_server);
  hs2_server_.reset(hs2_server);
  beeswax_server_.reset(beeswax_server);

  RETURN_IF_ERROR(be_server_->Start());
  RETURN_IF_ERROR(hs2_server_->Start());
  RETURN_IF_ERROR(beeswax_server_->Start());

  // Wait for up to 1s for the backend server to start
  RETURN_IF_ERROR(WaitForServer(hostname_, backend_port_, 100, 100));

  return Status::OK();
}

Status InProcessImpalaServer::StartAsBackendOnly(bool use_statestore) {
  RETURN_IF_ERROR(exec_env_->StartServices());
  ThriftServer* be_server;
  RETURN_IF_ERROR(CreateImpalaServer(exec_env_.get(), 0, 0, backend_port_, NULL, NULL,
                                     &be_server, &impala_server_));
  be_server_.reset(be_server);
  RETURN_IF_ERROR(be_server_->Start());
  return Status::OK();
}

Status InProcessImpalaServer::Join() {
  be_server_->Join();
  return Status::OK();
}

InProcessStatestore::InProcessStatestore(int statestore_port, int webserver_port)
    : webserver_(new Webserver(webserver_port)),
      metrics_(new MetricGroup("statestore")),
      statestore_port_(statestore_port),
      statestore_(new Statestore(metrics_.get())) {
  AddDefaultUrlCallbacks(webserver_.get());
  statestore_->RegisterWebpages(webserver_.get());
}

Status InProcessStatestore::Start() {
  webserver_->Start();
  shared_ptr<TProcessor> processor(
      new StatestoreServiceProcessor(statestore_->thrift_iface()));

  statestore_server_.reset(new ThriftServer("StatestoreService", processor,
      statestore_port_, NULL, metrics_.get(), 5));
  if (EnableInternalSslConnections()) {
    LOG(INFO) << "Enabling SSL for Statestore";
    EXIT_IF_ERROR(statestore_server_->EnableSsl(
        FLAGS_ssl_server_certificate, FLAGS_ssl_private_key));
  }
  statestore_main_loop_.reset(
      new Thread("statestore", "main-loop", &Statestore::MainLoop, statestore_.get()));

  RETURN_IF_ERROR(statestore_server_->Start());
  return WaitForServer("localhost", statestore_port_, 10, 100);
}
