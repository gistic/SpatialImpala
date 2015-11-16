// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.testutil;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.authorization.SentryConfig;

/**
 * Wrapper script for starting a Sentry Policy Server in our test environment.
 */
public class SentryServiceWrapper {
  private final static Logger LOG =
      LoggerFactory.getLogger(SentryServiceWrapper.class);
  private final SentryConfig serviceConfig_;

  public SentryServiceWrapper(SentryConfig serviceConfig) throws Exception {
    serviceConfig_ = serviceConfig;
    serviceConfig_.loadConfig();
  }

  public void start() throws Exception {
    // Start the server.
    LOG.info("Starting Sentry Policy Server...");
    startSentryService();
    LOG.info(String.format("Sentry Policy Server running on: %s:%s",
        serviceConfig_.getConfig().get("sentry.service.server.rpc-address"),
        serviceConfig_.getConfig().get("sentry.service.server.rpc-port")));
  }

  private void startSentryService() throws Exception {
    throw new UnsupportedOperationException(
        "Sentry Policy Service is not supported on CDH4");
  }

  // Suppress warnings from OptionBuilder.
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    // Parse command line options to get config file path.
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("config_file")
        .withDescription("Absolute path to a sentry-site.xml config file")
        .hasArg()
        .withArgName("CONFIG_FILE")
        .isRequired()
        .create('c'));
    BasicParser optionParser = new BasicParser();
    CommandLine cmdArgs = optionParser.parse(options, args);

    SentryServiceWrapper server = new SentryServiceWrapper(
        new SentryConfig(cmdArgs.getOptionValue("config_file")));
    server.start();
  }
}