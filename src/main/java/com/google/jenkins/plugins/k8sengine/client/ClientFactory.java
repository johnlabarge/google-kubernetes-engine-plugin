/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.jenkins.plugins.k8sengine.client;

import com.cloudbees.plugins.credentials.CredentialsMatchers;
import com.cloudbees.plugins.credentials.CredentialsProvider;
import com.cloudbees.plugins.credentials.domains.DomainRequirement;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.googleapis.services.GoogleClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.container.Container;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.jenkins.plugins.credentials.oauth.GoogleRobotCredentials;
import hudson.AbortException;
import hudson.model.ItemGroup;
import hudson.security.ACL;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;

/** Creates clients for communicating with Google APIs. */
public class ClientFactory {
  public static final String APPLICATION_NAME = "jenkins-google-gke-plugin";

  private final Credential credential;
  private final HttpTransport transport;
  private final JsonFactory jsonFactory;

  /**
   * Creates a {@link ClientFactory} instance.
   *
   * @param itemGroup A handle to the Jenkins instance.
   * @param domainRequirements A list of domain requirements.
   * @param credentialsId The ID of the GoogleRobotCredentials to be retrieved from Jenkins and
   *     utilized for authorization.
   * @param httpTransport If specified, the HTTP transport this factory will utilize for clients it
   *     creates.
   * @throws AbortException If failed to create a new client factory.
   */
  public ClientFactory(
      ItemGroup itemGroup,
      ImmutableList<DomainRequirement> domainRequirements,
      String credentialsId,
      Optional<HttpTransport> httpTransport)
      throws AbortException {
    Preconditions.checkNotNull(itemGroup);
    Preconditions.checkNotNull(domainRequirements);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(credentialsId));

    GoogleRobotCredentials robotCreds =
        CredentialsMatchers.firstOrNull(
            CredentialsProvider.lookupCredentials(
                GoogleRobotCredentials.class, itemGroup, ACL.SYSTEM, domainRequirements),
            CredentialsMatchers.withId(credentialsId));
    if (robotCreds == null) {
      throw new AbortException(Messages.ClientFactory_FailedToRetrieveCredentials(credentialsId));
    }

    try {
      this.credential = robotCreds.getGoogleCredential(new ContainerScopeRequirement());
    } catch (GeneralSecurityException gse) {
      throw new AbortException(
          Messages.ClientFactory_FailedToInitializeHTTPTransport(gse.getMessage()));
    }

    try {
      this.transport = httpTransport.orElse(GoogleNetHttpTransport.newTrustedTransport());
    } catch (GeneralSecurityException | IOException e) {
      throw new AbortException(
          Messages.ClientFactory_FailedToInitializeHTTPTransport(e.getMessage()));
    }

    this.jsonFactory = new JacksonFactory();
  }

  /**
   * Creates a new {@link ContainerClient}.
   *
   * @return A new {@link ContainerClient} instance.
   */
  public ContainerClient containerClient() {
    return new ContainerClient(
        new Container.Builder(transport, jsonFactory, credential)
            .setGoogleClientRequestInitializer(
                new GoogleClientRequestInitializer() {
                  @Override
                  public void initialize(AbstractGoogleClientRequest<?> request)
                      throws IOException {
                    request.setRequestHeaders(
                        request.getRequestHeaders().setUserAgent(APPLICATION_NAME));
                  }
                })
            .setHttpRequestInitializer(new RetryHttpInitializerWrapper(credential))
            .setApplicationName(APPLICATION_NAME)
            .build());
  }
}
