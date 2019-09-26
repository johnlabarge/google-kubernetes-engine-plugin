/*
 * Copyright 2019 Google LLC
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

package com.google.jenkins.plugins.k8sengine;

import com.google.common.base.Preconditions;
import com.jayway.jsonpath.JsonPath;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Verification adapters for verifying ManifestObjects using kubectl and returning and logging the
 * result. Inner class, {@link VerificationResult} encapsulates the result and the class will keep a
 * registry of Verifiers per Kubernetes API Kind and Version.
 */
public class KubernetesVerifiers {
  private static final Logger LOGGER = Logger.getLogger(KubernetesEngineBuilder.class.getName());
  private static Map<String, Verifier> verifiers = new HashMap<String, Verifier>();
  private static final Verifier defaultVerifier = new DefaultVerifier();
  public static final String DEPLOYMENT_KIND = "deployment";
  // Register the available verifiers.
  static {
    verifiers.put("*/deployment", new DeploymentVerifier());
  }

  /**
   * Represents the result object for verification action. The status field represents whether the
   * verification succeeded. The toString() returns relevant information pertaining to why the
   * status is what it is.
   */
  public static class VerificationResult {
    private String log;
    private boolean status;
    private Manifests.ManifestObject manifestObject;

    /** @return Information relevant to the result and why the status is what it is. */
    private String getLog() {
      return log;
    }

    /** @return If the Kubernetes object was verified. */
    public boolean isVerified() {
      return status;
    }

    /** @return The {@link Manifests.ManifestObject} for which verification was attempted. */
    public Manifests.ManifestObject getManifestObject() {
      return manifestObject;
    }

    /**
     * Constructs a new {@link VerificationResult}.
     *
     * @param log Information relevant to why the status is what it is.
     * @param status The status of this verification.
     * @param object The {@link Manifests.ManifestObject} for which this verification was attempted.
     */
    public VerificationResult(String log, boolean status, Manifests.ManifestObject object) {
      this.log = log;
      this.status = status;
      this.manifestObject = object;
    }

    /**
     * Get text description of the result.
     *
     * @return Description of the result including whether it was successful (i8n) and the log
     *     pertaining to why.
     */
    public String toString() {
      return new StringBuilder()
          .append(
              (status)
                  ? Messages.KubernetesEngineBuilder_VerifyingLogSuccess(manifestObject.describe())
                  : Messages.KubernetesEngineBuilder_VerifyingLogFailure(manifestObject.describe()))
          .append("\n")
          .append(log)
          .toString();
    }
  }

  /**
   * A verifier is an adapter that uses a {@link KubectlWrapper} to verify that a {@link
   * Manifests.ManifestObject} was successfully applied to the Kubernetes cluster.
   */
  public interface Verifier {
    /**
     * Verify the Kubernetes object represented by the {@link Manifests.ManifestObject} was applied
     * to the Kubernetes cluster.
     *
     * @param kubectl A {@link KubectlWrapper} object for querying the object type in the cluster.
     * @param object The manifest to be verified.
     * @return true If the resource was verified, false otherwise.
     */
    VerificationResult verify(KubectlWrapper kubectl, Manifests.ManifestObject object);
  }

  /**
   * DefaultVerifier is a fallback verifier for types of objects that are not in the registry. It
   * fails verification and reports that a verify for the type of object is not implemented. This
   * verifier shouldn't be used in production.
   */
  private static class DefaultVerifier implements Verifier {

    /**
     * Default verifier returns false for unimplemented verifiers.
     *
     * @param kubectl A KubectlWrapper object for querying the object type in the cluster.
     * @param object The Kubernetes object to verify represented by the {@link
     *     Manifests.ManifestObject}
     * @return false The unimplemented Verifier case.
     */
    public VerificationResult verify(KubectlWrapper kubectl, Manifests.ManifestObject object) {
      LOGGER.info("Reached unimplemented default verifier.");
      return new VerificationResult(
          Messages.KubernetesEngineBuilder_VerifierNotImplementedFor(object.describe()),
          false,
          object);
    }
  }

  /**
   * A {@link Verifier} that verifies a "deployment" object. For a deployment object to be verified
   * it must have it's minimum number of replicas (spec.replicas) less than or equal to it's
   * available replicas (status.availableReplicas).
   */
  private static class DeploymentVerifier implements Verifier {
    private static final String AVAILABLE_REPLICAS = "availableReplicas";
    private static final String MINIMUM_REPLICAS_JSONPATH = "spec.replicas";
    private static final String STATUS_JSONPATH = "status";

    /**
     * Verifies that the deployment was applied to the GKE cluster.
     *
     * @param kubectl A {@link KubectlWrapper} object for querying the object type in the cluster.
     * @param object The deployment {@link Manifests.ManifestObject} that is being verified.
     * @return true If the minimum number of replicas is less than or equal to the available
     *     replicas.
     */
    public VerificationResult verify(KubectlWrapper kubectl, Manifests.ManifestObject object) {
      Preconditions.checkArgument(object.getName().isPresent());
      String name = object.getName().get();
      LOGGER.info(String.format("Verifying deployment, %s", name));
      StringBuilder log = new StringBuilder();
      Object json = null;

      try {
        json = kubectl.getObject(object.getKind().toLowerCase(), name);
      } catch (Exception e) {
        return errorResult(e, object);
      }

      Integer minReplicas = JsonPath.read(json, MINIMUM_REPLICAS_JSONPATH);
      Map<String, Object> status = JsonPath.read(json, STATUS_JSONPATH);
      Integer availableReplicas = (Integer) status.getOrDefault(AVAILABLE_REPLICAS, 0);
      boolean verified =
          minReplicas != null
              && availableReplicas != null
              && minReplicas.intValue() <= availableReplicas.intValue();

      log.append("AvailableReplicas = ")
          .append(availableReplicas)
          .append(",")
          .append(" MinimumReplicas = ")
          .append(minReplicas)
          .append("\n");

      return new VerificationResult(log.toString(), verified, object);
    }
  }

  /**
   * Gets the default verifier for an object kind.
   *
   * @param kind The object Kind for the verifier.
   * @return An API verison agnostic Verifier for verifying the object of the given kind.
   */
  private static Verifier getKindVerifier(String kind) {
    return verifiers.get("*/" + kind.toLowerCase());
  }

  /**
   * Gets the verifier for the API object.
   *
   * @param apiVersion The Kubernetes API version.
   * @param kind The Kubernetes object kind.
   * @return Verifier object for the given apiVersion and Kubernetes object kind.
   */
  private static Verifier getVerifier(String apiVersion, String kind) {
    Verifier verifier = verifiers.get(apiVersion + "/" + kind);
    if (verifier == null) {
      verifier = getKindVerifier(kind);
      if (verifier == null) {
        verifier = defaultVerifier;
      }
    }
    return verifier;
  }

  /**
   * Verify that the Kubernetes object was successfully applied to the Kubernetes cluster.
   *
   * @param kubectl The {@link KubectlWrapper} that will query the cluster.
   * @param object The {@link Manifests.ManifestObject} representation of the Kubernetes object to
   *     verify.
   * @return {@link VerificationResult} that encapsulates whether the Kubernetes object was verified
   *     together with relevant log that is dependent on the type of Kubernetes object.
   */
  public static VerificationResult verify(KubectlWrapper kubectl, Manifests.ManifestObject object) {
    Preconditions.checkNotNull(object);
    return getVerifier(object.getApiVersion(), object.getKind()).verify(kubectl, object);
  }

  /* Convenience create a failed result with stacktrace of a throwable. */
  private static VerificationResult errorResult(Throwable t, Manifests.ManifestObject object) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    pw.flush();
    return new VerificationResult(sw.toString(), false, object);
  }
}
/*
  public static class ServiceVerifier implements Verifier {

    public VerificationResult verify(Kube kube, Manifests.ManifestObject object) {
      String serviceName = object.getName();
      return verify(kube, object.getName());
    }

    public VerificationResult verify(Kube kube, String serviceName) {

      StringBuilder log = new StringBuilder(0);
      Object serviceJson = null;
      Object endpointJson = null;
      List<Object> matchingPodsJson = null;
      try {
        serviceJson = kube.query(Kube.get("service", serviceName));
        endpointJson = kube.query(Kube.get("endpoints", serviceName));
      } catch (Exception e) {
        return errorResult(e);
      }
      Map<String, String> labelsMap = new HashMap<String, String>();
      Map<String, Object> serviceSelectorMap = JsonPath.read(serviceJson, "spec.selectors");
      serviceSelectorMap.forEach((k, v) -> labelsMap.put(k, v.toString()));

      try {
        matchingPodsJson = kube.query(Kube.withLabels("pods", labelsMap));
      } catch (Exception e) {
        return errorResult(e);
      }

      int requiredEndpoints = matchingPodsJson.size();
      // Note that the subsets field is an array, but it's not clear that there would ever be
      // multiple subsets
      List<Map<String, Object>> addresses = JsonPath.read(endpointJson, "subsets.addresses");
      Set<String> podNames =
          addresses.stream().map((m) -> m.get("podName").toString()).collect(Collectors.toSet());
      boolean endpointsForAllMatchingPods =
          matchingPodsJson
              .stream()
              .map(
                  o -> {
                    String podName = JsonPath.read(o, "metadata.name");
                    return podNames.contains(podName);
                  })
              .reduce(true, (acc, contained) -> acc && contained);

      log.append("Endpoints status for service %s, configured addresses : \n");
      log.append(Configuration.defaultConfiguration().jsonProvider().toJson(addresses));
      log.append("\n");
      return new VerificationResult(log.toString(), endpointsForAllMatchingPods);
    }
  }

  public static class IngressVerifier implements Verifier {

    public VerificationResult verify(Kube kube, Manifests.ManifestObject object) {
      Object ingJson = null;
      try {
        ingJson = kube.query(Kube.get(object.getKind(), object.getName()));
      } catch (Exception e) {
        return errorResult(e);
      }

      List<Object> rules = JsonPath.read(ingJson, "rules");
      List<String> backendServicesForIngress = new ArrayList<String>();
      rules.forEach(
          (r) -> {
            backendServicesForIngress.addAll(getBackendServicesForRule(r));
          });

      ServiceVerifier serviceVerifier = new ServiceVerifier();
      List<VerificationResult> serviceVerificationResults =
          backendServicesForIngress
              .stream()
              .map((serviceName) -> serviceVerifier.verify(kube, serviceName))
              .collect(Collectors.toList());

      StringBuilder log = new StringBuilder();
      serviceVerificationResults.forEach((result) -> log.append(result.toString()));

      boolean resultingStatus =
          serviceVerificationResults
              .stream()
              .map((result) -> result.status)
              .reduce(true, (acc, status) -> acc && status);

      return new VerificationResult(log.toString(), resultingStatus);
    }

    private List<String> getBackendServicesForRule(Object rule) {
      List<Object> httpbackendServices = new ArrayList<Object>();
      List<Object> httpsBackendServices = new ArrayList<Object>();
      Object http = JsonPath.read(rule, "http");
      Object https = JsonPath.read(rule, "https");
      List<String> httpServices = backendSerivcesForHttp(http);
      List<String> httpsServices = backendSerivcesForHttp(https);
      httpServices.addAll(httpsServices);
      return httpServices;
    }

    private List<String> backendSerivcesForHttp(Object http) {
      List<String> services = new ArrayList<String>(0);
      if (http != null) {
        List<Object> paths = (List<Object>) JsonPath.read(http, "paths");
        services =
            paths
                .stream()
                .map((Object o) -> (String) JsonPath.read(o, "backend.service"))
                .collect(Collectors.toList());
      }
      return services;
    }
  }

  public static void registerVerifer(Verifier verifier, String apiVersion, String kind) {
    verifiers.put(apiVersion + "/" + kind, verifier);
  }
 */