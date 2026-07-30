# hcp-nodeport-reflector
A Kubernetes operator designed to seamlessly bridge network traffic from an OpenShift HyperShift (HCP) Management Cluster (Hub) directly to workloads running inside Hosted Clusters.

In a HyperShift architecture, worker nodes and workloads live in a Hosted Cluster, while the control plane lives in the Management (Hub) Cluster. Exposing Hosted Cluster applications usually requires provisioning external cloud load balancers. But what if you want to route traffic centrally through the Hub cluster's Ingress Controller or Gateway?

The **HCP NodePort Reflector** solves this by creating a direct L4 network bridge. 

When you deploy a `NodePortReflector` Custom Resource in the Hub cluster, the operator dynamically connects to the Hosted Cluster, inspects a target `NodePort` service, and automatically provisions a "Shadow Service" and an `EndpointSlice` in the Hub. This allows resources in the Hub (like a central HAProxy Router) to send traffic directly to the worker nodes of the Hosted Cluster.

**Key Features:**
*   **Zero-SNAT IP Preservation:** It intelligently reads the remote service's `externalTrafficPolicy`. If set to `Local`, the operator maps endpoints exclusively to the nodes actively running the pods, preserving the original client IP across cluster boundaries.
*   **Multi-Port Sync:** Automatically discovers and reflects all exposed ports from the remote service.
*   **Dynamic Synchronization:** Continuously monitors the Hosted Cluster to update the Hub's endpoints if worker nodes scale up/down or if the remote service changes.
*   **Ingress Chaining:** Enables advanced multi-tier routing (e.g., exposing a Hosted Cluster's Ingress controller through the Hub's Ingress controller using Passthrough routes).

---

## Architecture & How it Works

1. The Operator runs in the **Hub Cluster**.
2. A user creates a `NodePortReflector` Custom Resource in the Hub.
3. The controller locates the automatically generated `kubeconfig` secret for the specified Hosted Cluster.
4. It connects to the Hosted Cluster and reads the target `NodePort` Service.
5. It creates a **Shadow Service** (type `ClusterIP`) and a corresponding **EndpointSlice** in the Hub, mapping the Hub service directly to the IPs of the Hosted Cluster's worker nodes.

---

## Usage Example

To reflect a service, you need to expose your application as a `NodePort` in the Hosted Cluster, and then create a `NodePortReflector` in the Hub Cluster.

### 1. In your Hosted Cluster
Deploy your application and expose it using a `NodePort` service. To preserve the client's original IP, use `externalTrafficPolicy: Local`.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: my-backend-app
  namespace: default
spec:
  type: NodePort
  externalTrafficPolicy: Local
  selector:
    app: my-app
  ports:
  - name: http
    port: 80
    targetPort: 8080
```

### 2. In your Hub Cluster
Create the Custom Resource. The operator will automatically find the Hosted Cluster, locate `my-backend-app`, and create a shadow service in the Hub.

```yaml
apiVersion: reflector.bizcochillo.io/v1alpha1
kind: NodePortReflector
metadata:
  name: reflect-my-backend
  namespace: default
spec:
  hostedCluster:
    name: my-hosted-cluster-name # The name of the HyperShift HostedCluster
    namespace: clusters          # The namespace where the HostedCluster resides in the Hub
  targetService:
    name: my-backend-app         # The name of the Service inside the Hosted Cluster
    namespace: default           # The namespace of the Service inside the Hosted Cluster
```
Once applied, you can verify the status:

```bash
[user@host ~]$ oc get nodeportreflector reflect-my-backend
NAME                PHASE    POLICY    ACTIVE NODES   AGE
reflect-my-backend  Synced   Cluster   2              89m
```
You can now point any Hub Ingress/Route to the newly created shadow service (`reflect-my-backend`).

### 3. Advanced: Ingress Chaining (Edge-to-Cluster Proxying)
A powerful architectural pattern enabled by this operator is chaining Ingress controllers. You can route wildcard traffic from the Hub directly to an Ingress Controller running inside the Hosted Cluster.

By leveraging `externalTrafficPolicy: Local` on the remote Ingress Service, the client's original IP is preserved across clusters without SNAT. Furthermore, by using an OpenShift `passthrough` Route in the Hub, the Hub router forwards the raw TCP/TLS stream based on SNI (Server Name Indication).

This means: 
- The `Host` header is never rewritten.
- The Hosted Cluster handles TLS termination natively with its own certificates.

**In the Hosted cluster**
We create a service `router-nodeport` of NodePort type which will be monitored by the reflector to create the EndpointSlices. 

```yaml
kind: Service
apiVersion: v1
metadata:
  name: router-nodeport
  namespace: openshift-ingress
spec:
  externalTrafficPolicy: Cluster
  ports:
    - name: http
      protocol: TCP
      port: 80
      targetPort: 80
    - name: https
      protocol: TCP
      port: 443
      targetPort: 443
  internalTrafficPolicy: Cluster
  type: NodePort
  selector:
    ingresscontroller.operator.openshift.io/deployment-ingresscontroller: default
```

**In the Hub cluster**

We enable wildcard routes in the targeted Ingress controller of the Hub cluster 
```bash
oc patch ingresscontroller default -n openshift-ingress-operator \
  --type=merge \
  -p '{"spec": {"routeAdmission": {"wildcardPolicy": "WildcardsAllowed"}}}'
```

We create a NodePortReflector, that is monitoring the just created `NodePort` service named `router-nodeport` in the cluster `my-hosted-cluster-name`
```yaml
apiVersion: reflector.bizcochillo.io/v1alpha1
kind: NodePortReflector
metadata:
  name: router-hosted
  namespace: demo-reflectors
spec:
  hostedCluster:
    name: my-hosted-cluster-name # The name of the HyperShift HostedCluster
    namespace: clusters          # The namespace where the HostedCluster resides in the Hub
  targetService:
    name: router-nodeport
    namespace: openshift-ingress
```

We create routes for HTTP and HTTPS 
```yaml
kind: Route
apiVersion: route.openshift.io/v1
metadata:
  name: router-hosted-http
  namespace: demo-reflectors
spec:
  host: wildcard.apps2.hosted.hypershift.lab
  path: /
  to:
    kind: Service
    name: router-hosted
    weight: 100
  port:
    targetPort: http
  wildcardPolicy: Subdomain
---
kind: Route
apiVersion: route.openshift.io/v1
metadata:
  name: router-hosted-https
  namespace: demo-reflectors
spec:
  host: wildcard.apps2.hosted.hypershift.lab
  to:
    kind: Service
    name: router-hosted
    weight: 100
  port:
    targetPort: https
  tls:
    termination: passthrough
    insecureEdgeTerminationPolicy: None
  wildcardPolicy: Subdomain
```

To use this approach, you must configure a wildcard DNS record (e.g., `*.apps2.hosted.hypershift.lab`) that resolves to the Management (Hub) cluster. Once configured, all HTTP and HTTPS traffic targeting this wildcard domain is received by the Hub's Ingress Controller and transparently forwarded to the default Ingress Controller running inside the Hosted cluster.

## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/hcp-nodeport-reflector:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/hcp-nodeport-reflector:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/hcp-nodeport-reflector:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/hcp-nodeport-reflector/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing
// TODO(user): Add detailed information on how you would like others to contribute to this project

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

