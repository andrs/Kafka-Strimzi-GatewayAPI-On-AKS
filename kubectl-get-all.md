NAMESPACE              NAME                                                        READY   STATUS    RESTARTS   AGE
cert-manager           pod/cert-manager-76d9876496-xqcx4                           1/1     Running   0          36m
cert-manager           pod/cert-manager-cainjector-9f76b67db-55vkn                 1/1     Running   0          36m
cert-manager           pod/cert-manager-webhook-7699796787-j56dv                   1/1     Running   0          36m
envoy-gateway-system   pod/envoy-gateway-7ff64c9f74-jqjqr                          1/1     Running   0          38m
envoy-gateway-system   pod/envoy-strimzi-kafka-gateway-d398360c-568964cccb-c6kf2   2/2     Running   0          27m
envoy-gateway-system   pod/envoy-strimzi-kafka-gateway-d398360c-568964cccb-fnbh8   2/2     Running   0          27m
kube-system            pod/azure-cns-bv4rj                                         2/2     Running   0          48m
kube-system            pod/azure-cns-jt5xv                                         2/2     Running   0          62m
kube-system            pod/azure-ip-masq-agent-cmmd4                               1/1     Running   0          62m
kube-system            pod/azure-ip-masq-agent-w5rbk                               1/1     Running   0          48m
kube-system            pod/cloud-node-manager-2xwgm                                1/1     Running   0          48m
kube-system            pod/cloud-node-manager-z75ht                                1/1     Running   0          62m
kube-system            pod/coredns-7d4699998d-m97zb                                1/1     Running   0          62m
kube-system            pod/coredns-7d4699998d-pzmnw                                1/1     Running   0          61m
kube-system            pod/coredns-autoscaler-bcc8bd9f-9xzmd                       1/1     Running   0          62m
kube-system            pod/csi-azuredisk-node-d8kjt                                3/3     Running   0          48m
kube-system            pod/csi-azuredisk-node-lkmqz                                3/3     Running   0          62m
kube-system            pod/csi-azurefile-node-dgbh9                                4/4     Running   0          62m
kube-system            pod/csi-azurefile-node-zq9b4                                4/4     Running   0          48m
kube-system            pod/konnectivity-agent-6d8bc54ccd-chtt6                     1/1     Running   0          39m
kube-system            pod/konnectivity-agent-6d8bc54ccd-xmzr5                     1/1     Running   0          39m
kube-system            pod/konnectivity-agent-autoscaler-6f75c49d4f-z5d28          1/1     Running   0          62m
kube-system            pod/kube-proxy-69h8w                                        1/1     Running   0          62m
kube-system            pod/kube-proxy-w9wvz                                        1/1     Running   0          48m
kube-system            pod/metrics-server-c7f8fcc7c-6vk85                          2/2     Running   0          61m
kube-system            pod/metrics-server-c7f8fcc7c-d5sd6                          2/2     Running   0          61m
strimzi                pod/gateway-api-kafka-broker-10                             1/1     Running   0          26m
strimzi                pod/gateway-api-kafka-broker-11                             1/1     Running   0          26m
strimzi                pod/gateway-api-kafka-broker-12                             1/1     Running   0          26m
strimzi                pod/gateway-api-kafka-controller-0                          1/1     Running   0          26m
strimzi                pod/gateway-api-kafka-entity-operator-6f6f7f8bb7-jfct8      2/2     Running   0          16m
strimzi                pod/strimzi-cluster-operator-5f8595494d-bjbtf               1/1     Running   0          28m

NAMESPACE              NAME                                                TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)                                   AGE
cert-manager           service/cert-manager                                ClusterIP      10.0.75.28     <none>          9402/TCP                                  37m
cert-manager           service/cert-manager-cainjector                     ClusterIP      10.0.168.101   <none>          9402/TCP                                  37m
cert-manager           service/cert-manager-webhook                        ClusterIP      10.0.60.28     <none>          443/TCP,9402/TCP                          37m
default                service/kubernetes                                  ClusterIP      10.0.0.1       <none>          443/TCP                                   64m
envoy-gateway-system   service/envoy-gateway                               ClusterIP      10.0.210.176   <none>          18000/TCP,18001/TCP,18002/TCP,19001/TCP   38m
envoy-gateway-system   service/envoy-strimzi-kafka-gateway-d398360c        LoadBalancer   10.0.201.83    85.210.67.170   9092:32583/TCP                            27m
kube-system            service/kube-dns                                    ClusterIP      10.0.0.10      <none>          53/UDP,53/TCP                             63m
kube-system            service/metrics-server                              ClusterIP      10.0.16.225    <none>          443/TCP                                   62m
strimzi                service/gateway-api-kafka-broker-gateway-10         ClusterIP      10.0.213.132   <none>          9092/TCP                                  26m
strimzi                service/gateway-api-kafka-broker-gateway-11         ClusterIP      10.0.54.94     <none>          9092/TCP                                  26m
strimzi                service/gateway-api-kafka-broker-gateway-12         ClusterIP      10.0.2.189     <none>          9092/TCP                                  26m
strimzi                service/gateway-api-kafka-kafka-bootstrap           ClusterIP      10.0.44.127    <none>          9091/TCP                                  26m
strimzi                service/gateway-api-kafka-kafka-brokers             ClusterIP      None           <none>          9090/TCP,9091/TCP,8443/TCP                26m
strimzi                service/gateway-api-kafka-kafka-gateway-bootstrap   ClusterIP      10.0.85.44     <none>          9092/TCP                                  26m

NAMESPACE     NAME                                            DESIRED   CURRENT   READY   UP-TO-DATE   AVAILABLE   NODE SELECTOR   AGE
kube-system   daemonset.apps/azure-cns                        2         2         2       2            2           <none>          63m
kube-system   daemonset.apps/azure-cns-win                    0         0         0       0            0           <none>          63m
kube-system   daemonset.apps/azure-ip-masq-agent              2         2         2       2            2           <none>          63m
kube-system   daemonset.apps/cloud-node-manager               2         2         2       2            2           <none>          62m
kube-system   daemonset.apps/cloud-node-manager-windows       0         0         0       0            0           <none>          62m
kube-system   daemonset.apps/csi-azuredisk-node               2         2         2       2            2           <none>          62m
kube-system   daemonset.apps/csi-azuredisk-node-win           0         0         0       0            0           <none>          62m
kube-system   daemonset.apps/csi-azurefile-node               2         2         2       2            2           <none>          62m
kube-system   daemonset.apps/csi-azurefile-node-win           0         0         0       0            0           <none>          62m
kube-system   daemonset.apps/kube-proxy                       2         2         2       2            2           <none>          62m
kube-system   daemonset.apps/windows-kube-proxy-initializer   0         0         0       0            0           <none>          62m

NAMESPACE              NAME                                                   READY   UP-TO-DATE   AVAILABLE   AGE
cert-manager           deployment.apps/cert-manager                           1/1     1            1           36m
cert-manager           deployment.apps/cert-manager-cainjector                1/1     1            1           36m
cert-manager           deployment.apps/cert-manager-webhook                   1/1     1            1           36m
envoy-gateway-system   deployment.apps/envoy-gateway                          1/1     1            1           38m
envoy-gateway-system   deployment.apps/envoy-strimzi-kafka-gateway-d398360c   2/2     2            2           27m
kube-system            deployment.apps/coredns                                2/2     2            2           63m
kube-system            deployment.apps/coredns-autoscaler                     1/1     1            1           63m
kube-system            deployment.apps/konnectivity-agent                     2/2     2            2           62m
kube-system            deployment.apps/konnectivity-agent-autoscaler          1/1     1            1           62m
kube-system            deployment.apps/metrics-server                         2/2     2            2           62m
strimzi                deployment.apps/gateway-api-kafka-entity-operator      1/1     1            1           24m
strimzi                deployment.apps/strimzi-cluster-operator               1/1     1            1           28m

NAMESPACE              NAME                                                              DESIRED   CURRENT   READY   AGE
cert-manager           replicaset.apps/cert-manager-76d9876496                           1         1         1       36m
cert-manager           replicaset.apps/cert-manager-cainjector-9f76b67db                 1         1         1       36m
cert-manager           replicaset.apps/cert-manager-webhook-7699796787                   1         1         1       36m
envoy-gateway-system   replicaset.apps/envoy-gateway-7ff64c9f74                          1         1         1       38m
envoy-gateway-system   replicaset.apps/envoy-strimzi-kafka-gateway-d398360c-568964cccb   2         2         2       27m
kube-system            replicaset.apps/coredns-7d4699998d                                2         2         2       62m
kube-system            replicaset.apps/coredns-autoscaler-bcc8bd9f                       1         1         1       62m
kube-system            replicaset.apps/konnectivity-agent-67cb9b4df5                     0         0         0       62m
kube-system            replicaset.apps/konnectivity-agent-6d8bc54ccd                     2         2         2       39m
kube-system            replicaset.apps/konnectivity-agent-autoscaler-6f75c49d4f          1         1         1       62m
kube-system            replicaset.apps/metrics-server-6769b5cc57                         0         0         0       62m
kube-system            replicaset.apps/metrics-server-c7f8fcc7c                          2         2         2       61m
strimzi                replicaset.apps/gateway-api-kafka-entity-operator-6f6f7f8bb7      1         1         1       24m
strimzi                replicaset.apps/strimzi-cluster-operator-5f8595494d               1         1         1       28m
