# AKS Node Scaling Commands

## Scale a Node Pool to 2 Nodes

```bash
# Using nodepool subcommand (explicit)
az aks nodepool scale \
  --resource-group <AZURE_RESOURCE_GROUP> \
  --cluster-name <CLUSTER_NAME> \
  --name nodepool1 \
  --node-count 2
```

```bash
# Using top-level shorthand
az aks scale \
  --resource-group <RESOURCE_GROUP> \
  --name <CLUSTER_NAME> \
  --node-count 2 \
  --nodepool-name nodepool1
```

## Verify Scale Operation

```bash
# Check node count via az cli
az aks nodepool show \
  --resource-group <RESOURCE_GROUP> \
  --cluster-name <CLUSTER_NAME> \
  --name nodepool1 \
  --query "count"

# Check node status via kubectl
kubectl get nodes -o wide

# Watch nodes become Ready
kubectl get nodes -w
```

## Disable Cluster Autoscaler (if enabled)

```bash
# Autoscaler will revert manual scaling — disable it first if needed
az aks nodepool update \
  --resource-group <RESOURCE_GROUP> \
  --cluster-name <CLUSTER_NAME> \
  --name nodepool1 \
  --disable-cluster-autoscaler
```

## Notes

- Scale operations typically take **2–5 minutes**
- System node pool minimum is **1 node** (3 for HA)
- Scaling down evicts pods — ensure PodDisruptionBudgets (PDBs) allow it
- If Cluster Autoscaler is enabled, it will override manual scaling
