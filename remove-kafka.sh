#!/usr/bin/env bash
# Remove what deploy-kafka.sh installed, and optionally its Veeam Kasten restore points.
set -euo pipefail

VERSION="1.0.0"

echo "remove-kafka.sh v${VERSION}"

read -rp "Namespace [kafka]: " NAMESPACE
NAMESPACE=${NAMESPACE:-kafka}
read -rp "Helm release name [kafka]: " RELEASE
RELEASE=${RELEASE:-kafka}

helm uninstall "$RELEASE" -n "$NAMESPACE" --ignore-not-found
# Deleting the namespace also removes the StatefulSet PVCs, which helm uninstall keeps
kubectl delete ns "$NAMESPACE" --ignore-not-found

read -rp "Also delete the Veeam Kasten restore points of namespace ${NAMESPACE}? [y/N]: " DEL_RP
if [[ "$DEL_RP" =~ ^[Yy]$ ]]; then
  # --wait=false: the Veeam Kasten aggregated API does not support watch
  kubectl delete restorepointcontent -l "k10.kasten.io/appNamespace=${NAMESPACE}" --wait=false
fi
