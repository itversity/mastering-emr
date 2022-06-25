# Listing active clusters
aws emr list-clusters --active

# Describe Cluster
aws emr describe-cluster \
    --cluster-id j-100ZC8QX517UU

# Terminate Cluster
aws emr terminate-cluster \
    --cluster-id j-100ZC8QX517UU