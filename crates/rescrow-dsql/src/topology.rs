use crate::{Config, io, wire::corrupt};
use aws_config::{BehaviorVersion, Region};
use aws_sdk_dsql::{operation::get_cluster::GetClusterOutput, types::ClusterStatus};
use rescrow_application::{StoreError, capabilities::*};
use std::collections::BTreeSet;

pub(crate) struct Cluster {
    pub arn: String,
    pub region: String,
    pub account: String,
    pub id: String,
}
impl Cluster {
    pub fn parse(arn: &str) -> Result<Self, StoreError> {
        let parts: Vec<_> = arn.split(':').collect();
        if parts.len() != 6
            || parts[0] != "arn"
            || parts[1] != "aws"
            || parts[2] != "dsql"
            || parts[3].is_empty()
            || parts[3].len() > 20
            || !parts[3]
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || parts[4].len() != 12
            || !parts[4].bytes().all(|b| b.is_ascii_digit())
        {
            return Err(corrupt("expected an AWS DSQL cluster ARN"));
        }
        let id = parts[5]
            .strip_prefix("cluster/")
            .ok_or_else(|| corrupt("invalid cluster ARN resource"))?;
        if id.len() != 26
            || !id
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        {
            return Err(corrupt("invalid DSQL cluster identifier"));
        }
        Ok(Self {
            arn: arn.into(),
            region: parts[3].into(),
            account: parts[4].into(),
            id: id.into(),
        })
    }
    pub fn endpoint(&self) -> String {
        format!("{}.dsql.{}.on.aws", self.id, self.region)
    }
}

async fn describe(
    config: &aws_config::SdkConfig,
    cluster: &Cluster,
) -> Result<GetClusterOutput, StoreError> {
    let sdk = aws_sdk_dsql::config::Builder::from(config)
        .region(Region::new(cluster.region.clone()))
        .build();
    let output = aws_sdk_dsql::Client::from_conf(sdk).get_cluster().identifier(&cluster.id).send().await
        .map_err(|_| io("DSQL GetCluster failed; verify credentials and dsql:GetCluster on both active peers"))?;
    validate_description(&output, cluster)?;
    Ok(output)
}

fn validate_description(output: &GetClusterOutput, cluster: &Cluster) -> Result<(), StoreError> {
    if output.arn() != cluster.arn
        || output.identifier() != cluster.id
        || output.status() != &ClusterStatus::Active
        || output
            .endpoint()
            .is_some_and(|host| host != cluster.endpoint())
    {
        return Err(corrupt(
            "DSQL cluster identity, status, or endpoint does not match",
        ));
    }
    Ok(())
}

/// Verify both peers live, reciprocal membership, account and witness. A
/// requested profile or a hostname alone never establishes MR durability.
pub(crate) async fn inspect(config: &Config) -> Result<Capabilities, StoreError> {
    let cluster = Cluster::parse(&config.cluster_arn)?;
    let mut loader =
        aws_config::defaults(BehaviorVersion::latest()).region(Region::new(cluster.region.clone()));
    if let Some(profile) = &config.profile {
        loader = loader.profile_name(profile);
    }
    let sdk = loader.load().await;
    let local = describe(&sdk, &cluster).await?;
    let (_, peer, _, _) = peer_for(&local)?;
    let remote = describe(&sdk, &peer).await?;
    verify_pair(&local, &remote)
}

fn peer_for(
    local: &GetClusterOutput,
) -> Result<(Cluster, Cluster, String, BTreeSet<String>), StoreError> {
    let cluster = Cluster::parse(local.arn())?;
    validate_description(local, &cluster)?;
    let properties = local
        .multi_region_properties()
        .ok_or_else(|| corrupt("DSQL qualification requires a multi-Region cluster"))?;
    let witness = properties
        .witness_region()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| corrupt("missing DSQL witness"))?;
    let mut members: BTreeSet<_> = properties.clusters().iter().cloned().collect();
    members.insert(cluster.arn.clone());
    if members.len() != 2 {
        return Err(corrupt("DSQL requires exactly two active peers"));
    }
    let peer_arn = members
        .iter()
        .find(|arn| **arn != cluster.arn)
        .ok_or_else(|| corrupt("missing active DSQL peer"))?;
    let peer = Cluster::parse(peer_arn)?;
    if peer.region == cluster.region
        || peer.account != cluster.account
        || witness == cluster.region
        || witness == peer.region
    {
        return Err(corrupt("invalid DSQL active/witness topology"));
    }
    Ok((cluster, peer, witness.into(), members))
}

fn verify_pair(
    local: &GetClusterOutput,
    remote: &GetClusterOutput,
) -> Result<Capabilities, StoreError> {
    let (cluster, peer, witness, members) = peer_for(local)?;
    validate_description(remote, &peer)?;
    let remote_properties = remote
        .multi_region_properties()
        .ok_or_else(|| corrupt("DSQL peer is not multi-Region"))?;
    let mut remote_members: BTreeSet<_> = remote_properties.clusters().iter().cloned().collect();
    remote_members.insert(peer.arn);
    if remote_members != members || remote_properties.witness_region() != Some(witness.as_str()) {
        return Err(corrupt(
            "DSQL peering is not reciprocal or witnesses differ",
        ));
    }
    Ok(Capabilities {
        durability_profile: DurabilityProfile::EnterpriseMr,
        failure_domain: FailureDomain::Region,
        active_regions: BTreeSet::from([cluster.region, peer.region]),
        witness_region: Some(witness),
        max_record_bytes: Some(crate::MAX_COLUMN_BYTES as u64),
        max_participants: Some(crate::MAX_PARTICIPANTS),
        multi_tenant: true,
        cross_process: true,
        reopen_after_crash: true,
        ..Capabilities::simulation()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dsql::{primitives::DateTime, types::MultiRegionProperties};
    const LEFT: &str = "arn:aws:dsql:us-east-1:123456789012:cluster/aaaaaaaaaaaaaaaaaaaaaaaaaa";
    const RIGHT: &str = "arn:aws:dsql:us-west-2:123456789012:cluster/bbbbbbbbbbbbbbbbbbbbbbbbbb";
    fn description(arn: &str, peer: &str, witness: &str) -> GetClusterOutput {
        let cluster = Cluster::parse(arn).unwrap();
        GetClusterOutput::builder()
            .arn(arn)
            .identifier(&cluster.id)
            .endpoint(cluster.endpoint())
            .creation_time(DateTime::from_secs(0))
            .deletion_protection_enabled(true)
            .status(ClusterStatus::Active)
            .multi_region_properties(
                MultiRegionProperties::builder()
                    .clusters(peer)
                    .witness_region(witness)
                    .build(),
            )
            .build()
            .unwrap()
    }
    #[test]
    fn only_reciprocal_active_peers_with_a_distinct_witness_qualify() {
        let left = description(LEFT, RIGHT, "us-east-2");
        let right = description(RIGHT, LEFT, "us-east-2");
        verify_pair(&left, &right)
            .unwrap()
            .require(DurabilityProfile::EnterpriseMr)
            .unwrap();
        let mut single = left.clone();
        single.multi_region_properties = None;
        assert!(verify_pair(&single, &right).is_err());
        assert!(verify_pair(&left, &description(RIGHT, LEFT, "us-west-2")).is_err());
        assert!(verify_pair(&description(LEFT, RIGHT, "us-east-1"), &right).is_err());
        let mut unlinked = right.clone();
        unlinked.multi_region_properties = Some(
            MultiRegionProperties::builder()
                .witness_region("us-east-2")
                .build(),
        );
        assert!(verify_pair(&left, &unlinked).is_err());
        let mut failed = right.clone();
        failed.status = ClusterStatus::Failed;
        assert!(verify_pair(&left, &failed).is_err());
        let mut wrong_endpoint = right.clone();
        wrong_endpoint.endpoint = Some("elsewhere.invalid".into());
        assert!(verify_pair(&left, &wrong_endpoint).is_err());
        assert!(verify_pair(&left, &left).is_err());
    }
}
