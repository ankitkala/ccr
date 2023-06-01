/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.seqno

import org.apache.logging.log4j.LogManager
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.index.Index
import org.opensearch.index.seqno.RetentionLeaseActions
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException
import org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException
import org.opensearch.index.seqno.RetentionLeaseNotFoundException
import org.opensearch.index.shard.ShardId
import org.opensearch.replication.repository.RemoteClusterRepository

class RemoteClusterRetentionLeaseHelper constructor(val followerClusterName: String, val followerClusterUUID: String,
                                                    val leaderClient: Client) {
    companion object {
        private val log = LogManager.getLogger(RemoteClusterRetentionLeaseHelper::class.java)
        const val RETENTION_LEASE_PREFIX = "replication:"
        fun retentionLeaseSource(retentionLeaseID: String): String
        = "${RETENTION_LEASE_PREFIX}${retentionLeaseID}"

        fun getFollowerClusterNameWithUUID(followerClusterName: String, followerClusterUUID: String): String{
            return "$followerClusterName:$followerClusterUUID"
        }
    }

    private fun newRetentionLeaseId(followerShardId: ShardId): String {
        return "${retentionLeaseSource(getFollowerClusterNameWithUUID(followerClusterName, followerClusterUUID))}:${followerShardId}"
    }

    private fun oldRetentionLeaseId(followerShardId: ShardId): String {
        return "${retentionLeaseSource(followerClusterName)}:${followerShardId}"
    }


    /**
     * Add a retention lease to the leaderShard.
     */
    private fun addRetentionLease(leaderShardId: ShardId, retentionLeaseId: String, seqNo: Long, timeout: Long) {
        val request = RetentionLeaseActions.AddRequest(leaderShardId, retentionLeaseId, seqNo,
                retentionLeaseSource(retentionLeaseId))
        log.info("Adding the retention lease shardID - $leaderShardId, id - $retentionLeaseId, seqNo - $seqNo")
        leaderClient.execute(RetentionLeaseActions.Add.INSTANCE, request).actionGet(timeout)
    }

    /**
     * Remove the retention lease
     */
    private fun removeRetentionLease(leaderShardId: ShardId, retentionLeaseId: String) {
        val request = RetentionLeaseActions.RemoveRequest(leaderShardId, retentionLeaseId)
        log.info("Removing retention lease with shardID - $leaderShardId, id - $retentionLeaseId")
        leaderClient.execute(RetentionLeaseActions.Remove.INSTANCE, request).actionGet()
    }

    /**
     * Renew the retention lease.
     */
    private fun renewRetentionLease(remoteClient: Client, leaderShardId: ShardId, retentionLeaseId: String,
                                   seqNo: Long) {
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId, seqNo, retentionLeaseSource(retentionLeaseId))
        remoteClient.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet()
    }

    /**
     * Check if the given retention lease exists.
     */
    private fun checkRetentionLeaseExists(leaderShardId: ShardId, retentionLeaseId: String): Boolean {
        val request = RetentionLeaseActions.RenewRequest(leaderShardId, retentionLeaseId,
                RetentionLeaseActions.RETAIN_ALL, retentionLeaseSource(retentionLeaseId))
        try {
            leaderClient.execute(RetentionLeaseActions.Renew.INSTANCE, request).actionGet()
        } catch (e : RetentionLeaseInvalidRetainingSeqNoException) {
            return true
        } catch (e: RetentionLeaseNotFoundException) {
            return false
        } catch (e : Exception) {
            log.error("Error checking if retention lease exists {}", e)
        }
        return false
    }

    /**
     * Swaps the older format retention lease with the new format one.
     */
    private fun addNewRetentionLeaseIfOldExists(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId): Boolean {
        val newRetentionLeaseID = newRetentionLeaseId(followerShardId)
        val oldRetentionLeaseID = oldRetentionLeaseId(followerShardId)
        var status = false
        try {
            // Check if old retention lease exists.
            if (checkRetentionLeaseExists(leaderShardId, oldRetentionLeaseID)) {
                // Add the new lease at desired sequence number.
                addRetentionLease(leaderShardId, newRetentionLeaseID, seqNo,
                        RemoteClusterRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC)
                // new retention lease added successfully
                status = true

                // Remove the old retention lease and the new one has been applied successfully above.
                removeRetentionLease(leaderShardId, oldRetentionLeaseID)
            }
        } catch (e: Exception) {
            log.error("Unable to addNewRetentionLeaseIfOldExists {}", e)
        }
        return status
    }

    /**
     * Add new format retention lease to the leaderShard. If the lease already exists, remove and retry.
     */
    public fun addRetentionLeaseDuringBootstrap(leaderShardId: ShardId, sequenceNumber: Long, followerShardId: ShardId, timeout: Long) {
        val newRetentionLeaseID = newRetentionLeaseId(followerShardId)
        var canRetry = true
        while (true) {
            try {
                addRetentionLease(leaderShardId, newRetentionLeaseID, sequenceNumber, timeout)
                break
            } catch (e: RetentionLeaseAlreadyExistsException) {
                if (canRetry) {
                    canRetry = false
                    removeRetentionLease(leaderShardId, newRetentionLeaseID)
                } else {
                    throw e
                }
            }
        }
    }

    /**
     * If new format lease exists, return true
     * If the old format lease exists, create the new format lease and return true
     */
    fun retentionLeaseExistsForResume(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId): Boolean  {
        if (checkRetentionLeaseExists(leaderShardId, newRetentionLeaseId(followerShardId))) return true
        else return addNewRetentionLeaseIfOldExists(leaderShardId, seqNo, followerShardId)
    }

    /**
     * Attempts to renew the retention lease.
     * If the new format retention lease doesn't exist, we try to add if the old format one exists. This is done to
     * ensure smooth migration from older format to newer.
     */
    fun renewRetentionLease(leaderShardId: ShardId, seqNo: Long, followerShardId: ShardId) {
        val newRetentionLeaseID = newRetentionLeaseId(followerShardId)
        val oldRetentionLeaseID = oldRetentionLeaseId(followerShardId)
        try {
            renewRetentionLease(leaderClient, leaderShardId, newRetentionLeaseID, seqNo)
        } catch (e: RetentionLeaseNotFoundException) {
            log.info("Retention lease with ID: ${newRetentionLeaseID} not found, " +
                    "checking for old retention lease with ID: $oldRetentionLeaseID")
            if(!addNewRetentionLeaseIfOldExists(leaderShardId, seqNo, followerShardId)){
                log.info("Both new $newRetentionLeaseID and old $oldRetentionLeaseID retention lease not found.")
                throw e
            }
        }
    }

    /**
     * Tries to remove the retention lease on best effort basis.
     */
    fun attemptRetentionLeaseRemoval(leaderShardId: ShardId, followerShardId: ShardId) {
        val retentionLeaseId = newRetentionLeaseId(followerShardId)
        try {
            removeRetentionLease(leaderShardId, retentionLeaseId)
        } catch (e: Exception) {
            log.error("Exception in removing retention lease $retentionLeaseId", e)
        }
    }

    /**
     * Remove retention lease(best effort) for all shards of an index.
     */
    public suspend fun removeRetentionLeaseForIndex(clusterService: ClusterService, leaderIndex: Index, followerIndexName: String) {
        try {
            val shards = clusterService.state().routingTable.indicesRouting().get(followerIndexName)?.shards()
            shards?.forEach {
                val followerShardId = it.value.shardId
                log.debug("Removing lease for $followerShardId.id ")
                attemptRetentionLeaseRemoval(ShardId(leaderIndex, followerShardId.id), followerShardId)
            }
        } catch (e: Exception) {
            log.error("Exception while trying to remove Retention Lease ", e )
        }
    }
}
