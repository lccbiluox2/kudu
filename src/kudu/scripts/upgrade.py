#!/usr/bin/python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import argparse
import re
import time

from cm_api.api_client import ApiResource

def parse_args():
    parser = argparse.ArgumentParser(description="Uses Cloudera Manager to upgrade the specified "
                                                 "parcel to the newest compatible revision. "
                                                 "Will not upgrade to a new version of the "
                                                 "service, i.e. the release version will be the "
                                                 "same on the new parcel. After a new parcel is "
                                                 "successfully activated, the affected service is "
                                                 "restarted.")
    parser.add_argument("--host", type=str, default="localhost",
                        help="Hostname of the Cloudera Manager server. Default is localhost.")
    parser.add_argument("--port", type=int, default=7180,
                        help="Port of the Cloudera Manager server. Default is 7180, the default "
                        "Cloudera Manager port.")
    parser.add_argument("--user", type=str, default="admin",
                        help="Username with which to log into Cloudera Manager. Default is "
                        "'admin'.")
    parser.add_argument("--password", type=str, default="admin",
                        help="Password with which to log into Cloudera Manager. Default is "
                        "'admin'.")
    parser.add_argument("--parcel-name", type=str, default="CDH",
                       help="Parcel Name to upgrade.")
    parser.add_argument("--cluster", type=str,
                        help="Name of an existing cluster on which the service should be upgraded. "
                        "If not specified, uses the only cluster available or raises an exception "
                        "if multiple or no clusters are found.")
    parser.add_argument("--service-name", type=str,
                        help="Name of the service to be restarted after the parcel is upgraded. If "
                        "none specified, restarts all stale services on the cluster.")
    parser.add_argument("--max-time-per-stage", type=int, default=120,
                        help="Maximum amount of time in seconds allotted to waiting for any single "
                        "stage of parcel distribution (i.e. downloading, distributing, "
                        "activating) or removal. Default is two minutes.")
    parser.add_argument("--clear-after-success", action="store_true", default=False,
                        help="Flag indicating whether Cloudera Manager should remove unused Kudu "
                        "parcels after a successful upgrade. A parcel is deemed unused if its "
                        "status is not ACTIVATED (i.e. DISTRIBUTED or DOWNLOADED) after the "
                        "upgrade process is completed.")
    parser.add_argument("--dry-run", action="store_true", default=False,
                        help="Flag indicating that the script won't actually upgrade the cluster. "
                        "Useful to see what changes would be made.")
    return parser.parse_args()

def get_best_upgrade_candidate_parcel(cluster, parcel_name):
    # A parcel is an upgrade candidate if 1) it has the same release version as the currently active
    # parcel, and 2) it has a greater build number than the currently active parcel. The best
    # candidate will be the one with the greatest build number.
    activated_parcels = []
    candidate_parcels = []
    for parcel in cluster.get_all_parcels():
        if parcel.product == parcel_name:
            if parcel.stage == "ACTIVATED":
                activated_parcels.append(parcel)
            else:
                candidate_parcels.append(parcel)

    def get_build_number(parcel):
        # The following regexp matches the build number at the end of a full parcel version string.
        # The result is cast to an integer to facilitate build comparison.
        # E.g. "1.4.0-1.cdh5.12.0.p0.814" will return int(814)
        full_version = parcel.version
        match = re.match(".*p\d+\.(\d+)$", full_version)
        if match is None:
            raise Exception("Could not get the build number from %s." % full_version)
        return int(match.group(1))

    def release_versions_match(parcel1, parcel2):
        def get_release_version(full_version):
            # The following regexp matches the major, minor, and patch version numbers at the
            # beginning of the full parcel version string.
            # E.g. "1.4.0-1.cdh5.12.0.p0.814" will return "1.4.0"
            match = re.match("(\d+\.\d+\.\d+).*", full_version)
            if match is None:
                raise Exception("Could not get the release version from %s." % full_version)
            return match.group(1)
        return get_release_version(parcel1.version) == get_release_version(parcel2.version)

    if len(activated_parcels) > 0:
        greatest_activated = max(activated_parcels, key=get_build_number)

        # Filter out parcels that have different release versions or are downgrades.
        candidate_parcels = [parcel for parcel in candidate_parcels
                             if release_versions_match(parcel, greatest_activated) and
                                get_build_number(parcel) > get_build_number(greatest_activated)]
        if len(candidate_parcels) > 0:
            greatest_candidate = max(candidate_parcels, key=lambda p: p.version)
            print("Chose the new parcel %s-%s (Stage: %s)." % (greatest_candidate.product,
                                                               greatest_candidate.version,
                                                               greatest_candidate.stage))
            return greatest_candidate
        else:
            print("No upgrade candidates available for parcel version %s-%s." %
                  (greatest_activated.product, greatest_activated.version))
            return None
    raise Exception("No activated %s parcels found. Activate one first and then upgrade."
                    % parcel_name)

def wait_for_parcel_stage(cluster, parcel, stage, max_time):
    for attempt in xrange(1, max_time + 1):
        target_parcel = cluster.get_parcel(parcel.product, parcel.version)
        if target_parcel.stage == stage:
            return
        if target_parcel.state.errors:
            raise Exception("Fetching parcel resulted in error %s" %
                            str(target_parcel.state.errors))
        print("progress: %s / %s" % (target_parcel.state.progress,
              target_parcel.state.totalProgress))
        time.sleep(1)
    else:
        raise Exception("Parcel %s-%s did not reach stage %s in %d seconds." %
                        (parcel.product, parcel.version, stage, max_time))

def ensure_parcel_activated(cluster, parcel, max_time_per_stage):
    if dry_run:
        print("Running a dry-run. Not activating parcel %s-%s" % (parcel.product, parcel.version))
        return
    parcel_stage = parcel.stage
    if parcel_stage == "AVAILABLE_REMOTELY":
        print("Downloading parcel: %s-%s" % (parcel.product, parcel.version))
        parcel.start_download()
        wait_for_parcel_stage(cluster, parcel, "DOWNLOADED", max_time_per_stage)
        print("Downloaded parcel: %s-%s " % (parcel.product, parcel.version))
        parcel_stage = "DOWNLOADED"
    if parcel_stage == "DOWNLOADED":
        print("Distributing parcel: %s-%s " % (parcel.product, parcel.version))
        parcel.start_distribution()
        wait_for_parcel_stage(cluster, parcel, "DISTRIBUTED", max_time_per_stage)
        print("Distributed parcel: %s-%s " % (parcel.product, parcel.version))
        parcel_stage = "DISTRIBUTED"
    if parcel_stage == "DISTRIBUTED":
        print("Activating parcel: %s-%s " % (parcel.product, parcel.version))
        parcel.activate()
        wait_for_parcel_stage(cluster, parcel, "ACTIVATED", max_time_per_stage)
        print("Activated parcel: %s-%s " % (parcel.product, parcel.version))

def find_cluster(api, cluster_name):
    if cluster_name:
        return api.get_cluster(cluster_name)
    all_clusters = api.get_all_clusters()
    if len(all_clusters) == 0:
        raise Exception("No clusters found; create one before calling this script.")
    if len(all_clusters) > 1:
        raise Exception("More than one cluster found; specify which cluster to use with --cluster.")
    cluster = all_clusters[0]
    print("Found cluster: %s" % cluster.displayName)
    return cluster

def find_service(cluster, service_name):
    all_services = [s for s in cluster.get_all_services() if s.displayName == service_name]
    if len(all_services) == 0:
        raise Exception("No services named %s found on %s." % (service_name, cluster.displayName))
    if not len(all_services) == 1:
        raise Exception("Input service name does not uniquely identify a service.")
    service = all_services[0]
    print("Found service: %s" % service.displayName)
    return service

def ensure_parcel_removed(cluster, parcel, max_time_per_stage):
    parcel_stage = parcel.stage
    if parcel_stage == "DISTRIBUTED":
        print("Removing parcel distribution: %s-%s" % (parcel.product, parcel.version))
        parcel.start_removal_of_distribution()
        wait_for_parcel_stage(cluster, parcel, "DOWNLOADED", max_time_per_stage)
        parcel_stage = "DOWNLOADED"
        print("Removed parcel distribution: %s-%s" % (parcel.product, parcel.version))
    if parcel_stage == "DOWNLOADED":
        # Don't wait for AVAILABLE_REMOTELY, as the parcel may no longer exist in the repo.
        # If this is the case, CM will not be able to find the parcel to verify its stage.
        print("Removing parcel download: %s-%s" % (parcel.product, parcel.version))
        parcel.remove_download()

def main():
    args = parse_args()
    print("Connecting to %s:%d..." % (args.host, args.port))
    api = ApiResource(args.host,
                      args.port,
                      username=args.user,
                      password=args.password,
                      version=10)
    cluster = find_cluster(api, args.cluster)

    global dry_run
    dry_run = args.dry_run

    # Get the parcels available to this cluster. Get the newest one that is not activated, ensuring
    # that it has a greater build number than that ACTIVATED and that it distributes the same
    # release version of Kudu.
    parcel = get_best_upgrade_candidate_parcel(cluster, args.parcel_name)
    if parcel is None:
        print("Cannot upgrade %s parcel. Exiting early." % args.parcel_name)
        return

    # Start up the upgrade process and activate the new parcel.
    ensure_parcel_activated(cluster, parcel, args.max_time_per_stage)

    # Restart the services synchronously, ensuring they come to a stop before continuing. This
    # ensures that the parcels will not be in-use when they're removed.
    if args.service_name:
        service = find_service(cluster, args.service_name)
        to_restart = "service %s" % args.service_name
        run_restart = lambda: service.restart().wait()
    else:
        to_restart = "stale services"
        run_restart = lambda: cluster.restart(restart_only_stale_services=True).wait()
    if dry_run:
        print("Running a dry-run. Not restarting %s..." % to_restart)
    else:
        print("Restarting %s..." % to_restart)
        run_restart()

    # Now that the services have been restarted, and older, existing parcels are not being used,
    # clear the unused parcels if needed.
    if args.clear_after_success:
        inactive_parcels = [parcel for parcel in cluster.get_all_parcels()
            if parcel.product == args.parcel_name and not parcel.stage == "ACTIVATED"]
        if dry_run:
            for parcel in inactive_parcels:
                print("Runnning a dry-run. Parcel %s-%s is not activated..."
                    % (parcel.product, parcel.version))
        else:
            for parcel in inactive_parcels:
                ensure_parcel_removed(cluster, parcel, args.max_time_per_stage)

if __name__ == "__main__":
    main()
