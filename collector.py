import random
import sys
import re
import threading
import time
import traceback
from copy import deepcopy
from logging import getLogger
from threading import Lock

from wekalib.circular import circular_list

from async_api import Async

# initialize logger - configured in main routine
import wekalib
from prometheus_client.core import GaugeMetricFamily

log = getLogger(__name__)

# the WEKA api returns this as a cookie to indicate that there are no more entries
MAX_U64 = 2**64 - 1


class Collector(object):
    def __init__(self, config, cluster_obj):  # wekaCollector

        # dynamic module globals
        self._access_lock = Lock()  # prevents double-scrapes
        self.gather_timestamp = None
        self.collect_time = None
        self.clusterdata = {}
        self.threaderror = False
        self.api_stats = {'num_calls': 0,}
        #self.backends_only = config['exporter']['backends_only']    # calling routine verifies this is there
        self.exceeded_only = config['exporter']['exceeded_only']    # calling routine verifies this is there
        self.max_procs = config['exporter']['max_procs'] if 'max_procs' in config['exporter'] else 2
        self.max_threads_per_proc = config['exporter']['max_threads_per_proc'] if 'max_threads_per_proc' \
                                                                                  in config['exporter'] else 40
        self.enable_name_cache = config['exporter']['enable_name_cache'] if 'enable_name_cache' in config['exporter'] else True
        self.force_name_resolution = config['exporter']['force_name_resolution'] \
                                    if 'force_name_resolution' in config['exporter'] else False
        self.filesystems = config['cluster']['filesystems']

        self.cluster = cluster_obj

        #self.quotas = dict()
        self.quotas_last = dict()

        self.asyncobj = None
        self.quota_objs = list()
        self.quota_map = dict()

        self.hostlist = None
        self.circular_host_list = None

        self.new_api = None
        self.method = None
        self.totalBytes = None
        self.softLimitBytes = None
        self.hardLimitBytes = None
        self.nextCookie = None
        self.snapViewId = None
        self.inodeId = None
        self.inodeContext = None
        self.get_path_integrated = None

        # Background name cache updater... not needed if we're not caching names
        if self.enable_name_cache:
            log.info("enable name cache")
            self.background_thread = threading.Thread(target=self._background_name_updater, daemon=True)
            self.background_thread.start()


    def collect(self):

        log.info("starting collector...")
        with self._access_lock:  # be thread-safe
            log.info("lock acquired...")

            self.api_stats['num_calls'] = 0

            log.info("Entering collect() routine")
            second_pass = False
            should_gather = False

            # get the time once here
            start_time = time.time()

            # first time being called?   force gathering info?
            if self.collect_time is None:
                log.info("never gathered before")
                should_gather = True
            elif start_time - self.collect_time > 5:  # prometheus always calls twice; only gather if it's been a while since last call
                should_gather = True
            else:
                second_pass = True

            if should_gather:
                log.info("gathering")
                try:
                    #for objs in self.gather():
                    #    yield objs
                    self.quota_objs = self.gather()
                except wekalib.exceptions.NameNotResolvable as exc:
                    log.critical(f"Unable to resolve names; terminating")
                    sys.exit(1)
                except Exception as exc:
                    log.critical(f"Error gathering data: {exc}, {traceback.format_exc()}")
                    return  # raise?

            # yield for each metric
            log.debug("Yielding metrics")
            for i in self.quota_objs:
                yield i
            log.debug("Yielding complete")

            # report time if we gathered, otherwise, it's meaningless
            if should_gather:
                self.last_elapsed = time.time() - start_time
            else:
                elapsed = self.last_elapsed

            log.debug("Yielding process metrics")
            yield GaugeMetricFamily('weka_collect_seconds', 'Total Time spent in Prometheus collect',
                                    value=self.last_elapsed)
            yield GaugeMetricFamily('weka_collect_apicalls', 'Total number of api calls',
                                    value=self.api_stats['num_calls'])
            log.debug("Yielding process metrics complete")

            if not second_pass:
                log.info(
                    f"stats returned. total time = {round(self.last_elapsed, 2)}s {self.api_stats['num_calls']} api calls made. {time.asctime()}")
            self.collect_time = time.time()
        log.info("ending collector...")

    def gather(self):

        # reset the cluster config to be sure we can talk to all the hosts
        try:
            self.cluster.refresh()
        except wekalib.exceptions.NameNotResolvable as exc:
            log.critical(f"Names are not resolvable - are they in /etc/hosts or DNS? {exc}")
            raise
        except Exception as exc:
            log.error(f"Cluster refresh failed on cluster '{self.cluster}' - check connectivity ({exc})")
            # log.error(traceback.format_exc())
            return

        # value is the total bytes used
        quota_gauge = GaugeMetricFamily("weka_quota", "Weka Directory Quota Summary",
                                        labels=["cluster",
                                                "filesystem",
                                                "directory",
                                                "owner",
                                                "soft_quotaGB",
                                                "hard_quotaGB"])

        # value is the soft quota
        soft_gauge = GaugeMetricFamily("weka_quota_soft", "Weka Directory Soft Quota",
                                        labels=["cluster",
                                                "filesystem",
                                                "directory",
                                                "owner"])

        # value is the hard quota
        hard_gauge = GaugeMetricFamily("weka_quota_hard", "Weka Directory Hard Quota",
                                        labels=["cluster",
                                                "filesystem",
                                                "directory",
                                                "owner"])

        # value is the used bytes
        used_gauge = GaugeMetricFamily("weka_quota_used", "Weka Directory Quota Used Bytes",
                                        labels=["cluster",
                                                "filesystem",
                                                "directory",
                                                "owner"])

        # value is the used bytes
        remaining_hard_gauge = GaugeMetricFamily("weka_quota_remaining", "Weka Directory Quota Remaining Bytes (hard quota)",
                                        labels=["cluster",
                                                "filesystem",
                                                "directory",
                                                "owner"])

        # we have to ask for quotas for each FS individually, so get a list of filesystems
        filesystems = self.get_filesystems()

        # for each FS, ask for quotas... this may require several calls
        for fs in filesystems:
            self.test_new_api(fs)

            # save the last set of data; get fresh data
            quotas = self.get_quotas(fs)

            log.info(f"{len(quotas)} quotas in fs {fs}")
            self.quotas_last[fs] = deepcopy(dict(quotas))

            dirname_start = time.time()
            for quota, details in quotas.items():

                dirname = details.get('path', 'error')

                owner = details['owner'] if details['owner'] else ""
                soft_limit = str(round(details[self.softLimitBytes]/1000/1000/1000,1)) if details[self.softLimitBytes] else ""
                hard_limit = str(round(details[self.hardLimitBytes]/1000/1000/1000,1)) if details[self.hardLimitBytes] else ""

                quota_gauge.add_metric([str(self.cluster), fs, dirname, owner,
                                       soft_limit, hard_limit],
                                       str(round(details[self.totalBytes]/1000/1000/1000,1)) )
                if (not self.new_api and details[self.softLimitBytes] <= details[self.hardLimitBytes]) or \
                                    self.new_api and details[self.softLimitBytes] is not None:
                    soft_gauge.add_metric([str(self.cluster), fs, dirname, owner],
                                       float(details[self.softLimitBytes]))

                if self.new_api and details[self.hardLimitBytes] is not None:
                    hard_gauge.add_metric([str(self.cluster), fs, dirname, owner],
                                       details[self.hardLimitBytes])
                used_gauge.add_metric([str(self.cluster), fs, dirname, owner],
                                       details[self.totalBytes])
                if self.new_api and details[self.hardLimitBytes] is not None:
                    remaining_hard_gauge.add_metric([str(self.cluster), fs, dirname, owner],
                                       int(details[self.hardLimitBytes]) - int(details[self.totalBytes]))

            log.info(f"ET to yield metrics for filesystem '{fs}': {round(time.time() - dirname_start, 2)}")
        yield quota_gauge
        yield soft_gauge
        yield hard_gauge
        yield used_gauge
        yield remaining_hard_gauge

    # returns a list of filesystem names
    def get_filesystems(self):
        if self.filesystems is not None:
            return self.filesystems
        self.api_stats['num_calls'] += 1
        try:
            filesystems_cap = self.cluster.call_api(method="filesystems_get_capacity", parms={})
        except Exception as exc:
            log.error(f"Error querying cluster for FS names: {exc}")
            return None

        fsnames = list()

        # filesystems_cap is a list of dict's - NOT ANYMORE!
        if type(filesystems_cap) is dict:
            fs_list = filesystems_cap.values()
        elif type(filesystems_cap) is list:
            fs_list = filesystems_cap
        else:
            log.error(f"Error querying cluster for FS names: unexpected datatype: {filesystems_cap}")
            return []

        for fs in fs_list:
            fsnames.append(fs['name'])

        return fsnames


    def test_new_api(self, fs_name):
        def set_old_api():
            self.new_api = False
            self.totalBytes = 'totalBytes'
            self.softLimitBytes = 'softLimitBytes'
            self.hardLimitBytes = 'hardLimitBytes'
            self.nextCookie = 'nextCookie'
            self.snapViewId = 'snapViewId'
            self.inodeId = 'inodeId'
            self.inodeContext = 'inodeContext'
            self.method = "directory_quota_list"
            self.get_path_integrated = False

        def set_new_api():
            self.new_api = True
            self.totalBytes = 'total_bytes'
            self.softLimitBytes = 'soft_limit_bytes'
            self.hardLimitBytes = 'hard_limit_bytes'
            self.nextCookie = 'next_cookie'
            self.snapViewId = 'snap_view_id'
            self.inodeId = 'inode_id'
            self.inodeContext = 'inode_context'
            self.method = "list_directory_quota_v2"
            self.get_path_integrated = True

        # for testing:
        #set_old_api()
        #return
        # make the exporter backward compatible
        if self.new_api is None:
            try:
                quotas = self.cluster.call_api(method='list_directory_quota_v2',
                                               parms={"fs_name": fs_name, "start_cookie": 0})

            except Exception as exc:
                # old API
                log.info(f"New API not available, using old API: {exc}")
                set_old_api()
                return

            # new API (thanks for changing everything LOL)
            log.info(f"New API available")
            set_new_api()
            return

    def get_quotas(self, fs_name):
        first_time = True
        nextCookie = 0
        all_quotas = dict()
        quotas = dict()
        start_time = time.time()
        self.asyncobj = Async(self.cluster, self.max_procs, self.max_threads_per_proc)

        # nextCookie == MAX_U64 means there are no more quotas to fetch for this filesystem
        while nextCookie != MAX_U64 and (len(quotas) > 0 or first_time):
            self.api_stats['num_calls'] += 1
            first_time = False
            this_call_start = time.time()

            parms = {"fs_name": fs_name, "start_cookie": nextCookie }
            if self.get_path_integrated and self.force_name_resolution:
                parms['get_path'] = False    # default is True, so no need to specify it if True

            log.debug(f"{parms}")
            try:
                result = self.cluster.call_api(method=self.method, parms=parms)
            except Exception as exc:
                log.error(f"Error fetching more quotas for fs {fs_name}: {exc}")
                return None

            log.debug(f"ET for api call: {time.time() - this_call_start}")
            nextCookie = result[self.nextCookie]

            quotas = result['quotas']
            if type(quotas) is dict:
                all_quotas.update(quotas)   # new API format
            else:
                for quota in quotas:
                    all_quotas[quota['quota_id']] = quota # old API format
            log.debug(f"number of quotas returned is {len(quotas)}")

        log.info(f"ET for filesystem '{fs_name}': to collect quotas: {round(time.time() - start_time, 2)}s;" +
                                                f" total quotas is {len(all_quotas)}")

        if len(all_quotas) > 0:
            firstkey = next(iter(all_quotas))
            if 'path' in all_quotas[firstkey]:
                log.info(f"The first quota has a path, and that path is '{all_quotas[firstkey]['path']}'")
            else:
                log.info(f"The first quota does not have a path")
        else:
            log.info(f"No quotas found")

        quotas_to_return = dict()
        if self.new_api and not self.enable_name_cache and not self.force_name_resolution:
            for quota_id, quota_details in all_quotas.items():
                if not self.exceeded_only or (self.exceeded_only and
                                          (quota_details[self.totalBytes] > quota_details[self.softLimitBytes] or
                                           quota_details[self.totalBytes] > quota_details[self.hardLimitBytes])):
                    quotas_to_return[quota_id] = quota_details
        else:
            # Get list of hosts
            self.hostlist = self.fetch_hostlist()
            self.circular_host_list = circular_list(inputlist=self.hostlist)

            self.quota_map = dict()

            # resolve all dirnames asynchronously
            async_start_time = time.time()
            for quota_id, quota_details in all_quotas.items():
                if not self.exceeded_only or (self.exceeded_only and
                                              (quota_details[self.totalBytes] > quota_details[self.softLimitBytes] or
                                               quota_details[self.totalBytes] > quota_details[self.hardLimitBytes])):

                    # is it in our cache?
                    if (self.enable_name_cache and
                                       (fs_name in self.quotas_last and quota_id in
                                       self.quotas_last[fs_name] and 'path' in self.quotas_last[fs_name][quota_id])):
                        # update the path we last used for this quota
                        quota_details['path'] = self.quotas_last[fs_name][quota_id]['path']
                        if 'last_update_time' in self.quotas_last[fs_name][quota_id]:
                            quota_details['last_update_time'] = self.quotas_last[fs_name][quota_id]['last_update_time']
                        else:
                            quota_details['last_update_time'] = time.time() - random.randint(0,60*60*12)
                        # log.info(f"{quota_details['path']} found in cache")
                        quotas_to_return[quota_id] = quota_details
                    else:
                        # not in cache, so go resolve the name with the cluster
                        # make a map so we can correlate the path and the quota easily
                        if 'path' not in quota_details or quota_details['path'] is None: # might be in there, if using new API
                            map_key = (quota_details.get(self.inodeId), quota_details.get(self.snapViewId))
                            self.quota_map[map_key] = quota_id
                            parms = {'inodeContext': quota_details[self.inodeId], 'snapViewId': quota_details[self.snapViewId]}
                            # queue up API calls
                            self.asyncobj.submit(self.circular_host_list.next(), 'filesystem_resolve_inode', parms)
                            self.api_stats['num_calls'] += 1
                        elif self.enable_name_cache:
                            quota_details['last_update_time'] = time.time() - random.randint(0, 60 * 60 * 12)
            # actually execute the API calls (if any, and in parallel)
            timestamp = time.time()
            namecount = 0
            for nameobj in self.asyncobj.wait():
                namecount += 1
                quota = self.quota_map[(nameobj.parms.get('inodeContext'), nameobj.parms.get('snapViewId'))]
                #if 'path' in all_quotas[quota]:
                #    log.error(f"{all_quotas[quota]['path']} is not supposed to be in cache")
                all_quotas[quota]['path'] = nameobj.result['path']
                # we'll explain the random age adjustment... on startup, we fetch all quotas, they will have timestamps
                # within a minute or so of each other... The background updater would likely have to update nearly all of
                # them at once, so we'll adjust the age to make sure they get spread out over time
                all_quotas[quota]['last_update_time'] = timestamp - random.randint(0,60*60*12)
                quotas_to_return[quota] = all_quotas[quota]

            elapsed_time = time.time() - async_start_time
            time_per_name = (elapsed_time / namecount) if namecount > 0 else 0
            log.info(f"ET for name resolution: {round(elapsed_time, 2)}, ave "
                        + f"secs/name={round(time_per_name,4)}")
        return quotas_to_return

    # no longer needed?
    def _background_name_updater(self):
        # update the path names in the quotas in the background so none are too old...
        while self.new_api is None:
            time.sleep(60)   # don't interfere with initial fetch of data on startup
        if self.new_api:
            return  # new api doesn't need a background updater
        log.info(f"Background name updater starting...")
        while True:
            #log.info(f"Background name updater looping...")
            time.sleep(10)   # only query once every X secs so we don't overload the cluster
            # serialize with the collector so we don't step on each other
            for filesystem in self.quotas_last.keys():
                log.debug(f"Background name updater updating {filesystem} - acquiring lock")
                with self._access_lock:
                    #log.info(f"Background name updater - lock acquired")
                    timenow = time.time()
                    updated_count = 0
                    self.asyncobj = Async(self.cluster, self.max_procs, self.max_threads_per_proc)
                    for quota_id, quota_info in self.quotas_last[filesystem].items():
                        if timenow - quota_info['last_update_time'] > 60*60*12:  # older than 12 hours
                            parms = {'inodeContext': quota_info[self.inodeId], 'snapViewId': quota_info[self.snapViewId]}
                            self.asyncobj.submit(self.circular_host_list.next(), 'filesystem_resolve_inode', parms)
                            updated_count += 1
                            if updated_count >= len(self.hostlist): # that's enough for now
                                break
                    for nameobj in self.asyncobj.wait():
                        #log.info(f"Background name updater updating an entry")
                        try:
                            quota = self.quota_map[(nameobj.parms.get(self.inodeContext), nameobj.parms.get(self.snapViewId))]
                        except KeyError:
                            continue
                        log.debug(f"{quota} updated")
                        self.quotas_last[filesystem][quota]['path'] = nameobj.result['path']
                        self.quotas_last[filesystem][quota]['last_update_time'] = time.time()

                log.debug(f"Background name updater - lock released; held for {round(time.time() - timenow, 4)} seconds")


    def _resolve_dirname(self, quota):
        # use the cluster API to resolve the dirname 
        start_time = time.time()
        #self.api_stats['num_calls'] += 1
        #log.info(f"resolving directory name in background updater")
        try:
            result = self.cluster.call_api(method='filesystem_resolve_inode',
                                           parms={'inodeContext': quota['inodeId'], 'snapViewId': quota['snapViewId']})
        except Exception as exc:
            log.error(f"Error resolving directory name: {exc}")
            return None

        log.debug(f"ET to background resolve name: {time.time() - start_time}")
        return result['path']

    def fetch_hostlist(self):
        start_time = time.time()
        self.api_stats['num_calls'] += 1
        try:
            result = self.cluster.call_api(method='hosts_list', parms={})
        except Exception as exc:
            log.error(f"Error fetching hostlist: {exc}")
            return None
        log.debug(f"ET to fetch hostlist: {time.time() - start_time}")

        up_list = list()
        backends_list = list()
        for host in result.values():
            if host['status'] == 'UP' and host['state'] == 'ACTIVE' and host['hostname'] not in up_list:
                up_list.append(host['hostname'])
                if host['mode'] == 'backend':
                    backends_list.append(host['hostname'])
        return backends_list

