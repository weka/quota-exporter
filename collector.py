import sys
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
        self.max_procs = config['exporter']['max_procs']
        self.max_threads_per_proc = config['exporter']['max_threads_per_proc']
        self.filesystems = config['cluster']['filesystems']

        self.cluster = cluster_obj

        #self.quotas = dict()
        self.quotas_last = dict()

        self.asyncobj = None
        self.quota_objs = list()

        # populate the name cache on startup
        #log.info("Collecting...")
        #self.collect()
        #log.info("Collect complete. ")

        self.background_thread = threading.Thread(target=self._background_name_updater, daemon=True)
        self.background_thread.start()


    def collect(self):

        # lock_starttime = time.time()
        log.info("starting collector...")
        with self._access_lock:  # be thread-safe
            log.info("lock acquired...")
            # log.info(f"Waited {round(time.time() - lock_starttime, 2)}s to obtain lock")

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
                    # log.critical(traceback.format_exc())
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
            # save the last set of data; get fresh data
            quotas = self.get_quotas(fs)
            log.info(f"{len(quotas)} quotas in fs {fs}")
            self.quotas_last[fs] = deepcopy(dict(quotas))

            dirname_start = time.time()
            for quota, details in quotas.items():
                if not self.exceeded_only or (self.exceeded_only and
                                              (details['totalBytes'] > details['softLimitBytes'] or
                                               details['totalBytes'] > details['hardLimitBytes'])):

                    dirname = details.get('path', 'error')

                    if details['owner'] is None:
                        details['owner'] = ""   # prevent prom client from puking if it's None
                    quota_gauge.add_metric([str(self.cluster), fs, dirname, details['owner'],
                                            str(round(details['softLimitBytes']/1000/1000/1000,1)),
                                            str(round(details['hardLimitBytes']/1000/1000/1000,1))],
                                            str(round(details['totalBytes']/1000/1000/1000,1)) )
                    if details['softLimitBytes'] <= details['hardLimitBytes']:
                        soft_gauge.add_metric([str(self.cluster), fs, dirname, details['owner']],
                                           float(details['softLimitBytes']))
                    hard_gauge.add_metric([str(self.cluster), fs, dirname, details['owner']],
                                           details['hardLimitBytes'])
                    used_gauge.add_metric([str(self.cluster), fs, dirname, details['owner']],
                                           details['totalBytes'])
                    remaining_hard_gauge.add_metric([str(self.cluster), fs, dirname, details['owner']],
                                           int(details['hardLimitBytes']) - int(details['totalBytes']))
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

    def get_quotas(self, fs_name):
        first_time = True
        nextCookie = 0
        all_quotas = dict()
        quotas = dict()
        start_time = time.time()
        self.asyncobj = Async(self.cluster, self.max_procs, self.max_threads_per_proc)

        while len(quotas) > 0 or first_time:
            self.api_stats['num_calls'] += 1
            first_time = False
            this_call_start = time.time()
            try:
                result = self.cluster.call_api(method='directory_quota_list',
                                               parms={"fs_name": fs_name, "start_cookie": nextCookie})
            except Exception as exc:
                log.error(f"Error fetching more quotas for fs {fs_name}: {exc}")
                return None
            log.debug(f"ET for api call: {time.time() - this_call_start}")
            nextCookie = result['nextCookie']
            quotas = result['quotas']
            all_quotas.update(quotas)
            log.debug(f"number of quotas returned is {len(quotas)}")

        log.info(f"ET for filesystem '{fs_name}': to collect quotas: {round(time.time() - start_time, 2)}s; total quotas is {len(all_quotas)}")

        # Get list of hosts
        hostlist = self.fetch_hostlist()
        self.circular_host_list = circular_list(inputlist=hostlist)

        quota_map = dict()
        quotas_to_return = dict()

        # resolve all dirnames asynchronously
        async_start_time = time.time()
        for quota_id, quota_details in all_quotas.items():
            if not self.exceeded_only or (self.exceeded_only and
                                          (quota_details['totalBytes'] > quota_details['softLimitBytes'] or
                                           quota_details['totalBytes'] > quota_details['hardLimitBytes'])):
                if fs_name in self.quotas_last and quota_id in self.quotas_last[fs_name] and 'path' in self.quotas_last[fs_name][quota_id]:
                    # update the path we last used for this quota
                    quota_details['path'] = self.quotas_last[fs_name][quota_id]['path']
                    quota_details['last_update_time'] = self.quotas_last[fs_name][quota_id]['last_update_time']
                    #log.info(f"{quota_details['path']} found in cache")
                    quotas_to_return[quota_id] = quota_details
                else:
                    # map the keys to something we can identify - the quota id returned by the API isn't useful
                    map_key = (quota_details.get('inodeId'), quota_details.get('snapViewId'))
                    quota_map[map_key] = quota_id
                    parms = {'inodeContext': quota_details['inodeId'], 'snapViewId': quota_details['snapViewId']}
                    # queue up API calls
                    self.asyncobj.submit(self.circular_host_list.next(), 'filesystem_resolve_inode', parms)
                    self.api_stats['num_calls'] += 1
        # actually execute the API calls
        timestamp = time.time()
        for nameobj in self.asyncobj.wait():
            #quota = quota_location_dict[str(nameobj.parms['inodeContext']) + '-' + str(nameobj.parms['snapViewId'])]
            quota = quota_map[(nameobj.parms.get('inodeContext'), nameobj.parms.get('snapViewId'))]
            if 'path' in all_quotas[quota]:
                log.error(f"{all_quotas[quota]['path']} is not supposed to be in cache")
            all_quotas[quota]['path'] = nameobj.result['path']
            all_quotas[quota]['last_update_time'] = timestamp
            quotas_to_return[quota] = all_quotas[quota]
            #log.info(f"{nameobj.result['path']} saved in cache")
        log.info(f"ET for name resolution: {round(time.time() - async_start_time, 2)}")
        #return all_quotas
        return quotas_to_return

    def _background_name_updater(self):
        """ update the path names in the quotas in the background so none are too old... """
        time.sleep(60)   # don't interfere with initial fetch of data on startup
        log.info(f"Background name updater starting...")
        while True:
            log.info(f"Background name updater looping...")
            time.sleep(5)
            #min_time_delta = 1000000 # million secs should be good
            # serialize with the collector so we don't step on each other
            for filesystem in self.quotas_last.keys():
                log.info(f"Background name updater updating {filesystem} - acquiring lock")
                with self._access_lock:
                    #log.info(f"Background name updater - lock acquired")
                    timenow = time.time()
                    updated_count = 0
                    for quota_id, quota_info in self.quotas_last[filesystem].items():
                        #time_delta = timenow - quota_info['last_update_time']
                        #if time_delta < min_time_delta:
                        #    min_time_delta = time_delta
                        if timenow - quota_info['last_update_time'] > len(self.quotas_last[filesystem]):  # assume 1s per query
                            quota_info['path'] = self._resolve_dirname(quota_info)
                            quota_info['last_update_time'] = time.time()
                            updated_count += 1
                            if updated_count >= 5:
                                break   # only do a handful, then re-acquire lock so we don't interfere with Prometheus
                log.info(f"Background name updater - lock released; held for {round(time.time() - timenow, 4)} seconds")
        #return None


    def _resolve_dirname(self, quota):
        """ use the cluster API to resolve the dirname """
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

