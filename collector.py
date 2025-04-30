import sys
import time
import traceback
from logging import getLogger
from threading import Lock

# initialize logger - configured in main routine
import wekalib
from prometheus_client.core import GaugeMetricFamily

log = getLogger(__name__)


class Collector(object):
    def __init__(self, config, cluster_obj):  # wekaCollector

        # dynamic module globals
        self._access_lock = Lock()
        self.gather_timestamp = None
        self.collect_time = None
        self.clusterdata = {}
        self.threaderror = False
        self.api_stats = {}
        self.backends_only = config['exporter']['backends_only']    # calling routine verifies this is there
        self.exceeded_only = config['exporter']['exceeded_only']    # calling routine verifies this is there
        self.filesystems = config['cluster']['filesystems']

        self.cluster = cluster_obj

    def collect(self):

        global quota_objs

        # lock_starttime = time.time()
        with self._access_lock:  # be thread-safe, and don't conflict with events collection
            # log.info(f"Waited {round(time.time() - lock_starttime, 2)}s to obtain lock")

            self.api_stats['num_calls'] = 0

            log.debug("Entering collect() routine")
            second_pass = False
            should_gather = False

            # get the time once here
            start_time = time.time()

            # first time being called?   force gathering info?
            if self.collect_time is None:
                log.debug("never gathered before")
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
                    quota_objs = self.gather()
                except wekalib.exceptions.NameNotResolvable as exc:
                    log.critical(f"Unable to resolve names; terminating")
                    sys.exit(1)
                except Exception as exc:
                    log.critical(f"Error gathering data: {exc}, {traceback.format_exc()}")
                    # log.critical(traceback.format_exc())
                    return  # raise?

            # yield for each metric
            log.debug("Yielding metrics")
            for i in quota_objs:
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
            quotas = self.get_quotas(fs)
            for quota, details in quotas.items():
                if not self.exceeded_only or (self.exceeded_only and
                                              (details['totalBytes'] > details['softLimitBytes'] or
                                               details['totalBytes'] > details['hardLimitBytes'])):
                    dirname = self.resolve_dirname(details)
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

        # filesystems_cap is a list of dict's
        for _, fs in filesystems_cap.items():
            fsnames.append(fs['name'])

        return fsnames

    def get_quotas(self, fs_name):
        first_time = True
        nextCookie = 0
        all_quotas = dict()
        quotas = dict()
        start_time = time.time()

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

        log.debug(f"ET for filesystem '{fs_name}': {time.time() - start_time}; total quotas is {len(all_quotas)}")
        return all_quotas

    def resolve_dirname(self, quota):
        start_time = time.time()
        self.api_stats['num_calls'] += 1
        try:
            result = self.cluster.call_api(method='filesystem_resolve_inode',
                                           parms={'inodeContext': quota['inodeId'], 'snapViewId': quota['snapViewId']})
        except Exception as exc:
            log.error(f"Error resolving directory name: {exc}")
            return None

        log.debug(f"ET to resolve name: {time.time() - start_time}")
        return result['path']
