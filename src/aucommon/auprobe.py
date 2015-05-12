#! /usr/bin/env python3

import logging
import json
import os
import argparse
import time
import pprint
import shlex

from cocommon.utils import tricks
from cocommon.utils.compat import subprocess
from cocommon.quick_config import config_log

WEIGHT_OF_CODEC = {
    'aac': 1.2,
    'vorbis': 1.2,
    'default': 1,
}

NONEXIST = -1
TIME_PUNISHMENT = 100


class InvalidURL(Exception):
    pass


class AudioProber(object):

    """Audio Prober for local files and urls (only for ffprobe).

    Basic probing probes protocol, track,
        codec, profile, container, duration,
        bitrate, sample_rate, channel;
    Detailed probing probes volume, loudness(ebur128), and issues such as
        inversion, one-sided"""

    def __init__(self, url, input_options=[],
                 repeat_times=3, timeout=10, retry_times=5):
        self._url = url
        self._input_options = input_options
        self._repeat_times = repeat_times
        self._timeout = timeout
        self._retry_times = retry_times

        self._proto = None
        self._tracks = None  # a dict keyed of track-index
        self._con_time = None
        self._best_track_index = None
        self._logger = logging.getLogger(__name__)

        if '://' not in self._url:  # local file
            if not os.path.isfile(self._url):
                raise InvalidURL(self._url)
            self._ori_proto = 'file'
            self._url_without_proto = self._url
            self._repeat_times = 1
            self._retry_times = 1
        else:
            (self._ori_proto,
             self._url_without_proto) = self._url.split('://', 1)

    def __str__(self):
        return pprint.pformat(vars(self))

    @property
    def possible_protocols(self):
        """Get a list of possible protocols for <url>.

        rtsp or mmsh for mms
        http or mmsh for http

        :param repeat_times: times to repeat the test"""

        proto = self._ori_proto.replace(
            'rtspt', 'rtsp').replace('rtmpt', 'rtmp')

        # list possible protos
        if proto == 'file':
            return ['file']
        elif proto == 'rtmp':
            return ['rtmp']
        elif proto == 'http':
            return ['http', 'mmsh']
        elif proto in ['mms', 'mmsh', 'mmst', 'rtsp']:
            return ['rtsp', 'mmsh']
        else:
            self._logger.warning("Protocol %s not supported", proto)
            return []

    @property
    def tracks(self):
        if self._tracks:
            return self._tracks
        return self._get_audio_tracks()

    @property
    def best_track(self):
        if self._tracks is not None and self._best_track_index and \
                self._best_track_index in self._tracks:
            return self._tracks[self._best_track_index]
        self._get_audio_tracks()
        return self._get_best_track()

    def _get_audio_tracks(self):
        """Probe a url to get all audio tracks.

        Will try every possible protocol for given schema.
        Will return a dict:
            {proto: {track_index: track_info}}"""

        tracks = {}
        for proto in self.possible_protocols:
            url = proto + '://' + self._url_without_proto
            if proto != 'file' and not tricks.is_ascii(url):
                url = tricks.url_fix(url)
            input_options = list(self._input_options)
            if proto == 'rtsp':
                input_options = ['-rtsp_transport', 'tcp'] + input_options
            elif proto == 'rtmp':
                url = url + ' live=1'

            cmd = ['ffprobe'] + input_options + \
                [url, '-show_entries', 'format:stream',
                 '-print_format', 'json']
            json_data = None
            probing_time = []
            for i in range(self._repeat_times):
                try:
                    start_time = time.time()
                    tmp_data = tricks.retry(
                        self._retry_times, subprocess.check_output,
                        cmd, timeout=self._timeout)
                except subprocess.CalledProcessError as e:
                    self._logger.warning('Called Process Error: %s', e)
                    probing_time.append(TIME_PUNISHMENT)
                except subprocess.TimeoutExpired as e:
                    self._logger.warning('Timeout Expired: %s', e.cmd)
                    probing_time.append(TIME_PUNISHMENT)
                else:
                    json_data = tmp_data
                    probing_time.append(time.time() - start_time)

            if json_data:
                data = json.loads(json_data.decode('utf-8', 'ignore'))
                tracks_for_current_proto = {}
                for i in data['streams']:
                    if i.get('codec_type') == 'audio':
                        track = {}
                        track['codec'] = i.get('codec_name', '')
                        track['profile'] = i.get('profile', '')
                        track['bit_rate'] = int(float(
                            i.get('bit_rate', NONEXIST)))
                        track['sample_rate'] = int(
                            i.get('sample_rate', NONEXIST))
                        track['channels'] = int(
                            i.get('channels', NONEXIST))
                        track['duration'] = float(
                            i.get('duration',
                                  data['format'].get('duration', NONEXIST)))
                        track['format_name'] = data['format'].get(
                            'format_name', '')
                        index = int(i['index'])
                        track['index'] = index
                        tracks_for_current_proto[index] = track
            else:
                tracks_for_current_proto = None

            avg_conn_time = sum(probing_time) / len(probing_time)
            tracks[proto] = {
                'proto': proto,
                'con_time': avg_conn_time,
                'tracks': tracks_for_current_proto,
                }
        self._logger.info(pprint.pformat(tracks))
        info_of_selected_track = min(
            tracks.values(), key=lambda x: x['con_time'])
        self._con_time = info_of_selected_track['con_time']
        self._proto = info_of_selected_track['proto']
        self._tracks = info_of_selected_track['tracks']
        return self._tracks

    def _get_best_track(self, flush=False):
        """Get best track.

        1. select tracks with almost longest duration;
        2. select tracks with best quality from tracks filtered by 1"""

        if not self.tracks:
            return

        # Calculate longest duration
        max_duration = -1
        for index, track in self.tracks.items():
            if track['duration'] > max_duration:
                max_duration = track['duration']

        # Calculate value of track
        def value(track):
            # if duration does not satisfy, return a negative value
            if max_duration - track['duration'] < 1:
                return -1
            bit_rate = track['bit_rate']
            weight_by_bit_rate = WEIGHT_OF_CODEC.get(
                track['codec'], WEIGHT_OF_CODEC['default'])
            return float(bit_rate * weight_by_bit_rate)

        best_track = max(self.tracks.values(), key=lambda x: value(x))

        self._best_track_index = best_track['index']
        return best_track


def select_best_protocol_for_stream(url, **kwargs):
    ap = AudioProber(url, **kwargs)
    best_track = ap.best_track
    best_track['con_time'] = ap._con_time
    best_track['selected_protocol'] = ap._proto
    return best_track


def main():
    # set up argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('url', help='local file / url to probe')
    parser.add_argument('-i', '--input_options',
                        type=lambda x: shlex.split(x),
                        default='', help='prober')
    parser.add_argument('-r', '--repeat_times', type=int,
                        default=3, help='repeat times for probing protocol')
    parser.add_argument('-t', '--timeout', type=int,
                        default=10, help='timeout for probing')
    parser.add_argument('-f', '--retry_times', type=int,
                        default=5, help='retry times for probing protocol')
    args = parser.parse_args()

    # set up logging
    config_log.config_log('/tmp', 'auprober.log', 'DEBUG')
    logger = logging.getLogger(__name__)
    logger.info('-' * 40 + '<%s>' + '-' * 40, time.asctime())
    logger.info('Arguments: %s', args)

    pprint.pprint(
        select_best_protocol_for_stream(
            args.url,
            input_options=args.input_options,
            repeat_times=args.repeat_times,
            timeout=args.timeout,
            retry_times=args.retry_times))


if __name__ == '__main__':
    main()
