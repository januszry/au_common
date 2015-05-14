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
CHANNEL_ORI = -1
CHANNEL_MERGED = -2
TIME_PUNISHMENT = 100
LOUDNESS_MIN = -16
LOUDNESS_MAX = -12
LOUDNESS_TAR = -14


class InvalidURL(Exception):
    pass


class AudioProber(object):

    """Audio Prober for local files and urls (only for ffprobe).

    Probes protocol, track,
        codec, profile, container, duration,
        bitrate, sample_rate, channel."""

    def __init__(self, url, input_options=[],
                 repeat_times=3, timeout=10, retry_times=5,
                 min_len=10, max_len=20, force_proto=False):
        """Prober.

        Volume and loudness are only for the best_track.

        :param url: url provided as input, can be local file
        :param input_options: a list of ffmpeg input options
        :param repeat_times: times to repeat protocol probing to get con_time
        :param timeout: timeout of probing
        :param retry_times: times of retries to try probing
        :param min_len: min length to get volume / loudness of input
        :param max_len: max length to get volume / loudness of input."""

        self._url = url
        self._input_options = input_options
        self._repeat_times = repeat_times
        self._timeout = timeout
        self._retry_times = retry_times
        self._min_len = min_len
        self._max_len = max_len
        self._force_proto = force_proto

        self._proto = None
        self._tracks = None  # a dict keyed of track-index
        self._con_time = None
        self._best_track_index = None
        self._tested_duration = None

        self._volume = None
        self._loudness = None

        self._logger = logging.getLogger(__name__)

        if '://' not in self._url:  # local file
            if not os.path.isfile(self._url):
                raise InvalidURL(self._url)
            self._ori_proto = 'file'
            self._url_without_proto = self._url
            self._repeat_times = 1
            self._timeout = None
            self._retry_times = 1
        else:
            (self._ori_proto,
             self._url_without_proto) = self._url.split('://', 1)
        if self._force_proto:
            self._logger.info("Forcing protocol to be %s", self._ori_proto)

    def __str__(self):
        return pprint.pformat(vars(self))

    @property
    def output_options(self):
        output_options = []
        index = self.best_track['index']

        if self.is_inverted or self.is_ll or self.best_track['channels'] != 2:
            channel_selected = 0
            output_options.extend(  # select left channel
                ['-map_channel', '0.{}.0'.format(index),
                 '-map_channel', '0.{}.0'.format(index)])
        elif self.is_rr:
            channel_selected = 1
            output_options.extend(  # select left channel
                ['-map_channel', '0.{}.1'.format(index),
                 '-map_channel', '0.{}.1'.format(index)])
        else:
            channel_selected = CHANNEL_ORI

        loudness = self.loudness[channel_selected]
        volume_max = self.volume[channel_selected]['volume_max']
        volume_adjust = None
        if loudness > LOUDNESS_MAX:
            volume_adjust = LOUDNESS_TAR - loudness
        elif loudness < LOUDNESS_MIN:
            volume_adjust = min(
                LOUDNESS_TAR - loudness, 0 - volume_max)
        if volume_adjust:
            output_options.extend(
                ['-af', 'volume={:+.2f}dB'.format(volume_adjust)])

        return output_options

    @property
    def is_inverted(self):
        if self.best_track['channels'] != 2:
            return False
        if self.is_ll or self.is_rr:
            return False

        if self.volume[CHANNEL_MERGED]['volume_mean'] + 10 \
                <= self.volume[CHANNEL_ORI]['volume_mean']:
            return True
        else:
            return False

    @property
    def is_ll(self):
        if self.best_track['channels'] != 2:
            return False

        if self.volume[1]['volume_mean'] + 10 \
                <= self.volume[0]['volume_mean']:
            return True
        else:
            return False

    @property
    def is_rr(self):
        if self.best_track['channels'] != 2:
            return False

        if self.volume[0]['volume_mean'] + 10 \
                <= self.volume[1]['volume_mean']:
            return True
        else:
            return False

    @property
    def is_too_loud(self):
        if self.volume[CHANNEL_ORI]['volume_max'] == 0.0:
            return True
        else:
            return False

    @property
    def is_too_low(self):
        if self.volume[CHANNEL_ORI]['volume_mean'] <= -30.0:
            return True
        else:
            return False

    @property
    def possible_protocols(self):
        """Get a list of possible protocols for <url>.

        rtsp or mmsh for mms
        http or mmsh for http

        :param repeat_times: times to repeat the test"""

        proto = self._ori_proto.replace(
            'rtspt', 'rtsp').replace('rtmpt', 'rtmp')

        # list possible protos
        if self._force_proto:
            return [proto]
        elif proto == 'file':
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
        if self._tracks is not None and \
                self._best_track_index is not None and \
                self._best_track_index in self._tracks:
            return self._tracks[self._best_track_index]
        if self._get_audio_tracks() is None:
            raise InvalidURL(self._url)
        return self._get_best_track()

    @property
    def best_url(self):
        if self._ori_proto == 'file':
            return self._url
        if not self._proto:
            self._get_best_track()
        return self._proto + '://' + self._url_without_proto

    @property
    def volume(self):
        if self._volume is not None:
            return self._volume
        self._get_volume_and_loudness()
        return self._volume

    @property
    def loudness(self):
        if self._loudness is not None:
            return self._loudness
        self._get_volume_and_loudness()
        return self._loudness

    def _get_volume_and_loudness(self):
        """Get volume and ebur128 loudness.

        Get volume with FFMPEG and audio filter volumedetect.
        Get loudness with FFMPEG and filter_complex ebur128.

        volumedetect result example:
        [Parsed_volumedetect_0 @ 0x7fe66361a000] n_samples: 672064
        [Parsed_volumedetect_0 @ 0x7fe66361a000] mean_volume: -22.2 dB
        [Parsed_volumedetect_0 @ 0x7fe66361a000] max_volume: -9.4 dB
        [Parsed_volumedetect_0 @ 0x7fe66361a000] histogram_9db: 11
        [Parsed_volumedetect_0 @ 0x7fe66361a000] histogram_10db: 642
        [Parsed_volumedetect_0 @ 0x7fe66361a000] histogram_11db: 3868

        ebur128 result example:
        [Parsed_ebur128_1 @ 0x7fe663400c40] Summary:

          Integrated loudness:
            I:         -27.9 LUFS
            Threshold: -37.9 LUFS

          Loudness range:
            LRA:         0.8 LU
            Threshold: -47.5 LUFS
            LRA low:   -28.0 LUFS
            LRA high:  -27.2 LUFS"""
        self._tested_duration = self.best_track['duration']

        if self._tested_duration < self._min_len:
            self._tested_duration = self._min_len
        elif self._proto != 'file' and self._tested_duration > self._max_len:
            self._tested_duration = self._max_len

        index = self.best_track['index']
        # a filter_complex graph to get volume and loudness of each channel
        filter_complex_list = ['[0:{}]volumedetect,ebur128[cfull]'.format(
            index)]
        # module_data indexed by module index
        module_data = {
            0: {'name': 'volumedetect',
                'channel': CHANNEL_ORI,
                'volume_mean': None,
                'volume_max': None,
                },
            1: {'name': 'ebur128',
                'channel': CHANNEL_ORI,
                'loudness': None,
                },
            }

        for i in range(self.best_track['channels']):
            filter_complex_list.append(
                '[0:{}]pan=mono|c0=c{},volumedetect,ebur128[c{}]'.format(
                    index, i, i))
            # pan, volumedetect and ebur128 are 3 modules
            module_data[1 + 3 * i + 2] = {
                'name': 'volumedetect',
                'channel': i,
                'volume_mean': None,
                'volume_max': None,
                }
            module_data[1 + 3 * i + 3] = {
                'name': 'ebur128',
                'channel': i,
                'loudness': None,
                }

        # if stereo, add inversion check
        if self.best_track['channels'] == 2:
            filter_complex_list.append(
                '[0:{}]pan=mono|c0=0.5*c0+0.5*c1'
                ',volumedetect,ebur128[cinverted]'.format(
                    index))
            module_data[1 + 3 * self.best_track['channels'] + 2] = {
                'name': 'volumedetect',
                'channel': CHANNEL_MERGED,
                'volume_mean': None,
                'volume_max': None,
                }
            module_data[1 + 3 * self.best_track['channels'] + 3] = {
                'name': 'ebur128',
                'channel': CHANNEL_MERGED,
                'loudness': None,
                }

        url = self.best_url
        input_options = list(self._input_options)
        if self._proto == 'rtsp':
            input_options = ['-rtsp_transport', 'tcp'] + input_options
        elif self._proto == 'rtmp':
            url = url + ' live=1'

        cmd = ['ffmpeg', '-t', str(self._tested_duration)] + input_options + \
            ['-i', url,
             '-filter_complex', ';'.join(filter_complex_list)] + \
            ['-map', '[cfull]', '-f', 'null', '-']
        for i in range(self.best_track['channels']):
            cmd += ['-map', '[c{}]'.format(i), '-f', 'null', '-']
        if self.best_track['channels'] == 2:
            cmd += ['-map', '[cinverted]', '-f', 'null', '-']

        if self._timeout is None:
            timeout = None
        else:  # TODO: timeout adjustment
            timeout = max(self._timeout, self._con_time * 2)
        self._logger.info(
            'Checking volume and loudness of best track %s of %s, '
            'length: %s, timeout: %s',
            pprint.pformat(self.best_track),
            self.best_url,
            self._tested_duration,
            timeout)

        output = subprocess.check_output(
            cmd, timeout=timeout,
            stderr=subprocess.STDOUT).splitlines()

        in_ebur128_summary_flag = False
        current_ebur128_module_index = None
        for line in output:
            line = line.decode('utf-8', 'ignore').strip()
            # if line is not in a summary of ebur128, skip
            if not in_ebur128_summary_flag and not line.startswith('['):
                continue
            if in_ebur128_summary_flag and \
                    line.startswith('I:') and line.endswith('LUFS'):
                module_data[current_ebur128_module_index][
                    'loudness'] = float(
                        line.split()[-2])
                in_ebur128_summary_flag = False
                current_ebur128_module_index = None
            elif line.startswith('[Parsed_ebur128_') and 'Summary' in line:
                current_ebur128_module_index = int(
                    line.split()[0].split('_')[-1])
                in_ebur128_summary_flag = True

            elif line.startswith('[Parsed_volumedetect_') and \
                    'mean_volume' in line:
                line_split = line.split()
                index = int(line_split[0].split('_')[-1])
                module_data[index]['volume_mean'] = float(line_split[-2])
            elif line.startswith('[Parsed_volumedetect_') and \
                    'max_volume' in line:
                line_split = line.split()
                index = int(line_split[0].split('_')[-1])
                module_data[index]['volume_max'] = float(line_split[-2])

        volume = {}
        loudness = {}
        for k, v in module_data.items():
            if v['name'] == 'volumedetect':
                volume[v['channel']] = {
                    'volume_max': v['volume_max'],
                    'volume_mean': v['volume_mean']}
            elif v['name'] == 'ebur128':
                loudness[v['channel']] = v['loudness']
        self._volume = volume
        self._loudness = loudness

    def _get_audio_tracks(self):
        """Probe a url to get all audio tracks.

        Will try every possible protocol for given scheme.
        Will return a dict:
            {proto: {track_index: track_info}}"""

        streams = {}
        for proto in self.possible_protocols:
            if proto == 'file':
                url = self._url
            else:
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
                    tmp_data = None
                    te = None
                    for i in range(self._retry_times):
                        start_time = time.time()
                        if self._timeout is not None:
                            # TODO: timeout adjustment
                            timeout = self._timeout * (1 + i / 2)
                            self._logger.info(
                                'Adjusting timeout to %s', timeout)
                        else:
                            timeout = None
                        try:
                            tmp_data = subprocess.check_output(
                                cmd, timeout=timeout)
                        except Exception as e:
                            te = e
                        else:
                            break
                    if tmp_data is None and te is not None:
                        raise te
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
            streams[proto] = {
                'proto': proto,
                'con_time': avg_conn_time,
                'tracks': tracks_for_current_proto,
                }
        self._logger.info(pprint.pformat(streams))
        valid_tracks = [i for i in streams.values() if i['tracks'] is not None]
        if valid_tracks:
            info_of_selected_track = min(valid_tracks,
                                         key=lambda x: x['con_time'])
            self._con_time = info_of_selected_track['con_time']
            self._proto = info_of_selected_track['proto']
            self._tracks = info_of_selected_track['tracks']
            return self._tracks

    def _get_best_track(self):
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


def probe_and_select_from_stream(url, **kwargs):
    ap = AudioProber(url, **kwargs)
    ap._get_volume_and_loudness()
    result = dict(ap.best_track)
    result['input_options'] = ap._input_options
    result['output_options'] = ap.output_options
    result['best_url'] = ap.best_url
    result['tested_duration'] = ap._tested_duration
    result['con_time'] = ap._con_time
    result['selected_protocol'] = ap._proto
    result['volume'] = ap.volume
    result['loudness'] = ap.loudness
    result['abnormals'] = {
        'inverted': ap.is_inverted,
        'll': ap.is_ll,
        'rr': ap.is_rr,
        'too_loud': ap.is_too_loud,
        'too_low': ap.is_too_low,
        }
    return result


def main():
    # set up argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('url', help='local file / url to probe')
    parser.add_argument('-i', '--input_options',
                        type=lambda x: shlex.split(x),
                        default=[], help='prober')
    parser.add_argument('-r', '--repeat_times', type=int,
                        default=3, help='repeat times for probing protocol')
    parser.add_argument('-t', '--timeout', type=int,
                        default=5, help='timeout for probing')
    parser.add_argument('-f', '--retry_times', type=int,
                        default=3, help='retry times for probing protocol')
    parser.add_argument('--force_proto', action='store_true',
                        help='use scheme in url as proto')
    args = parser.parse_args()

    # set up logging
    config_log.config_log('/tmp', 'auprober.log', 'DEBUG')
    logger = logging.getLogger(__name__)
    logger.info('-' * 40 + '<%s>' + '-' * 40, time.asctime())
    logger.info('Arguments: %s', args)

    logger.info('\n' + pprint.pformat(
        probe_and_select_from_stream(
            args.url,
            input_options=args.input_options,
            repeat_times=args.repeat_times,
            timeout=args.timeout,
            retry_times=args.retry_times,
            force_proto=args.force_proto)))


if __name__ == '__main__':
    main()
