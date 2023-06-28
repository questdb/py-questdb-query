__all__ = ['Stats', 'StatsDict']

NS_IN_S = 1e9

STATS_TEMPLATE = '''Duration: {duration_s:.3f}s
Millions of lines: {line_count_millions:.3f}
Millions of lines/s: {throughput_mlps:.3f}
MiB: {byte_count_mib:.3f}
MiB/s: {throughput_mbs:.3f}'''


class Stats:
    def __init__(self, duration_ns: int, line_count: int, byte_count: int):
        self.duration_ns = duration_ns
        self.line_count = line_count
        self.byte_count = byte_count

    @property
    def duration_s(self) -> float:
        """
        How long the query took in seconds.
        """
        return self.duration_ns / NS_IN_S

    @property
    def throughput_mbs(self) -> float:
        """
        How many MiB/s were downloaded and parsed.
        """
        return self.byte_count / self.duration_ns * NS_IN_S / 1024 / 1024

    @property
    def throughput_mlps(self) -> float:
        """
        How many millions of lines per second were parsed.
        """
        return self.line_count / self.duration_ns * NS_IN_S / 1e6

    def __repr__(self) -> str:
        return (f'Stats(duration_s={self.duration_s}, '
                f'line_count={self.line_count}, '
                f'byte_count={self.byte_count}, '
                f'throughput_mbs={self.throughput_mbs}, '
                f'throughput_mlps={self.throughput_mlps})')

    def __str__(self):
        return STATS_TEMPLATE.format(
            duration_s=self.duration_s,
            line_count_millions=self.line_count / 1e6,
            throughput_mbs=self.throughput_mbs,
            byte_count_mib=self.byte_count / 1024 / 1024,
            throughput_mlps=self.throughput_mlps)


class StatsDict(dict):
    """A dict with an additional .query_stats attribute."""

    def __init__(self, other: dict, query_stats: Stats):
        super().__init__(other)
        self.query_stats = query_stats
