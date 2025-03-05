import json
import sys
from datetime import datetime
from urllib.parse import unquote

import requests

_CONTENT_TYPE = "application/x-www-form-urlencoded"
_REALTIME_CHART_API_URL = "https://app.genie.co.kr/chart/j_RealTimeRankSongList.json"
_ALLTIME_CHART_API_URL = "https://app.genie.co.kr/chart/j_RankSongListAlltime.json"
_CHART_API_URL = "https://app.genie.co.kr/chart/j_RankSongList.json"


class GenieChartPeriod:
    Realtime = "R"
    Alltime = "A"
    Daily = "D"
    Weekly = "W"
    Monthly = "M"


class GenieChartRequestException(Exception):
    pass


class GenieChartParseException(Exception):
    pass


class ChartEntry:
    """Represents an entry on a chart.
    Attributes:
        title: The title of the track
        artist: The name of the artist.
        image: The URL of the cover image for the track
        peakPos: The track's peak position on the chart.
        lastPos: The track's last position on the previous period.
        rank: The track's current rank position on the chart.
    """

    def __init__(
            self,
            title: str,
            artist: str,
            image: str,
            peakPos: int,
            lastPos: int,
            rank: int):
        self.title = title
        self.artist = artist
        self.image = image
        self.peakPos = peakPos
        self.lastPos = lastPos
        self.rank = rank

    def __repr__(self):
        return "{}.{}(title={!r}, artist={!r})".format(
            self.__class__.__module__, self.__class__.__name__, self.title, self.artist)

    def __str__(self):
        """Returns a string of the form 'TITLE by ARTIST'."""
        if self.title:
            s = "'%s' by %s" % (self.title, self.artist)
        else:
            s = "%s" % self.artist

        if sys.version_info.major < 3:
            return s.encode(getattr(sys.stdout, "encoding", "") or "utf8")
        else:
            return s

    def json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4,
            ensure_ascii=False,
        )


class ChartData:
    """Represents a particular Bugs chart by a particular period.
    Attributes:
        date: The chart date.
        chartType: The chart type.
        chartPeriod: The period for the chart. (default: GenieChartPeriod.Realtime)
        fetch: A boolean value that indicates whether to retrieve the chart data immediately. If set to `False`, you can fetch the data later using the `fetchEntries()` method.
    """

    def __init__(
        self,
        chartPeriod: GenieChartPeriod = GenieChartPeriod.Realtime,
        fetch: bool = True,
    ):
        self.chartPeriod = chartPeriod
        self.entries = []

        if fetch:
            self.fetchEntries()

    def __getitem__(self, key):
        return self.entries[key]

    def __len__(self):
        return len(self.entries)

    def json(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__,
            sort_keys=True,
            indent=4,
            ensure_ascii=False,
        )

    def fetchEntries(self):
        """headers = {
            "Content-Type": _CONTENT_TYPE
        }"""

        """
            응답 내용(140,141번째 줄 print 결과)에 따르면 서버에서 반환된 데이터가 HTML 형식이며, 보안 정책에 의해 연결 요청이 차단된 상태입니다.
            이 메시지는 주로 서버의 보안 정책이나 방화벽이 요청을 차단했을 때 발생합니다. 구체적으로, 요청에 대해 서버가 접속을 허용하지 않음을 알리고 있습니다.

            해결 방법:
            보안 정책 우회
            요청을 보낼 때 User-Agent 헤더를 추가하여 요청이 브라우저에서 온 것처럼 보이도록 할 수 있습니다.
            일부 웹 서버는 봇이나 자동화된 요청을 차단하기 때문에, 이 헤더를 설정하여 요청을 보내면 차단을 우회할 수 있습니다.
        """
        headers = {
            "Content-Type": _CONTENT_TYPE,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36",
        }

        if (
            self.chartPeriod != GenieChartPeriod.Realtime
            and self.chartPeriod != GenieChartPeriod.Alltime
        ):
            data = {"ditc": self.chartPeriod}
        else:
            data = {"pgSize": "200"}

        if self.chartPeriod == GenieChartPeriod.Realtime:
            url = _REALTIME_CHART_API_URL
        elif self.chartPeriod == GenieChartPeriod.Alltime:
            url = _ALLTIME_CHART_API_URL
        else:
            url = _CHART_API_URL

        res = requests.post(url, headers=headers, data=data)

        # 응답 상태 코드와 응답 내용 확인
        # print(f"Response Status Code: {res.status_code}")
        # print(f"Response Content: {res.text[:500]}")  # 처음 500자만 출력 (내용이 길면
        # 일부만 보기)

        if res.status_code != 200:
            message = f"Request is invalid. response status code={res.status_code}"
            raise GenieChartParseException(message)

        data = res.json()
        if int(data["Result"]["RetCode"]) > 0:
            message = (
                f"Request is invalid. response message=${data['Result']['RetMsg']}"
            )
            raise GenieChartParseException(message)

        self._parseEntries(data)

    """def fetchEntries(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
            "Accept": "application/json"
        }

        url = "GENIE_API_URL"  # ✅ 실제 사용 중인 API URL 확인 필요

        res = requests.get(url, headers=headers)

        print(f"📌 응답 상태 코드: {res.status_code}")  # ✅ 상태 코드 확인
        print(f"📌 응답 본문: {res.text[:500]}")  # ✅ 응답 본문 일부 확인

        if res.status_code != 200:
            raise GenieChartRequestException(f"❌ 요청 실패: {res.status_code}")

        try:
            data = res.json()
        except json.JSONDecodeError:
            raise GenieChartRequestException("❌ JSON 파싱 오류: 응답이 비어있거나 잘못된 형식입니다.")

        return data
"""

    def _parseEntries(self, data):
        try:
            self.date = self._parseDate(data["PageInfo"].get("ChartTime"))
            for item in data["DataSet"]["DATA"]:
                entry = ChartEntry(
                    title=unquote(item["SONG_NAME"]),
                    artist=unquote(item["ARTIST_NAME"]),
                    image=unquote(item["ALBUM_IMG_PATH"]),
                    peakPos=int(item.get("TOP_RANK_NO") or 0),
                    lastPos=int(item["PRE_RANK_NO"]),
                    rank=int(item["RANK_NO"]),
                )
                self.entries.append(entry)
            pass
        except Exception as e:
            raise GenieChartParseException(e)

    def _parseDate(self, time):
        now = datetime.now()
        if time is None:
            return now.replace(hour=0, minute=0, second=0, microsecond=0)

        date_format = "%H:%M"
        parsed_date = datetime.strptime(time, date_format)
        return parsed_date.replace(year=now.year, month=now.month, day=now.day)
