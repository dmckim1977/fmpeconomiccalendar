import copy
from typing import Optional

import numpy as np
import pandas as pd
import pytz
import requests
from pytz.exceptions import UnknownTimeZoneError


class Calendar:

    def __init__(self, apikey, start_date: str, end_date: str, timezone: Optional[str] = 'US/Eastern'):
        self._timezone = timezone
        self._api_key = apikey
        self.start_date = start_date
        self.end_date = end_date
        self._dataframe = None

        self._load_data()

    def _load_data(self):
        js = self._api_fetch()
        self._dataframe = self._to_dataframe(res=js)

    ############## Ran when Initialized #########################
    def _api_fetch(self) -> dict:
        econ_calendar_url = (f"https://financialmodelingprep.com/api/v3/economic_calendar?"
                             f"from={self.start_date}&to={self.end_date}&apikey={self._api_key}")
        res = requests.get(econ_calendar_url)
        return res.json()

    def _to_dataframe(self, res: dict) -> pd.DataFrame:
        """Converts raw response dict to dataframe.

        :return:
        """
        df = pd.DataFrame(res)
        df_aware = self._tz(df)

        return df_aware

    ##################### Return Something ########################
    def to_dict(
            self,
            country: Optional[str] = None,
            currency: Optional[str] = None,
            event: Optional[str] = None,
            impact: Optional[str] = None,
            inplace: Optional[bool] = True,
            *args,
            **kwargs) -> dict:  # is this the format we want?
        """Returns raw response as dict.

        :return:
        """
        filtered = self._filter(self._dataframe, **copy.deepcopy(locals()))
        return filtered.to_dict()

    def to_pandas(
            self,
            country: Optional[str] = None,
            currency: Optional[str] = None,
            event: Optional[str] = None,
            impact: Optional[str] = None,
            inplace: Optional[bool] = True,
            *args,
            **kwargs,
    ) -> pd.DataFrame:
        filtered = self._filter(self._dataframe, **copy.deepcopy(locals()))
        return filtered

    def to_csv(
            self,
            path_or_buf: str,
            sep: Optional[str] = None,
            na_rep: Optional[str] = " ",
            country: Optional[str] = None,
            currency: Optional[str] = None,
            event: Optional[str] = None,
            impact: Optional[str] = None,
            inplace: Optional[bool] = True,
            *args,
            **kwargs,
    ) -> None:
        """

        :param path_or_buf:
        :param sep:
        :param na_rep:
        :param country:
        :param currency:
        :param event:
        :param impact:
        :param inplace:
        :param args:
        :param kwargs:
        :return:
        """
        filtered = self._filter(self._dataframe, **copy.deepcopy(locals()))
        params = {
            "path_or_buf": path_or_buf,
        }
        if sep is not None:
            params['sep'] = sep
        filtered.to_csv(**params)

    ###################### Utilities ###############################
    @staticmethod
    def _filter(dataframe, **kwargs):
        if len(kwargs) > 0:
            # prevent from deleting the wrong keys
            keys_list = ['country', 'currency', 'event', 'impact']
            clean_dict = {key: kwargs[key] for key in kwargs if key in keys_list}
            d = {k: np.atleast_1d(v) for k, v in clean_dict.items() if v is not None}
            if len(d) > 0:
                return dataframe[dataframe[list(d)].isin(d).all(1)]
            else:
                return dataframe
        else:
            return dataframe

    def _tz(self, dataframe):
        dataframe['date'] = pd.to_datetime(dataframe['date'], format="ISO8601", utc=True)
        if self._timezone.upper() != 'UTC':
            try:
                _ = pytz.timezone(self._timezone)
                dataframe['date'] = dataframe['date'].dt.tz_convert(tz=_)
                return dataframe
            except UnknownTimeZoneError as u:
                print(
                    f"Unknown timezone. "
                    f"Visit https://docs.python.org/3/library/zoneinfo.html#zoneinfo.available_timezones"
                    f"for proper timezones. {u}")
