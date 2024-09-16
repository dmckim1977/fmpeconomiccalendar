import requests
import pytz

class Calendar:

    def __init__(self):
        self.time_dict = None
        self.timezone = None

    def set_dates(
            self,
            start_date: str,
            end_date: Optional[str] = None,
            timezone: str = "US/Eastern"):

        self.timezone = timezone
        local_tz = pytz.timezone(timezone)
        utc = pytz.utc

        dt_local_start = parser.parse(start_date).astimezone(local_tz)
        dt_utc_start = dt_local_start.astimezone(utc)
        if end_date:
            dt_local_end = parser.parse(end_date).astimezone(local_tz)
            dt_utc_end = dt_local_end.astimezone(utc)
        if not end_date:
            end_date = start_date
            dt_local_end = dt_local_start
            dt_utc_end = dt_utc_start

        self.time_dict = {
            "start_date": start_date,
            "dt_local_start": dt_local_start,
            "dt_utc_start": dt_utc_start,
            "end_date": end_date,
            "dt_local_end": dt_local_end,
            "dt_utc_end": dt_utc_end

        }


    def database_fetch(self, start, stop):
        conn = pg.connect(user=DB_USER, password=DB_PASS, host=DB_HOST,
                          port=DB_PORT, database=DB_NAME)
        cur = conn.cursor()

        query = """ 
            SELECT date, title, country, currency, forecast, previous, impact, category FROM economic_calendar_econdetail
            WHERE date BETWEEN %s and %s
            AND currency = 'USD'
            """

        # Database is UTC so replace hour to get from Midnight eastern
        start.replace(hour=6)
        stop.replace(hour=6)

        cur.execute(query, (start, stop))

        results = cur.fetchall()

        conn.close()

        return results

    def api_fetch(self, start_date: str, end_date: Optional[str] = None) -> dict:
        """Returns a json list of events from FinancialModelingPrep

        Parameters
        ----------
        start_date: str
            Format YYYY-MM-DD
        end_date: str
            Format YYYY-MM-DD
        api_key: str

        Returns
        -------
        dict
            json of events
        """


        api_key = os.getenv("FMP_API_KEY")
        econ_calendar_url = f"https://financialmodelingprep.com/api/v3/economic_calendar?from={start_date}&to={end_date}&apikey={api_key}"
        res = requests.get(econ_calendar_url)
        js = res.json()
        return js

    def to_dataframe(self, js: dict) -> pd.DataFrame:
        "Broke this code out in case something breaks, or we need a new api."
        df = pd.DataFrame(js)
        df.fillna("---", inplace=True)
        return df

    def get_fmp(self,
                countries: Optional[list] = None,
                impacts: Optional[list] = None,
                currencies: Optional[list] = None
                ):
        if not self.time_dict:
            raise Exception("You must run self.set_dates")
        start_date = self.time_dict['dt_utc_start'].strftime("%Y-%m-%d")
        end_date = self.time_dict['dt_utc_end'].strftime("%Y-%m-%d")

        js = self.api_fetch(
            start_date, end_date
        )

        if countries or impacts or currencies:
            df = self.to_dataframe(js)
            self.df = self.filter_dataframe(
                df, countries, currencies, impacts
            )
        else:
            self.df = self.to_dataframe(js)

    def filter_dataframe(
            self,
            currencies: Optional[list] = None,
            countries: Optional[list] = None,
            impacts: Optional[list] = None,
            inplace=False,
    ) -> pd.DataFrame:
        df = self.df.copy()
        if countries:
            df = df[df['country'].isin(countries)]
        if currencies:
            df = df[df['currency'].isin(currencies)]
        if impacts:
            df = df[df['impact'].isin(impacts)]
        if inplace:
            self.df = df
        else:
            return df

    def refactor_events(self, inplace=False):
        df = self.df.copy()
        df['event'] = df.event.apply(
            lambda x: self._refactor_event(x)
        )
        if inplace:
            self.df = df
        else:
            return df

    def reduce_dataframe(self, inplace=False):
        # TODO add deleted events to row. Need this to add to email description
        df = self.df.copy()
        df['extra'] = np.empty((len(df), 0)).tolist()
        grouped = df.groupby('date')

        small_frame = pd.DataFrame()

        for _, group in grouped:
            if len(group) == 1:
                small_frame = pd.concat([small_frame, group], ignore_index=True)
            elif len(group) > 1:
                new_group = self._reduce_group(group)
                small_frame = pd.concat([small_frame, new_group], ignore_index=True)

        print(f"Reduced from {len(df)} to {len(small_frame)}")

        if inplace:
            self.df = small_frame
        else:
            return small_frame

    def emojify_calendar(self,
                         impact_col: bool = False,
                         event_col: bool = False,
                         new_impact_col_name: Optional[str] = None,
                         inplace=False
                         ):
        if not impact_col and not event_col:
            raise Exception("You must pick a column to emojify.")
        else:
            df = self.df

        # Create new impact column
        if new_impact_col_name:
            impact_col_name = new_impact_col_name
        else:
            impact_col_name = 'impact'

        if impact_col and not event_col:
            df[impact_col_name] = df.apply(lambda x: self._emojify_impacts(x["impact"]), axis=1)
        elif impact_col and event_col:
            # this has to be done in this order if not renaming impact_col
            df['event'] = df.apply(lambda x: self._emojify_impacts(x["impact"], x["event"]), axis=1)
            df[impact_col_name] = df.apply(lambda x: self._emojify_impacts(x["impact"]), axis=1)

        if inplace:
            self.df = df
        else:
            return df

    def create_ics(self, filename: str):
        ics = """BEGIN:VCALENDAR
PRODID:-//The Steamroom Calendar V1.1//EN
CALSCALE:GREGORIAN
VERSION:2.0\n"""

        for i, row in self.df.iterrows():
            row['date'] = pd.to_datetime(row['date'], format="%Y-%m-%d %H:%M:%S")
            dtstart = row['date'].strftime("%Y%m%dT%H%M%SZ")
            dtend = row['date'].strftime("%Y%m%dT%H%M%SZ")
            categories = row['category']
            summary = row['event']
            uid_str = f"{summary}-{dtstart}"
            uuid = hashlib.shake_128(uid_str.encode('utf-8')).hexdigest(16)
            trigger = '-PT2M'

            section = f"""BEGIN:VEVENT
CATEGORIES:{categories}
DTSTART:{dtstart}
DTSTAMP:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
CREATED:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
LAST-MODIFIED:{datetime.now().strftime("%Y%m%dT%H%M%SZ")}
SEQUENCE:0
STATUS:CONFIRMED
TRANSP:OPAQUE
SUMMARY:{summary}
UID:{uuid}
BEGIN:VALARM
ACTION:DISPLAY
TRIGGER:{trigger}
DESCRIPTION:Default Mozilla Description
END:VALARM
END:VEVENT
"""
            ics += section

        ics += ("END:VCALENDAR")

        try:
            with open(filename, 'w', encoding="utf-8") as f:
                f.writelines(ics)
            print('Success')
        except Exception as e:
            print(f"Error saving file.")

    def set_icons(self):
        icons = {"Low": "icons/lowimpact.png", "Medium": "icons/mediumimpact.png", "High": "icons/highimpact.png"}
        self.df["icons"] = self.df["impact"].map(icons)

    def _set_index(self):
        self.df.set_index('Time (ET)', inplace=True)

    def _sort_index(self):
        self.df.sort_index(ascending=True, inplace=True)

    def _convert_timezone(self):
        if self.timezone is not None:
            self.df.index = self.df.index.tz_convert(self.timezone)

    def remove_duplicates_for_print(self):
        # csv_table.reset_index(inplace=True)
        self.df.loc[self.df['Time (ET)'].duplicated(), 'Time (ET)'] = ''
        # FIXME problem if the timezone is not Eastern

    def handle_df_dates(self):
        self.df['date'] = pd.to_datetime(self.df['date'], format="ISO8601", errors='coerce')
        self.df['date'] = self.df['date'].dt.tz_localize("UTC")
        self.df['date'] = self.df['date'].dt.tz_convert(self.timezone)

        # strip the seconds from the times
        self.df['Time (ET)'] = self.df['date'].dt.strftime('%H:%M')
        # FIXME problem if the timezone is not Eastern

    def _combine_units(self):
        self.df['previous'] = self.df['previous'].astype(str) + self.df['unit']
        self.df['estimate'] = self.df['estimate'].astype(str) + self.df['unit']

    def prepare_for_table(self):
        self.handle_df_dates()
        self._set_index()
        self._sort_index()
        self.df.reset_index(inplace=True)
        self.remove_duplicates_for_print()
        self._set_index()
        # self._combine_units() # TODO create a function to do this to handle null values


    def upsert(self):
        # TODO write upsert PG function
        ...

    @staticmethod
    def _emojify_impacts(
            impact: str, alt_category: Optional[str] = None
    ):
        if not alt_category:
            return f"{ImpactEmoji[impact].value} {impact}"
        else:
            return f"{ImpactEmoji[impact].value} {alt_category}"

    @staticmethod
    def _refactor_event(string):
        no_par = re.sub("[\(\[].*?[\)\]]", "", string)

        periods = ['MoM', 'QoQ', 'YoY', 'Adv']
        for period in periods:
            no_par = no_par.replace(period, "")
        return no_par.strip()

    @staticmethod
    def _reduce_group(group):
        idx_to_drop = set()
        idx_list = group.index.to_list()

        for idx, row in group.iterrows():
            main_var = row['event']
            main_impact = Impact[row['impact']].value

            for i in idx_list:
                if i != idx:
                    if i not in idx_to_drop:
                        row_var = group['event'].loc[i]
                        row_impact = Impact[group['impact'].loc[i]].value
                        dist1 = lev.seqratio(main_var.split(" "), row_var.split(" "))
                        dist2 = lev.distance(main_var.split(" ")[0], row_var.split(" ")[0])
                        if dist1 > .5 or dist2 < 3:
                            # print(f"[Dist: {dist1:.2f} | {dist2:.0f}] [Impact: {main_impact} | {row_impact}] {main_var} : {row_var}")
                            if main_impact < row_impact:
                                idx_to_drop.add(i)
                            else:
                                idx_to_drop.add(idx)

        return group.drop(idx_to_drop, axis='index', inplace=False)