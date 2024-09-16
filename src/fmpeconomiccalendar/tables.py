import re
from enum import Enum
from typing import Optional

import Levenshtein as Lev
import numpy as np
import pandas as pd


class Impact(Enum):
    High = 1
    Medium = 2
    Low = 3


class ImpactEmoji(Enum):
    Low = "★☆☆"
    Medium = "★★☆"
    High = "★★★"


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


def set_icons(self):
    icons = {"Low": "icons/lowimpact.png", "Medium": "icons/mediumimpact.png", "High": "icons/highimpact.png"}
    self.df["icons"] = self.df["impact"].map(icons)


def _set_index(self):
    self.df.set_index('Time (ET)', inplace=True)


def _sort_index(self):
    self.df.sort_index(ascending=True, inplace=True)


def remove_duplicates_for_print(self):
    # csv_table.reset_index(inplace=True)
    self.df.loc[self.df['Time (ET)'].duplicated(), 'Time (ET)'] = ''
    # FIXME problem if the timezone is not Eastern


def _combine_units(self):
    self.df['previous'] = self.df['previous'].astype(str) + self.df['unit']
    self.df['estimate'] = self.df['estimate'].astype(str) + self.df['unit']


def prepare_for_table(self):
    self._set_index()
    self._sort_index()
    self.df.reset_index(inplace=True)
    self.remove_duplicates_for_print()
    self._set_index()
    # self._combine_units() # TODO create a function to do this to handle null values


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
    no_par = re.sub("[\(\[].*?[)\]]", "", string)

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
                    dist1 = Lev.seqratio(main_var.split(" "), row_var.split(" "))
                    dist2 = Lev.distance(main_var.split(" ")[0], row_var.split(" ")[0])
                    if dist1 > .5 or dist2 < 3:
                        if main_impact < row_impact:
                            idx_to_drop.add(i)
                        else:
                            idx_to_drop.add(idx)

    return group.drop(idx_to_drop, axis='index', inplace=False)
