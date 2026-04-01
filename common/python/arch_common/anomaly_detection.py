"""
Baseline anomaly detection
"""

from __future__ import annotations
import logging
from typing import Optional
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def detect_all_anomalies(
    df: pd.DataFrame,
    *,
    machine_setpoints: Optional[pd.DataFrame] = None,
    temp_column: str = "mean_wash_tank_temp",
    rinse_temp_column: str = "mean_rinse_temp",
    conc_column: str = "mean_conductivity",
    setpoint_conc_column: str = "setpoint_conductivity",
    timestamp_column: str = "date_time_utc",
    device_id_col: str = "device_id",
    mac_column: str = "mac_address_string",
    en_wash_col: str = "en_wash_on",
    en_rinse_col: str = "en_rinse_on",
    rolling_conc_avg_column: Optional[str] = None,
) -> pd.DataFrame:
    output = df.copy()

    for column in [temp_column, rinse_temp_column, conc_column, setpoint_conc_column]:
        if column in output.columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")

    for flag in [
        "temp_trend_down",
        "temp_below_setpoint",
        "temp_frequent_violation",
        "rinse_temp_trend_down",
        "rinse_temp_below_setpoint",
        "conc_constant_over",
        "conc_constant_under",
        "conc_ok",
    ]:
        output[flag] = False

    wash_setpoint = None
    rinse_setpoint = None
    if machine_setpoints is not None and not machine_setpoints.empty and mac_column in output.columns:
        sp = machine_setpoints.rename(columns={"mac_address_string": mac_column}) if "mac_address_string" in machine_setpoints.columns and mac_column != "mac_address_string" else machine_setpoints
        if mac_column in sp.columns:
            setpoint_map_wash = dict(zip(sp[mac_column], sp.get("wash_temp_setpoint", pd.Series(dtype=float))))
            setpoint_map_rinse = dict(zip(sp[mac_column], sp.get("rinse_temp_setpoint", pd.Series(dtype=float))))
            wash_setpoint = output[mac_column].map(setpoint_map_wash)
            rinse_setpoint = output[mac_column].map(setpoint_map_rinse)

    #Temperature below setpoint
    if wash_setpoint is not None and temp_column in output.columns and en_wash_col in output.columns:
        for mac, group in output.groupby(mac_column):
            group = group.sort_values(by=timestamp_column)
            index = group.index
            active_mask = pd.to_numeric(group[en_wash_col], errors="coerce").fillna(0).astype(int) == 1
            if not active_mask.any():
                continue
            
            group_wash_setpoint = wash_setpoint.loc[index]
            
            if len(group) < 2:
                # Single event: use same logic as Speed Layer (no window available)
                output.loc[index[active_mask], "temp_below_setpoint"] = (group.loc[index[active_mask], temp_column] < group_wash_setpoint[active_mask]).fillna(False)
            elif len(group) < 4:
                current_below = (group[temp_column] < group_wash_setpoint).fillna(False)
                prev_below = current_below.shift(1).fillna(False)
                output.loc[index[active_mask], "temp_below_setpoint"] = (current_below[active_mask] & prev_below[active_mask]).fillna(False)
            else:
                current_below = (group[temp_column] < group_wash_setpoint).fillna(False)
                window_below = current_below.rolling(window=4, min_periods=1).sum()
                window_total = current_below.rolling(window=4, min_periods=1).count()
                window_ratio = window_below / window_total
                output.loc[index[active_mask], "temp_below_setpoint"] = (window_ratio[active_mask] > 0.5).fillna(False)

    if rinse_setpoint is not None and rinse_temp_column in output.columns and en_rinse_col in output.columns:
        for mac, group in output.groupby(mac_column):
            group = group.sort_values(by=timestamp_column)
            index = group.index
            active_mask = pd.to_numeric(group[en_rinse_col], errors="coerce").fillna(0).astype(int) == 1
            if not active_mask.any():
                continue
            
            grp_rinse_sp = rinse_setpoint.loc[index]
            
            if len(group) < 2:
                output.loc[index[active_mask], "rinse_temp_below_setpoint"] = (group.loc[index[active_mask], rinse_temp_column] < grp_rinse_sp[active_mask]).fillna(False)
            elif len(group) < 4:
                current_below = (group[rinse_temp_column] < grp_rinse_sp).fillna(False)
                prev_below = current_below.shift(1).fillna(False)
                output.loc[index[active_mask], "rinse_temp_below_setpoint"] = (current_below[active_mask] & prev_below[active_mask]).fillna(False)
            else:
                current_below = (group[rinse_temp_column] < grp_rinse_sp).fillna(False)
                window_below = current_below.rolling(window=4, min_periods=1).sum()
                window_total = current_below.rolling(window=4, min_periods=1).count()
                window_ratio = window_below / window_total
                output.loc[index[active_mask], "rinse_temp_below_setpoint"] = (window_ratio[active_mask] > 0.5).fillna(False)

    # Temperature trend down
    if temp_column in output.columns and mac_column in output.columns:
        for mac, group in output.groupby(mac_column):
            if len(group) < 2:
                continue
            group = group.sort_values(by=timestamp_column)
            index = group.index
            diff = group[temp_column].diff()
            output.loc[index, "temp_trend_down"] = (diff < -1.0).fillna(False)

    if rinse_temp_column in output.columns and mac_column in output.columns:
        for mac, group in output.groupby(mac_column):
            if len(group) < 2:
                continue
            group = group.sort_values(by=timestamp_column)
            index = group.index
            diff = group[rinse_temp_column].diff()
            output.loc[index, "rinse_temp_trend_down"] = (diff < -1.0).fillna(False)

    # Temp frequent violation
    if wash_setpoint is not None and temp_column in output.columns:
        for mac, group in output.groupby(mac_column):
            if len(group) < 3:
                continue
            sp_val = wash_setpoint.loc[group.index]
            below = (group[temp_column] < (sp_val - 2.0)).fillna(False)
            ratio = below.sum() / len(below)
            if ratio > 0.2:
                output.loc[group.index, "temp_frequent_violation"] = True

    # Conductivity flags
    conc_col = rolling_conc_avg_column if rolling_conc_avg_column and rolling_conc_avg_column in output.columns else conc_column
    if conc_col in output.columns and setpoint_conc_column in output.columns:
        conc = pd.to_numeric(output[conc_col], errors="coerce")
        sp = pd.to_numeric(output[setpoint_conc_column], errors="coerce")
        for mac, group in output.groupby(mac_column):
            group = group.sort_values(by=timestamp_column)
            index = group.index
            grp_conc = conc.loc[index]
            grp_sp = sp.loc[index]
            
            if len(group) < 2:
                output.loc[index, "conc_constant_over"] = (grp_conc > grp_sp * 1.1).fillna(False)
                output.loc[index, "conc_constant_under"] = (grp_conc < grp_sp * 0.9).fillna(False)
            elif len(group) < 4:
                current_over = (grp_conc > grp_sp * 1.1).fillna(False)
                current_under = (grp_conc < grp_sp * 0.9).fillna(False)
                previous_over = current_over.shift(1).fillna(False)
                previous_under = current_under.shift(1).fillna(False)
                output.loc[index, "conc_constant_over"] = (current_over & previous_over).fillna(False)
                output.loc[index, "conc_constant_under"] = (current_under & previous_under).fillna(False)
            else:
                current_over = (grp_conc > grp_sp * 1.1).fillna(False)
                current_under = (grp_conc < grp_sp * 0.9).fillna(False)
                window_over = current_over.rolling(window=4, min_periods=1).sum()
                window_under = current_under.rolling(window=4, min_periods=1).sum()
                window_total = current_over.rolling(window=4, min_periods=1).count()
                window_ratio_over = window_over / window_total
                window_ratio_under = window_under / window_total
                output.loc[index, "conc_constant_over"] = (window_ratio_over > 0.5).fillna(False)
                output.loc[index, "conc_constant_under"] = (window_ratio_under > 0.5).fillna(False)
        
        # Set derived flags
        output["conc_ok"] = (~output["conc_constant_over"] & ~output["conc_constant_under"])

    return output