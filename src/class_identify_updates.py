# -*- coding: utf-8 -*-
"""
Created on Wed Sep 28 11:51:37 2022

@author: E0642602
"""
# %% Setup Environment

import pandas as pd
import numpy as np
import src.config as CONF

# %% Define class variables

class IdentifyUpdates:

    def reshaping_ileads(self, ileads):
        """
        Reshapes/pivots iLeads table to compare with ERP table.

        Parameters
        ----------
        ileads : pandas dataframe. iLeads is compared with ERP table

        Returns
        -------
        ileads : pandas dataframe. Pivotted ileads table with one row for each
        SerialNumber

        """
        dict_map = CONF.DICT_MAP
        cols_ileads = [(dict_map[key][1]) for key in dict_map]
        cols_ileads.append('SerialNumber')

        ileads_reshaped = ileads.pivot(values=["Component_DateCode"],
                                       index="SerialNumber", columns="Component")
        ileads_reshaped.columns = ileads_reshaped.columns.droplevel()
        rename_dict = {"Battery": dict_map['BatteryDateCode'][1],
                       "Capacitor": dict_map['AcCapServDate'][1], "Fan": dict_map['FanServDate'][1]}
        ileads_reshaped.rename(columns=rename_dict, inplace=True)
        ileads_reshaped = ileads_reshaped.drop(["PowerModule", "UPS"], axis=1)

        ileads = ileads.drop_duplicates(subset=["SerialNumber"])
        ileads = pd.merge(ileads, ileads_reshaped, on="SerialNumber")
        ileads = ileads[ileads.columns.intersection(cols_ileads)]
        return ileads

    def check_updates_in_date_cols(self, erp, ileads):
        """
        Check if ERP table has any updates compared with iLeads table for DateCode columns.

        Parameters
        ----------
        erp : pandas dataframe. ERP table is filtered for Serial Numbers
        contained in iLeads table for specified DateCode columns.

        ileads : pandas dataframe. iLeads table is filtered for Serial Numbers
        contained in ERP table for specified DateCode columns, used for comparison.

        Raises
        ------
        TypeError
            Error raised if input is not a dataframe.

        ValueError
            Error raised if input does not have expected columns.

        Returns
        -------
        updated_ups : pandas dataframe.Contains ERP columns that need updates.

        """
        dict_map = CONF.DICT_MAP
        dict_map = {k: v for k, v in dict_map.items(
        ) if dict_map[k][2] == 'DateCode'}

        ileads = self.reshaping_ileads(ileads)
        ileads = ileads[ileads["SerialNumber"].isin(erp["SerialNumber"])]

        # DateCode changes
        cols_ileads = [(dict_map[key][1]) for key in dict_map]
        cols_ileads.append('SerialNumber')
        cols_erp = list(dict_map.keys())

        cols_erp.append('SerialNumber')

        if not set(cols_erp).issubset(set(erp.columns)):
            missing_cols = set(cols_erp).difference(erp.columns)
            raise ValueError(
                f"{missing_cols} are not available in the ERP table")

        if not set(cols_ileads).issubset(set(ileads.columns)):
            missing_cols = set(cols_ileads).difference(ileads.columns)
            raise ValueError(
                f"{missing_cols} are not available in the iLeads table")

        # Filter columns in erp and ileads for datecode columns
        erp = erp[erp.columns.intersection(cols_erp)]
        ileads = ileads[ileads.columns.intersection(cols_ileads)]

        erp = erp[erp["SerialNumber"].isin(ileads["SerialNumber"])] .sort_values(
            by="SerialNumber", ascending=False).set_index("SerialNumber")
        ileads = ileads.sort_values(
            by="SerialNumber", ascending=False).set_index("SerialNumber")

        updated_ups = erp.copy()
        #updated_ups = pd.DataFrame(index=erp.index, columns=erp.columns)

        for ecols, lcols in dict_map.items():
            # ecols = list(dict_map.keys())[2]
            # lcols = dict_map[ecols]
            print(f'{ecols}: {lcols}')
            # Fix Format of ERP
            empty_date = pd.to_datetime("01/01/1900").strftime(lcols[0])
            erp[ecols] = erp[ecols].fillna(empty_date)
            erp[ecols] = pd.to_datetime(erp[ecols],
                                        format=lcols[0], errors='coerce')

            # Fix Format of iLeads
            ls_dates_iLead = ileads[lcols[1]].copy()
            ls_dates_iLead = pd.to_datetime(ls_dates_iLead, errors='coerce')
            ls_dates_iLead = ls_dates_iLead.dt.strftime(lcols[0])
            ls_dates_iLead = pd.to_datetime(ls_dates_iLead, errors='coerce',
                                            format=lcols[0])

            ls_dates_iLead_str = ls_dates_iLead.dt.strftime(lcols[0])
            # Compare date codes
            updated_ups[ecols] = np.where(
                ((erp[ecols] >= ls_dates_iLead) | pd.isna(erp[ecols])),
                '',
                ls_dates_iLead_str)


        return updated_ups

    def check_updates_str_cols(self, erp, ileads):
        """
        Check if ERP table has updates compared with iLeads for string-type columns.

        Parameters
        ----------
        erp : pandas dataframe.ERP table is filtered for Serial Numbers
        contained in iLeads table for specified string-type columns.

        ileads : pandas dataframe.iLeads table is filtered for Serial Numbers
        contained in ERP table for specified string-type columns, used for comparison.

        Raises
        ------
        ValueError
            Error raised if input does not have expected columns.

        Returns
        -------
        updated_ups_str_cols : pandas dataframe.Contains ERP columns that need updates.

        """
        ileads = self.reshaping_ileads(ileads)
        ileads = ileads[ileads["SerialNumber"].isin(erp["SerialNumber"])]

        # Identify columns with "Missing Value" Logic
        dict_map = CONF.DICT_MAP
        dict_map = {k: v for k, v in dict_map.items(
        ) if dict_map[k][2] == 'MissingData'}
        cols_ileads = [(dict_map[key][1]) for key in dict_map]
        cols_ileads.insert(0, 'SerialNumber')
        cols_erp = list(dict_map.keys())
        cols_erp.insert(0, 'SerialNumber')

        # Raise Error if required columns are unavailable
        if not set(cols_erp).issubset(set(erp.columns)):
            missing_cols = set(cols_erp).difference(erp.columns)
            raise ValueError(
                f"{missing_cols} are not available in the ERP table")

        if not set(cols_ileads).issubset(set(ileads.columns)):
            missing_cols = set(cols_ileads).difference(ileads.columns)
            raise ValueError(
                f"{missing_cols} are not available in the iLeads table")

        # Pre-Process data frames before comparing
        erp = erp[erp.columns.intersection(cols_erp)]
        ileads = ileads[ileads.columns.intersection(cols_ileads)]

        erp = erp[erp["SerialNumber"].isin(
            ileads["SerialNumber"])].sort_values(
                by="SerialNumber", ascending=False).set_index("SerialNumber")
        ileads = ileads.sort_values(
            by="SerialNumber", ascending=False).set_index("SerialNumber")

        # Initialize Output
        updated_ups_str_cols = erp.copy()

        for ecols, lcols in dict_map.items():
            # ecols = list(dict_map.keys())[1]
            # lcols = dict_map[ecols]

            if lcols[0] == "":
                iLead_data = ileads[lcols[1]]
            else:
                iLead_data = pd.to_datetime(ileads[lcols[1]])
                iLead_data = iLead_data.dt.strftime(lcols[0])

            updated_ups_str_cols[ecols] = np.where(
									               np.logical_and(erp[ecols].isnull(),
												                  iLead_data.notnull()),
									               iLead_data,""
                                                  )
            del iLead_data

        return updated_ups_str_cols

    def update_values(self, erp, ileads):
        """
        Merge dataframes produced from above 2 functions.

        Parameters
        ----------
        erp : pandas dataframe. ERP all columns are compared to check for updates

        ileads : pandas dataframe. iLeads all columns are compared
            DESCRIPTION.

        Returns
        -------
        updated_erp : pandas dataframe.created a new dataframe with updates for all columns.

        """
        if not isinstance(erp, pd.DataFrame):
            raise TypeError("ERP table is not a Dataframe")

        if not isinstance(ileads, pd.DataFrame):
            raise TypeError("iLeads table is not a Dataframe")

        dict_map = CONF.DICT_MAP

        cols_ileads = [(dict_map[key][1]) for key in dict_map]
        cols_ileads.insert(0, 'SerialNumber')
        cols_erp = list(dict_map.keys())
        cols_erp.insert(0, 'SerialNumber')

        updated_ups = self.check_updates_in_date_cols(erp, ileads)
        updated_ups_str_cols = self.check_updates_str_cols(erp, ileads)

        updated_erp = updated_ups.join(updated_ups_str_cols)

        # Format Output
        updated_erp = updated_erp.reset_index()
        ls_cols = list(CONF.DICT_MAP.keys())
        ls_cols.append('SerialNumber')
        updated_erp = updated_erp[ls_cols]
        updated_erp = updated_erp.replace('nan', '', regex=True)

        return updated_erp
