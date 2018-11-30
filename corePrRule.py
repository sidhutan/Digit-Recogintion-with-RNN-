# -*- coding: utf-8 -*-
"""
Created on Sun Aug 13 18:50:06 2017

@author: Jasmeet.Gujral
"""

from enum import Enum
import numpy as np
import bisect
import os
import pandas as pd
import scipy.stats
import dateutil
import math
import calendar
import time
from decimal import *
import datetime as dt
from pandas.tseries.offsets import BMonthEnd, BDay
from dateutil import relativedelta
from sklearn import linear_model
from functools import partial
from coreHelperFunctions import UnitCalculations as hu, FundingCostCalculaitons as hf, DateNDays as hdd
import coreHelperFunctions as hp

class PrNames(Enum):
    genBankComp = 'GenerateBankComposition'
    genCompLevel = 'GenerateCompositionLevel'
    genVTComp = 'GenerateVTComposition'
    genVolSurf = 'GenerateVolSurface'
    genFwdCurve = 'GenerateForwardCurve'
    genOptionPremium = 'GenerateOptionPremium'    
#    genVTCompLevel = 'GenerateVTCompositionLevel'

import coreOptionMetrics as OM 
class SubPrNames(Enum):
    
    genBankComp_StdCloseComp = 'StdCloseComp'
    genBankComp_DBCloseComp = 'DBCloseComp'
    genBankComp_MRUECloseComp = 'MRUECloseComp'
    genBankComp_CloseBankOCwoCash = 'CloseBankOCwoCash'
    genBankComp_IXERClose = 'IXERClose'
    genBankComp_LNENClose = 'LNENClose'
    genBankComp_RDUEClose = 'RDUEClose'
    
    genBankComp_StdOpenComp = 'StdOpenComp'
    genBankComp_IoIOpenSameCurrency = 'IoIOpenSameCurrency'
    genBankComp_IoIMRUEOpen = 'IoIMRUEOpen'
    genBankComp_OpenBankOCwoCash = 'OpenBankOCwoCash'
    genBankComp_BarclaysExceedOpen = 'BarclaysExceedOpen'
    genBankComp_BarclaysExcessReturnOpen = 'BarclaysExcessReturnOpen'
    genBankComp_RDUEOpen = 'RDUEOpen'
    genComp2Level_StdComp2Level = 'StdComp2Level'
    genComp2Level_StdComp2woFXLevel = 'StdComp2LevelwoFX'
    genComp2Level_RoundStdComp2Level = 'RoundStdComp2Level'
    genComp2Level_EqComp2Level = 'EqComp2Level'
    genComp2Level_OptionComp2Level = 'OptionComp2Level'
    
    genVTComp_OpenClose = 'VtCompOpenClose' 
    genVolSurf_LnIntVolSurf = 'LnItpVolSurface'
    genFwdCurve_FwdCurve = 'FwdCurve'
    genOptionPremium_OptionPremium = 'OptionPricing'
    genBankComp_EquityOptionBankOpen = 'EquityOptionBankOpen'
    genBankComp_EquityOptionBankClose = 'EquityOptionBankClose'
    
    
class cl_BankOCClose:
    
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_Stdclose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("Stdclose")
            df_NewCloseComposition = cl_FileProcessor.dict_FileData["OpenComposition"].copy()
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            sumproduct_old = sum(df_NewCloseComposition["Units"]*df_NewCloseComposition["FxRate"]*df_NewCloseComposition["Price"])
              
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if instrument[:2] == "Ca":
                    OldCashUnits = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"]
                    CashPrice = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Price"]
                    CashFxRate = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "FxRate"]
                    sumproduct_wocash = sumproduct_old-(OldCashUnits*CashPrice*CashFxRate)
            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)].copy()
            
            sumproduct_new = np.nansum(temp_df["BankUnit"]*temp_df["FxRate"]*temp_df["Price"])
#            print(sumproduct_new)
            NewCashUnits = float((sumproduct_new - sumproduct_wocash)/(CashPrice*CashFxRate))
            
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if instrument[:2] == "Ca":
                    df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"] = NewCashUnits

            if 'SecurityId' in df_NewCloseComposition.columns:
                print(df_NewCloseComposition['SecurityId']) 
                df_NewCloseComposition['SecurityId'] = df_NewCloseComposition['SecurityId'].fillna(0).astype('int64',  errors = 'ignore')
                print(df_NewCloseComposition['SecurityId'].replace(0, np.nan))
            return [df_NewCloseComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
        
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_DBClose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("DBClose")
            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy() 
            CashUnits = set(temp_df["BankIndexLevel"]).pop()-sum(temp_df["BankUnit"]*temp_df["Price"]*temp_df["FxRate"])
            temp_df["Units"] = temp_df["BankUnit"]
            df_NewCloseComposition = temp_df.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["CloseComposition"][cl_FileProcessor.dict_FileData["CloseComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewCloseComposition = df_NewCloseComposition.append(df_cash)
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            print("Close composition calculated")
            return [df_NewCloseComposition]  
            cl_FileProcessor.log.process_success()               
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_MRUEClose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("MRUEClose")
            df_NewCloseComposition = cl_FileProcessor.dict_FileData["OpenComposition"].copy()
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            sumproduct_old = sum(df_NewCloseComposition["Units"]*df_NewCloseComposition["FxRate"]*df_NewCloseComposition["Price"])
            print(sumproduct_old)
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if "Ca:US:" in instrument:
                    OldCashUnits = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"]
                    CashPrice = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Price"]
                    CashFxRate = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "FxRate"]
                    sumproduct_wocash = sumproduct_old-(OldCashUnits*CashPrice*CashFxRate)

            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)].copy()
            sumproduct_new = sum(temp_df["BankUnit"]*temp_df["FxRate"]*temp_df["Price"])
            NewCashUnits = float((sumproduct_new - sumproduct_wocash)/(CashPrice*CashFxRate))
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if "Ca:US:" in instrument:
                    df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"] = NewCashUnits
            return [df_NewCloseComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
       

            
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_CloseBankOCwoCash(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("CloseBankOCwoCash")
            df_NewCloseComposition = cl_FileProcessor.dict_FileData["OpenComposition"].copy()
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            print(df_NewCloseComposition)
            sumproduct_old = sum(df_NewCloseComposition["Units"]*df_NewCloseComposition["FxRate"]*df_NewCloseComposition["Price"])
            print("New Close Composition created") 
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if instrument[:2] == "Ca":
                    OldCashUnits = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"]
                    CashPrice = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Price"]
                    CashFxRate = df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "FxRate"]
                    sumproduct_wocash = sumproduct_old-(OldCashUnits*CashPrice*CashFxRate)
            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)].copy()
            sumproduct_new = temp_df["BankIndexLevel"].values[0]
            NewCashUnits = float((sumproduct_new - sumproduct_wocash)/(CashPrice*CashFxRate))
            for instrument in df_NewCloseComposition["InstrumentVTId"]:
                if instrument[:2] == "Ca":
                    df_NewCloseComposition.loc[df_NewCloseComposition["InstrumentVTId"] == instrument, "Units"] = NewCashUnits
            return [df_NewCloseComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_IXERClose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("IXERClose")
            temp_dfRun = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy().set_index("InstrumentVTId")
            print(temp_dfRun)
            temp_dfLastRun = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.LastRunDate].copy().set_index("InstrumentVTId") 
            print(temp_dfLastRun)
            AbsSum = 0
            SumProduct = 0
            for instruments in temp_dfRun.index:
                if instruments in list(temp_dfLastRun.index):
                    AbsSum += abs(temp_dfRun.loc[instruments,"BankUnit"]-temp_dfLastRun.loc[instruments,"BankUnit"])
                    SumProduct += float(temp_dfRun.loc[instruments,"BankUnit"]*(temp_dfLastRun.loc[instruments,"Price"]-temp_dfRun.loc[instruments,"Price"]))
            print(AbsSum)
            ITC = 0.025*AbsSum
            print(ITC)
            print(temp_dfLastRun["BankIndexLevel"])
            IndexLevel = temp_dfLastRun["BankIndexLevel"].values[0]+SumProduct-ITC            
            CashUnits = IndexLevel-sum(temp_dfRun["BankUnit"]*temp_dfRun["Price"]*temp_dfRun["FxRate"])
                        
            temp_dfRun["Units"] = temp_dfRun["BankUnit"]
            temp_dfRun.reset_index(inplace = True)
            df_NewCloseComposition = temp_dfRun.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["CloseComposition"][cl_FileProcessor.dict_FileData["CloseComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewCloseComposition = df_NewCloseComposition.append(df_cash)
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            print("Close composition calculated")
            return [df_NewCloseComposition]  
            cl_FileProcessor.log.process_success()               
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure") 
            
            
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_LNENClose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("LNENClose")
            temp_dfRun = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy().set_index("InstrumentVTId")
            temp_df_LNE4 = temp_dfRun.loc[temp_dfRun.index.str.contains("LNE4"),:]
            temp_dfRun = temp_dfRun.loc[~(temp_dfRun.index.str.contains("LNE4")),:]
            temp_dfRun["Units"] = temp_dfRun["BankWeight"]*float(temp_df_LNE4.loc[:,"LastRunPrice"])/temp_dfRun["LastRunPrice"]
            IndexLevel = temp_dfRun["BankIndexLevel"].values[0]
            SumProduct = sum(temp_dfRun["Units"]*temp_dfRun["Price"])
            CashUnits = IndexLevel - SumProduct
            temp_dfRun.reset_index(inplace = True)
            df_NewCloseComposition = temp_dfRun.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["CloseComposition"][cl_FileProcessor.dict_FileData["CloseComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewCloseComposition = df_NewCloseComposition.append(df_cash)
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            print("Close composition calculated")
            return [df_NewCloseComposition]  
            cl_FileProcessor.log.process_success()                              
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure") 
        
    @staticmethod
    @hp.output_file(["CloseComposition"])
    def fn_RDUEClose(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("RDUEclose")
            temp_df = cl_FileProcessor.dict_FileData["OpenComposition"][~(cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca"))].copy().set_index("InstrumentVTId")            
            temp_dfLastRun = cl_FileProcessor.dict_FileData["BankOCRawData"][(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.LastRunDate) & (cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId) & ~(cl_FileProcessor.dict_FileData["BankOCRawData"]["InstrumentVTId"].str.contains("Ca"))].copy().set_index("InstrumentVTId")
            temp_df["PnL"] = temp_df["Units"]*(temp_df["Price"] - temp_dfLastRun["Price"])
            LastIndexLevel = temp_dfLastRun["BankIndexLevel"].max()
            IndexLevel = LastIndexLevel + sum(temp_df["PnL"]) - 0.001*sum(temp_dfLastRun["Price"])
            CashUnits = IndexLevel - sum(temp_df["Units"]*temp_df["Price"])
            df_cash = cl_FileProcessor.dict_FileData["CloseComposition"][cl_FileProcessor.dict_FileData["CloseComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            temp_df.reset_index(inplace = True)
            df_NewCloseComposition = temp_df.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_NewCloseComposition = df_NewCloseComposition.append(df_cash)
            df_NewCloseComposition["Date"] = cl_FileProcessor.RunDate
            print("Close composition calculated")
            return [df_NewCloseComposition]  
            cl_FileProcessor.log.process_success()               
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
        
class cl_BankOCOpen:
    
    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_StdOpen(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("StdOpen")
            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy()
            temp_df = temp_df[temp_df["IndexVTId"] == cl_FileProcessor.IndexVTId]
            temp_df.rename(columns = {"BankUnit" : "Units"}, inplace = True)
            df_NewOpenComposition = temp_df.loc[:,cl_FileProcessor.dict_FileData["OpenComposition"].columns]
            print("New Open Composition created")
            return [df_NewOpenComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
    
    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_IoIOpenSameCurrency(cl_FileProcessor):
        try:        
            cl_FileProcessor.log.processname_change("IoIOpenSameCurrency")
            if cl_FileProcessor.IsIndexOfIndices == True:
                temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy()
                temp_df_parent = temp_df[temp_df["IndexVTId"] == cl_FileProcessor.IndexVTId]
                df_cash = temp_df[np.array(temp_df["InstrumentVTId"].str.contains("Ca")) & np.array(temp_df["IndexVTId"] == cl_FileProcessor.IndexVTId)]
                temp_df = temp_df[~(temp_df["InstrumentVTId"].str.contains("Ca"))]                      
                for instruments in temp_df_parent["InstrumentVTId"]:
                    temp_df.loc[temp_df["IndexVTId"] == instruments,"UnitsModified"] = np.array(temp_df.loc[temp_df["IndexVTId"] == instruments, "BankUnit"])*np.array(temp_df_parent.loc[temp_df_parent["InstrumentVTId"] == instruments,"BankUnit"])
                temp_df = temp_df[np.isfinite(temp_df["UnitsModified"])]              
                temp_df.rename(columns = {"UnitsModified" : "Units"}, inplace = True)              
                df_NewOpenComposition = temp_df.loc[:,cl_FileProcessor.dict_FileData["OpenComposition"].columns]
                df_cash["UnitsModified"] = sum(temp_df_parent["Price"]*temp_df_parent["BankUnit"]*temp_df_parent["FxRate"]) \
                                        - sum(temp_df["Price"]*temp_df["Units"]*temp_df["FxRate"]) 
                df_cash.rename(columns = {"UnitsModified" : "Units"}, inplace = True)
                df_NewOpenComposition = df_NewOpenComposition.append(df_cash.loc[:,cl_FileProcessor.dict_FileData["OpenComposition"].columns])
                df_NewOpenComposition["IndexVTId"] = cl_FileProcessor.IndexVTId
                print("New Open Composition created")
                return [df_NewOpenComposition] 
                cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
        

    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_IoIMRUEOpen(cl_FileProcessor):
        try:        
            cl_FileProcessor.log.processname_change("IoIMRUEOpen")
            if cl_FileProcessor.IsIndexOfIndices == True:
                temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy()
                temp_df_parent = temp_df[temp_df["IndexVTId"] == cl_FileProcessor.IndexVTId]
                    
                for instruments in temp_df_parent["InstrumentVTId"]:
                    temp_df.loc[temp_df["IndexVTId"] == instruments,"UnitsModified"] = np.array(temp_df.loc[temp_df["IndexVTId"] == instruments, "BankUnit"])*np.array(temp_df_parent.loc[temp_df_parent["InstrumentVTId"] == instruments,"BankUnit"]) \
                                                                                        *np.array(round(temp_df.loc[temp_df["IndexVTId"] == instruments, "IndexLevel"],2))/np.array(temp_df.loc[temp_df["IndexVTId"] == instruments, "IndexLevel"])
                temp_df = temp_df[np.isfinite(temp_df["UnitsModified"])]
                for instruments in temp_df["InstrumentVTId"]:
                    if instruments in list(temp_df_parent["InstrumentVTId"]):
                        temp_df.loc[temp_df["InstrumentVTId"]==instruments,"UnitsModified"] += float(temp_df_parent.loc[temp_df_parent["InstrumentVTId"]==instruments,"BankUnit"])
                
                temp_df.rename(columns = {"UnitsModified" : "Units"}, inplace = True)                        
                df_NewOpenComposition = cl_FileProcessor.dict_FileData["OpenComposition"].copy()
                for instruments in temp_df["InstrumentVTId"]:
                    df_NewOpenComposition.loc[df_NewOpenComposition["InstrumentVTId"]==instruments, "Units"] = float(temp_df.loc[temp_df["InstrumentVTId"]==instruments,"Units"])
                CashUnits = sum(temp_df_parent["Price"]*temp_df_parent["BankUnit"]*temp_df_parent["FxRate"]) \
                            -sum(df_NewOpenComposition.loc[~(df_NewOpenComposition["InstrumentVTId"].str.contains("Ca:US")),"Units"] \
                                 *df_NewOpenComposition.loc[~(df_NewOpenComposition["InstrumentVTId"].str.contains("Ca:US")),"Price"] \
                                 *df_NewOpenComposition.loc[~(df_NewOpenComposition["InstrumentVTId"].str.contains("Ca:US")),"FxRate"])
                df_NewOpenComposition.loc[df_NewOpenComposition["InstrumentVTId"].str.contains("Ca:US"),"Units"] = CashUnits
                df_NewOpenComposition["Date"] = cl_FileProcessor.RunDate
                print("New Open Composition created")
                return [df_NewOpenComposition] 
                cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_OpenBankOCwoCash(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("OpenBankOCwoCash")
            temp_df = cl_FileProcessor.dict_FileData["BankOCRawData"][cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate].copy()
            temp_df = temp_df[temp_df["IndexVTId"] == cl_FileProcessor.IndexVTId]
            temp_df.rename(columns = {"BankUnit" : "Units"}, inplace = True)
            df_NewOpenComposition = temp_df.loc[:,cl_FileProcessor.dict_FileData["OpenComposition"].columns]
            df_cash = pd.DataFrame(columns = cl_FileProcessor.dict_FileData["OpenComposition"].columns)
            df_cash = cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca"),:]
            df_cash.loc[:,"Units"] = temp_df["BankIndexLevel"].values[0]-sum(temp_df["Price"]*temp_df["Units"])
            df_NewOpenComposition = df_NewOpenComposition.append(df_cash.loc[:,cl_FileProcessor.dict_FileData["OpenComposition"].columns])
            df_NewOpenComposition["Date"] = cl_FileProcessor.RunDate
            print("New Open Composition created")
            return [df_NewOpenComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_BarclaysExceedOpen(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("BarclaysExceedOpen")
            temp_dfRun = cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)].copy().set_index("InstrumentVTId")
            temp_dfRun["Units"] = temp_dfRun["BankIndexLevel"]*temp_dfRun["BankWeight"]/100
            IndexLevel = temp_dfRun["BankIndexLevel"].values[0]            
            SumProduct = sum(temp_dfRun["Units"]*temp_dfRun["Price"]*temp_dfRun["FxRate"])
            CashUnits = IndexLevel - SumProduct
            temp_dfRun.reset_index(inplace = True)
            df_NewOpenComposition = temp_dfRun.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["OpenComposition"][cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewOpenComposition = df_NewOpenComposition.append(df_cash)
            df_NewOpenComposition["Date"] = cl_FileProcessor.RunDate
            print("Open composition calculated")
            return [df_NewOpenComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            

    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_BarclaysExcessReturnOpen(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("BarclaysExcessReturnOpen")
            temp_dfRun = cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)].copy().set_index("InstrumentVTId")
            temp_dfRun["Units"] = temp_dfRun["BankIndexLevel"]*temp_dfRun["BankWeight"]/temp_dfRun["Price"]
            IndexLevel = temp_dfRun["BankIndexLevel"].values[0]            
            SumProduct = sum(temp_dfRun["Units"]*temp_dfRun["Price"]*temp_dfRun["FxRate"])
            CashUnits = IndexLevel - SumProduct
            temp_dfRun.reset_index(inplace = True)
            df_NewOpenComposition = temp_dfRun.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["OpenComposition"][cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewOpenComposition = df_NewOpenComposition.append(df_cash)
            df_NewOpenComposition["Date"] = cl_FileProcessor.RunDate
            print("Open composition calculated")
            return [df_NewOpenComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
    @staticmethod
    @hp.output_file(["OpenComposition"])
    def fn_RDUEOpen(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("RDUEopen")
            temp_dfRun = cl_FileProcessor.dict_FileData["BankOCRawData"][(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) & (cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId) & ~(cl_FileProcessor.dict_FileData["BankOCRawData"]["InstrumentVTId"].str.contains("Ca"))].copy().set_index("InstrumentVTId")
            last_BankLevel =  cl_FileProcessor.dict_FileData["BankOCRawData"][np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.LastRunDate) & np.array(cl_FileProcessor.dict_FileData["BankOCRawData"]["IndexVTId"] == cl_FileProcessor.IndexVTId)]['IndexLevel'].max()
            temp_dfRun["Units"] = temp_dfRun["BankWeight"]*last_BankLevel/temp_dfRun["LastRunPrice"]
            NewClosePath = os.path.join(cl_FileProcessor.directory, "P_CloseComposition_"+cl_FileProcessor.IndexVTId.replace(":","")+"_"+cl_FileProcessor.RunDate.to_pydatetime().strftime('%Y%m%d')+".txt")
            with open(NewClosePath, 'r') as in_file:
                JsonString = in_file.readline() #To skip the first "run status" line            
                NewCloseComposition = pd.read_json(in_file.readline(), orient = 'records')
            IndexLevel = sum(NewCloseComposition["Units"]*NewCloseComposition["Price"]*NewCloseComposition["FxRate"])
            SumProduct = sum(temp_dfRun["Units"]*temp_dfRun["Price"]*temp_dfRun["FxRate"])
            CashUnits = IndexLevel - SumProduct
            temp_dfRun.reset_index(inplace = True)
            df_NewOpenComposition = temp_dfRun.loc[:,cl_FileProcessor.dict_FileData["CloseComposition"].columns]
            df_cash = cl_FileProcessor.dict_FileData["OpenComposition"][cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca")]
            df_cash["Units"] = CashUnits
            df_NewOpenComposition = df_NewOpenComposition.append(df_cash)
            df_NewOpenComposition["Date"] = cl_FileProcessor.RunDate
            print("Open composition calculated")
            return [df_NewOpenComposition]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            

            
class cl_Comp2Level:
    @staticmethod
    @hp.output_file(["IndexLevel"])
    def fn_StdComp2LevelwoFX(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("StdComp2Level")
            IndexLevel = sum(cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Units"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Price"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["PriceFactor"])
            df_IndexLevel = pd.DataFrame(data = [[cl_FileProcessor.RunDate, cl_FileProcessor.IndexVTId, IndexLevel]], columns = ["Date", "IndexVTId", "IndexLevel"])
            print("Index level calculated")
            return [df_IndexLevel]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")

    
    @staticmethod
    @hp.output_file(["IndexLevel"])
    def fn_StdComp2Level(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("StdComp2Level")
            IndexLevel = sum(cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Units"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Price"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["FxRate"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["PriceFactor"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["FxFactor"])
            df_IndexLevel = pd.DataFrame(data = [[cl_FileProcessor.RunDate, cl_FileProcessor.IndexVTId, IndexLevel]], columns = ["Date", "IndexVTId", "IndexLevel"])
            print("Index level calculated")
            return [df_IndexLevel]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
    @staticmethod
    @hp.output_file(["IndexLevel"])
    def fn_RoundStdComp2Level(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("StdComp2Level")
            IndexLevel_unrounded = sum(cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Units"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Price"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["FxRate"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["PriceFactor"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["FxFactor"])
            IndexLevel = round(IndexLevel_unrounded,4)
            df_IndexLevel = pd.DataFrame(data = [[cl_FileProcessor.RunDate, cl_FileProcessor.IndexVTId, IndexLevel]], columns = ["Date", "IndexVTId", "IndexLevel"])
            print("Index level calculated")
            return [df_IndexLevel]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")    

            
    @staticmethod
    @hp.output_file(["IndexLevel"])    
    def fn_EqComp2Level(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("EqComp2Level")
            MarketCap = sum(cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Units"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["PriceInInstrumentCurrency"] \
                        *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["FxRate"])
            IndexLevel = (MarketCap/float(cl_FileProcessor.dict_FileData["IndexCalculationDetailsForLevel"]["Divisor"])) - float(cl_FileProcessor.dict_FileData["IndexCalculationDetailsForLevel"]["TransactionCost"])
            df_IndexLevel = pd.DataFrame(data = [[cl_FileProcessor.RunDate, cl_FileProcessor.IndexVTId, IndexLevel]], columns = ["Date", "IndexVTId", "IndexLevel"])
            print("Index level calculated")
            return [df_IndexLevel]
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")

            
    @staticmethod
    @hp.output_file(["IndexRiskParameters"]) 
    def fn_IndexRisk(cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("IndexRisk")
            RiskParameters = pd.DataFrame(columns = ["Delta", "Vega", "Gamma", "Theta", "Rho"])
            risk_dict = {}
            for columns in RiskParameters.columns:
                risk_dict[columns] = sum(cl_FileProcessor.dict_FileData["CloseCompositionForLevel"]["Units"] \
                                                    *cl_FileProcessor.dict_FileData["CloseCompositionForLevel"].fillna(value = 0)[columns])
            RiskParameters = pd.DataFrame(data = risk_dict, index = [1])
            RiskParameters["IndexVTId"] = cl_FileProcessor.IndexVTId
            RiskParameters["Date"] = cl_FileProcessor.RunDate
            print(RiskParameters)
            return [RiskParameters]     
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
    
    
            
    @classmethod
    def fn_OptionComp2Level(self, cl_FileProcessor):
        try:
            cl_FileProcessor.log.processname_change("OptionComp2Level")
            
            self.fn_StdComp2Level(cl_FileProcessor)
            self.fn_IndexRisk(cl_FileProcessor)

            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
        
class EDEIndexCalculation:
    
    def __init__(self, cl_FileProcessor):
        print("Index object initialisation")
        self.RunDate = cl_FileProcessor.RunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewCloseComposition = pd.DataFrame(columns = self.CloseComposition.columns)
        self.RebalFlag = 0
        self.LastRunDate = self.CloseComposition.ix[0,"Date"]
        self.CloseCost = 0
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)        
        self.CashUnits = 0
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.BusinessDays = 0
        
        
#    @staticmethod
    def fn_EDERebalCheck(self):
        try:
            self.log.processname_change("EDERebalCheck")
            if pd.to_datetime(max(set(self.DailyPrices["PriceDate"]))) in list(pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="DED1","SettlementDate"])):
                self.RebalFlag = 1
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
           
    def fn_EDECalculateCloseComposition(self):
        try:
            self.log.processname_change("EDECalculateCloseComposition")
            self.NewCloseComposition = self.OpenComposition[~(self.OpenComposition["GenericTicker"].str.contains("Ca"))].copy()
            self.NewCloseComposition["Date"] = self.RunDate

            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
        
    def fn_EDECalculateCloseCost(self):
        try:
            self.log.processname_change("EDECalculateCloseCost")
            self.fn_EDERebalCheck()
#           if self.RebalFlag==0:
            if (str(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2","InstrumentVTId"]) == str(self.CloseComposition.loc[self.CloseComposition["GenericTicker"] == "DED2","InstrumentVTId"])):
                self.CloseCost = abs(float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2","Units"]) \
                                        - float(self.CloseComposition.loc[self.CloseComposition["GenericTicker"] == "DED2", "Units"])) \
                                    * float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"] == self.LastRunDate, "Fee"])
            else:
               self.CloseCost = abs(float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED1","Units"]) \
                                        - float(self.CloseComposition.loc[self.CloseComposition["GenericTicker"] == "DED2", "Units"])) \
                                    * float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"] == self.LastRunDate, "Fee"])
#            
#            else:
#                self.CloseCost = abs(float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED1","Units"]) \
#                                    - float(self.CloseComposition.loc[self.CloseComposition["GenericTicker"] == "DED2", "Units"])) \
#                                * float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"] == self.LastRunDate, "Fee"])
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
                            
    def fn_EDECalculateCashUnits(self):
        try:
            self.log.processname_change("EDECalculateCashUnits")
 
            self.CashUnits = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"] == self.LastRunDate, "IndexLevel"]) \
                            - np.nansum(self.NewCloseComposition.set_index("InstrumentVTId")["Units"]*self.DailyPrices.set_index("SpecificInstrumentVTId").loc[self.DailyPrices.set_index("SpecificInstrumentVTId")["PriceDate"] == self.LastRunDate,"Price"]) \
                            - self.CloseCost
#            print(self.NewCloseComposition.set_index("GenericTicker")["Units"])
#            print((self.DailyPrices.set_index("GenericTicker").loc[self.DailyPrices.set_index("GenericTicker")["PriceDate"] == self.LastRunDate,"Price"]))
            print(self.CashUnits)
                                        
            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["GenericTicker"].str.contains("Ca")])
            for instruments in self.NewCloseComposition["GenericTicker"]:
                if instruments[:2]=="Ca":
                    self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"]==instruments,"Units"] = self.CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

    def fn_EDECalculateIndexLevel(self):
        try:
            self.log.processname_change("EDECalculateIndexLevel")
            self.DailyPrices.set_index("GenericTicker", inplace = True)
            self.NewCloseComposition.set_index("GenericTicker", inplace = True)            
            self.IndexLevel = np.nansum(self.NewCloseComposition.loc[~(self.NewCloseComposition.index.str.contains("Ca")),"Units"] \
                                  *self.DailyPrices.loc[self.DailyPrices["PriceDate"] == self.RunDate,"Price"])\
                              +self.CashUnits
            self.DailyPrices.reset_index(inplace = True)
            self.NewCloseComposition.reset_index(inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
                            

    def fn_EDECalculateIndexSpecificDetails(self):
        try:
            self.log.processname_change("EDECalculateIndexSpecificDetails")
            self.fn_EDERebalCheck()        
            if self.RebalFlag==0:
                self.OpenIndexSpecificData = self.IndexSpecificData.copy()
                self.OpenIndexSpecificData["PriceDate"] = self.RunDate
                self.OpenIndexSpecificData["IndexLevel"] = self.IndexLevel
            else:
                self.OpenIndexSpecificData = self.IndexSpecificData.copy()
                self.OpenIndexSpecificData["PriceDate"] = self.RunDate
                self.OpenIndexSpecificData["IndexLevel"] = self.IndexLevel
                RebalDate = self.RunDate
                NextRebal = pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"]=="DED2") & np.array(self.DailyPrices["PriceDate"]==self.RunDate),"SettlementDate"].item())
                HolidayList = list(pd.to_datetime(self.HolidayCalendar["Date"]))
                self.BusinessDays = np.busday_count(RebalDate, NextRebal, weekmask='1111100', holidays=HolidayList)
                self.OpenIndexSpecificData.ix[0,"IndexCalculationDates"] = self.BusinessDays
                self.OpenIndexSpecificData.ix[0,"DailyUnitChange"] = float(self.OpenComposition.loc[self.OpenComposition["GenericTicker"] == "DED1","Units"]) \
                                                                    / float(self.BusinessDays)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
                                                                
    def fn_EDECalculateOpenComposition(self):
        try:   
            self.log.processname_change("EDECalculateOpenComposition")
            self.fn_EDERebalCheck()
            temp_df = pd.DataFrame(columns = ["GenericTicker", "Units"])
            temp_df["GenericTicker"] = self.NewCloseComposition[~(self.NewCloseComposition["GenericTicker"].str.contains("Ca"))]["GenericTicker"].copy()
            temp_df=temp_df.set_index("GenericTicker")
            if self.RebalFlag==0:
                FrontUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED1", "Units"])
                FrontContract = self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED1", "InstrumentVTId"].values[0]
                BackUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2", "Units"]) \
                            + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "DailyUnitChange"]) \
                            * float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED1") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                            / (float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                               + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "Fee"])) \
                            * float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "BusinessDay"])
                BackContract = self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2", "InstrumentVTId"].values[0]
            else:
                DeltaUnits = float(self.IndexLevel) \
                            - float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2", "Units"]) \
                            * float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"])
                if DeltaUnits>0:
                    FrontUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2", "Units"]) \
                                 + DeltaUnits \
                                 / (float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"])
                                    + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "Fee"]))

                    self.OpenIndexSpecificData.ix[0,"DailyUnitChange"] = FrontUnits / float(self.BusinessDays)
                                 
                    BackUnits = 0 + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "DailyUnitChange"]) \
                                * float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                                / (float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED3") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                                   + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "Fee"])) \
                                * float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "BusinessDay"])

                else:
                    FrontUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"] == "DED2", "Units"]) \
                                 + DeltaUnits \
                                 / (float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"])
                                    - float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "Fee"]))
                    
                    self.OpenIndexSpecificData.ix[0,"DailyUnitChange"] = FrontUnits / float(self.BusinessDays)

                    BackUnits = 0 + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "DailyUnitChange"]) \
                                * float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                                / (float(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED3") & (self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                                   + float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "Fee"])) \
                                * float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"] == self.RunDate, "BusinessDay"])
                FrontContract = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED2") & (self.DailyPrices["PriceDate"] == self.RunDate), "SpecificInstrumentVTId"].values[0]
                BackContract = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "DED3") & (self.DailyPrices["PriceDate"] == self.RunDate), "SpecificInstrumentVTId"].values[0]
            
            temp_df.loc["DED1", "Units"] = FrontUnits
            temp_df.loc["DED2", "Units"] = BackUnits
            temp_df.loc["DED1", "SpecificInstrumentVTId"] = FrontContract
            temp_df.loc["DED2", "SpecificInstrumentVTId"] = BackContract
            self.DailyPrices.set_index("GenericTicker", inplace = True)
            sumproduct = 0
            for ticker in temp_df.index:
                sumproduct+=float(temp_df.loc[ticker,"Units"])*float(self.DailyPrices.loc[(self.DailyPrices.index==ticker)&(self.DailyPrices["PriceDate"] == self.RunDate),"Price"])
            UnitsCashOpen = self.IndexLevel-sumproduct
            
            self.DailyPrices.reset_index(inplace = True)
            temp_df.loc["Cash",  "Units"] = UnitsCashOpen
            temp_df.loc["Cash", "SpecificInstrumentVTId"] = self.OpenComposition.loc[self.OpenComposition["InstrumentVTId"].str.contains("Ca"),"InstrumentVTId"].values[0]                                                 
            self.NewOpenComposition = self.OpenComposition.copy()
            for ticker in self.NewOpenComposition["GenericTicker"]:
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]==ticker, "Units"] = temp_df.loc[ticker, "Units"]

                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]==ticker, "InstrumentVTId"] = temp_df.loc[ticker, "SpecificInstrumentVTId"]
            self.NewOpenComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.fn_EDERebalCheck()
        self.fn_EDECalculateCloseComposition()
        self.fn_EDECalculateCloseCost()
        self.fn_EDECalculateCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]

    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_EDECalculateIndexLevel()
        self.fn_EDECalculateIndexSpecificDetails()
        self.fn_EDECalculateOpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
        
    
class EDUIndexCalculation:

    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.UnderlyingCloseComposition = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.UnderlyingOpenComposition = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].item())
        self.RebalFlag = 0
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
            
    def fn_EDURebalCheck(self):
        try:
            self.log.processname_change("EDURebalCheck")
            start_date = pd.to_datetime({'year': [self.RunDate.year], 'month':[self.RunDate.month], 'day':[1]})
            end_date = pd.to_datetime({'year': [self.RunDate.year], 'month':[self.RunDate.month], 'day':[28]})
            wom = pd.tseries.offsets.WeekOfMonth(week=2,weekday=4)
            theo_rebal_dates = pd.Series(pd.date_range(start=start_date.iloc[0], end=end_date.iloc[0], freq = wom))
            if set(self.RunDate<=theo_rebal_dates) & set(self.OpenDate>theo_rebal_dates):
                self.RebalFlag = 1
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")        
            
    def fn_EDUCloseComposition(self):
        try:
            self.log.processname_change("EDUCloseComposition")
            self.NewCloseComposition = self.UnderlyingCloseComposition.loc[:,self.OpenComposition.columns].copy()
            NewUnits = np.array(self.UnderlyingCloseComposition["Units"])*np.array(self.IndexSpecificData["Units"])
            self.NewCloseComposition["Units"] = NewUnits
            self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"].str.contains("Ca"),"GenericTicker"] = "EURCash"
            self.DailyPrices.loc[self.DailyPrices["PriceDate"] == self.LastRunDate,"Price"]
            self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"]=="EURCash","Units"] -= float(self.DailyPrices.loc[self.DailyPrices["PriceDate"] == self.LastRunDate,"Price"])\
                                                                                                   * float(self.IndexSpecificData["Units"])
            USDCash = float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.LastRunDate,"IndexLevel"]) \
                     - float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"RebalancingTransactionCost"])
            temp_df = self.OpenComposition.loc[self.OpenComposition["GenericTicker"].str.contains("USDCa"),:].copy()         
            temp_df["Units"] = USDCash
            self.NewCloseComposition = self.NewCloseComposition.append(temp_df)
            self.NewCloseComposition["IndexVTId"] = self.IndexVTId
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
    
    def fn_EDUOpenIndexSpecificData(self):
        try:
            self.log.processname_change("EDUOpenIndexSpecificData")
            self.fn_EDURebalCheck()
            if self.RebalFlag==0:
                self.OpenIndexSpecificData = self.IndexSpecificData.copy()
                self.OpenIndexSpecificData["PriceDate"] = self.RunDate
                self.OpenIndexSpecificData["RebalancingTransactionCost"] = 0
            else:
                NewUnits = self.DailyPrices.sort_values(by = "PriceDate").iloc[-4,:]["IndexLevel"] \
                            /(self.DailyPrices.sort_values(by = "PriceDate").iloc[-4,:]["Price"] \
                              *self.DailyPrices.sort_values(by = "PriceDate").iloc[-4,:]["FxRate"])
                RTC = abs(NewUnits - float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"Units"])) \
                      *(float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"Price"]) \
                        *float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"FxRate"]) \
                        *0.005*(1+self.RunDate.month/12))
                self.OpenIndexSpecificData = self.IndexSpecificData.copy()
                self.OpenIndexSpecificData["PriceDate"] = self.RunDate
                self.OpenIndexSpecificData["Units"] = NewUnits
                self.OpenIndexSpecificData["RebalancingTransactionCost"] = RTC
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                
    
    def fn_EDUCalculateIndexLevel(self):
        try:
            self.log.processname_change("EDUCalculateIndexLevel")
            self.IndexLevel = sum(np.array(self.NewCloseComposition.loc[~(self.NewCloseComposition["GenericTicker"].str.contains("USD")),"Units"]) \
                                  *np.array(self.UnderlyingCloseComposition["Price"]) \
                                  *np.array(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"FxRate"])) \
                              +float(self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"].str.contains("USD"),"Units"])
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                

    def fn_EDUOpenComposition(self):
        try:
            self.log.processname_change("EDUOpenComposition")
            self.NewOpenComposition = self.UnderlyingOpenComposition.loc[:,self.OpenComposition.columns].copy()
            NewOpenUnits = np.array(self.UnderlyingOpenComposition["Units"])*np.array(self.OpenIndexSpecificData["Units"])
            self.NewOpenComposition["Units"] = NewOpenUnits
            self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"].str.contains("Ca"),"GenericTicker"] = "EURCash"
            self.DailyPrices.loc[self.DailyPrices["PriceDate"] == self.RunDate,"Price"]
            self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="EURCash","Units"] -= float(self.DailyPrices.loc[self.DailyPrices["PriceDate"] == self.RunDate,"Price"])\
                                                                                                        * float(self.OpenIndexSpecificData["Units"])
            USDCash = self.IndexLevel\
                    -sum(np.array(self.NewOpenComposition["Units"])\
                         *np.array(self.UnderlyingOpenComposition["Price"])\
                         *np.array(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"FxRate"]))
            
            temp_df = self.OpenComposition.loc[self.OpenComposition["GenericTicker"].str.contains("USDCa"),:].copy()         
            temp_df["Units"] = USDCash
            self.NewOpenComposition = self.NewOpenComposition.append(temp_df)
            self.NewOpenComposition["IndexVTId"] = self.IndexVTId
            self.NewOpenComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.fn_EDURebalCheck
        self.fn_EDUCloseComposition()
        print("New Close Composition created")
        return [self.NewCloseComposition]

    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_EDUCalculateIndexLevel()
        self.fn_EDUOpenIndexSpecificData()
        self.fn_EDUOpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]              
             


class SDIVIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]    
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.RebalFlag = 0
        self.SwitchRollFlag = 0 
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        

        
    def fn_SDIVRebalCheck(self):
        try:
            self.log.processname_change("SDIVRebalCheck")
            weekday = 3
            if (weekday-self.RunDate.weekday())==0:
                RebalDate = self.RunDate
            elif (weekday-self.RunDate.weekday())>0:
                RebalDate = self.RunDate + dt.timedelta(days = (weekday-self.RunDate.weekday()))
            elif (weekday-self.RunDate.weekday())<0:
                RebalDate = self.RunDate + dt.timedelta(days = (7 + weekday-self.RunDate.weekday()))                
                
            if self.RunDate<=RebalDate and self.OpenDate>RebalDate:
                self.RebalFlag = 1
            if self.RebalFlag==1:
                if self.OpenDate==pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"]=='DED1') & np.array(self.DailyPrices["PriceDate"]==self.RunDate),"SettlementDate"].values):
                    self.SwitchRollFlag=1
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
        
    def fn_SDIVCloseComposition(self):
        try:
            self.log.processname_change("SDIVCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["GenericTicker"].str.lower().str.contains("ca")),:].copy()
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
            
        
    def fn_SDIVCalculateIndexLevel(self):
        try:
            self.log.processname_change("SDIVCalculateIndexLevel")
            IndexLevelRebal = self.IndexSpecificData["IndexLevelRebal"].values[0]
            weight4 = -(float(self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED4',"Weights"]))
            weight5 = -(float(self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED5',"Weights"]))
            contract4 = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED4',"SpecificTicker"].item()
            contract5 = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED5',"SpecificTicker"].item()
            price4 = float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"]==contract4)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            price5 = float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"]==contract5)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            Price4 = float(self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED4',"Price"])
            Price5 = float(self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED5',"Price"])
            RebalancingTransactionCost = self.IndexSpecificData["RebalancingTransactionCost"].values[0]
            self.IndexLevel = IndexLevelRebal*(1-weight4*(price4/Price4-1)-weight5*(price5/Price5-1))-RebalancingTransactionCost
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
    
    def fn_SDIVCalculateCashUnits(self):
        try:
            self.log.processname_change("SDIVCalculateCashUnits")
            print(self.NewCloseComposition["GenericTicker"])
            contract4=self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED4',"SpecificInstrumentVTId"].item()
            contract5=self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=='DED5',"SpecificInstrumentVTId"].item()
            price4=float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificInstrumentVTId"]==contract4)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            price5=float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificInstrumentVTId"]==contract5)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            self.NewCloseComposition.loc[self.NewCloseComposition['InstrumentVTId']==contract4,'tempPrice'] = price4
            self.NewCloseComposition.loc[self.NewCloseComposition['InstrumentVTId']==contract5,'tempPrice'] = price5
            CashUnits=self.IndexLevel-np.nansum(self.NewCloseComposition.set_index('InstrumentVTId')['Units']*self.NewCloseComposition.set_index('InstrumentVTId')['tempPrice'])
            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["GenericTicker"].str.lower().str.contains("ca")])
            for instruments in self.NewCloseComposition["GenericTicker"]:
                if instruments.lower()[:2]=="ca":
                    self.NewCloseComposition.loc[self.NewCloseComposition["GenericTicker"]==instruments,"Units"] = CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
        
   
    def fn_SDIVOpenIndexSpecificData(self):
        try:
            self.log.processname_change("SDIVOpenIndexSpecificData")
            self.OpenIndexSpecificData=self.IndexSpecificData.copy()
            self.OpenIndexSpecificData["PriceDate"]=self.RunDate
            self.OpenIndexSpecificData["SwitchRollFlag"]=self.SwitchRollFlag
            if self.RebalFlag==1:
                if self.SwitchRollFlag==1:
                    Settlement5=pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED6"),"SettlementDate"].values[0])
                    contract5=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED6"),"SpecificTicker"].values[0]
                    InstrumentVTId5 = self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED6"),"SpecificInstrumentVTId"].values[0]
                    price5=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED6"),"Price"].values[0]
                    Settlement4=pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SettlementDate"].values[0])
                    contract4=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SpecificTicker"].values[0]
                    InstrumentVTId4 = self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SpecificInstrumentVTId"].values[0]
                    price4=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"Price"].values[0]
                else:
                    Settlement5=pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SettlementDate"].values[0])
                    contract5=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SpecificTicker"].values[0]
                    InstrumentVTId5 = self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"SpecificInstrumentVTId"].values[0]
                    price5=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED5"),"Price"].values[0]
                    Settlement4=pd.to_datetime(self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED4"),"SettlementDate"].values[0])
                    contract4=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED4"),"SpecificTicker"].values[0]
                    InstrumentVTId4 = self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED4"),"SpecificInstrumentVTId"].values[0]
                    price4=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["GenericTicker"]=="DED4"),"Price"].values[0]
                    
                DeltaTF5=float((Settlement5-self.RunDate).days-1)/7   
                DeltaTF4=float((Settlement4-self.RunDate).days-1)/7
                weight4=(DeltaTF5-4*365/7)/(DeltaTF5-DeltaTF4)
                weight5=(4*365/7-DeltaTF4)/(DeltaTF5-DeltaTF4)
                unit4=self.IndexLevel*weight4/price4
                unit5=self.IndexLevel*weight5/price5
                UnitLastRebal4=-(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=='DED4',"Units"].values[0])
                UnitLastRebal5=-(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=='DED5',"Units"].values[0])
                Fee=self.OpenIndexSpecificData["Fee"].values[0]
                if self.SwitchRollFlag==0:
                    RebalancingTransactionCost=(abs(unit4-UnitLastRebal4)*price4+abs(unit5-UnitLastRebal5)*price5)*Fee
                else:
                    LastContract4=self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]=="DED4","SpecificTicker"].values[0]
                    PriceRunLast4=self.DailyPrices.loc[np.array(self.DailyPrices["PriceDate"]==self.RunDate)&np.array(self.DailyPrices["SpecificTicker"]==LastContract4),"Price"].values[0]
                    RebalancingTransactionCost=(abs(0-UnitLastRebal4)*PriceRunLast4+abs(unit4-UnitLastRebal5)*price4+abs(unit5-0)*price5)*Fee
                self.OpenIndexSpecificData.set_index("GenericTicker", inplace = True)
                self.OpenIndexSpecificData.loc["DED4", "SpecificTicker"] = contract4
                self.OpenIndexSpecificData.loc["DED5", "SpecificTicker"] = contract5
                self.OpenIndexSpecificData.loc["DED4", "SpecificInstrumentVTId"] = InstrumentVTId4
                self.OpenIndexSpecificData.loc["DED5", "SpecificInstrumentVTId"] = InstrumentVTId5
                self.OpenIndexSpecificData.loc["DED4", "Price"] = price4
                self.OpenIndexSpecificData.loc["DED5", "Price"] = price5
                self.OpenIndexSpecificData.loc["DED4", "Weights"] = -weight4
                self.OpenIndexSpecificData.loc["DED5", "Weights"] = -weight5
                self.OpenIndexSpecificData.loc["DED4", "Units"] = -unit4
                self.OpenIndexSpecificData.loc["DED5", "Units"] = -unit5
                self.OpenIndexSpecificData["IndexLevelRebal"]=self.IndexLevel
                self.OpenIndexSpecificData["RebalancingTransactionCost"]=RebalancingTransactionCost
                self.OpenIndexSpecificData.reset_index(inplace=True)
        
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    

    def fn_SDIVOpenComposition(self):
        try:
            self.log.processname_change("SDIVOpenComposition")
            self.NewOpenComposition=self.OpenComposition.copy()
            self.NewOpenComposition["Date"]=self.RunDate
    #        SpecificTickers=list(self.OpenIndexSpecificData["SpecificTicker"])
            contract4 = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=='DED4',"SpecificInstrumentVTId"].item()
            contract5 = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=='DED5',"SpecificInstrumentVTId"].item()
            price4 = float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificInstrumentVTId"]==contract4)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            price5 = float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificInstrumentVTId"]==contract5)&(np.array(self.DailyPrices["PriceDate"]==self.RunDate)),"Price"])
            self.OpenIndexSpecificData.set_index('GenericTicker', inplace=True)
            self.OpenIndexSpecificData.loc['DED4','tempPrice'] = price4
            self.OpenIndexSpecificData.loc['DED5','tempPrice'] = price5
    
            CashUnits=self.IndexLevel-sum(self.OpenIndexSpecificData["Units"]*self.OpenIndexSpecificData["tempPrice"])
            self.OpenIndexSpecificData.reset_index(inplace=True)
#            print(self.IndexSpecificData)
            if self.IndexSpecificData["SwitchRollFlag"][0]==True:
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","SpecificTicker"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED6", "SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","SpecificTicker"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","InstrumentVTId"]=self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","SpecificInstrumentVTId"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED6","InstrumentVTId"]=self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","SpecificInstrumentVTId"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","Units"]=float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","Units"])
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED6","Units"]=float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","Units"])
                
            else:
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED4","SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","SpecificTicker"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","SpecificTicker"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED4","InstrumentVTId"]=self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","SpecificInstrumentVTId"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","InstrumentVTId"]=self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","SpecificInstrumentVTId"].item()
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED4","Units"]=float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED4","Units"])
                self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]=="DED5","Units"]=float(self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["GenericTicker"]=="DED5","Units"])            
            
            for ticker in self.NewOpenComposition["GenericTicker"]:
                if ticker.lower()[:2]=="ca":
                    self.NewOpenComposition.loc[self.NewOpenComposition["GenericTicker"]==ticker,"Units"]=CashUnits
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
            
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_SDIVRebalCheck()
        self.fn_SDIVCloseComposition()
        self.fn_SDIVCalculateIndexLevel()
        self.fn_SDIVCalculateCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]

    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_SDIVOpenIndexSpecificData()
        self.fn_SDIVOpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]



class MRETIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices = self.DailyPrices.pivot_table(index = "PriceDate", columns = "GenericTicker", values = "Price").reset_index()
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
#        self.DailyPrices.to_csv("DailyPrices.csv")
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]    
        self.TickPrices = cl_FileProcessor.dict_FileData["VTTickPrices"]
        self.TickPrices["PriceDate"] = pd.to_datetime(self.TickPrices["PriceDate"])
        self.TickPrices["EURUSD_F160"] = 1/self.TickPrices["EURUSD_F160"]
        self.TickPrices["EURUSD_F153"] = 1/self.TickPrices["EURUSD_F153"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.IndexLevel = 0
        self.temp_df = pd.DataFrame()
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        
        
        
    def fn_MRETCloseComposition(self):
        try:    
            self.log.processname_change("MRETCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
    
    def fn_CalculateDayFraction(self):
        return (self.RunDate-self.LastRunDate).days/360       
        
    def fn_MRETCalculateIndexLevel(self):
        try:
            self.log.processname_change("MRETCalculateIndexLevel")
            ExcessReturn = self.NewCloseComposition.copy()
            for ticker in ExcessReturn["SpecificTicker"]:                
                UnderlyingUnits = float(ExcessReturn.loc[ExcessReturn["SpecificTicker"]==ticker,"Units"])

                UnderlyingPriceCCYRun = float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,ticker]) \
                                        /float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.RunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"EURUSD_F160"])
                UnderlyingPriceCCYLastRun = float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.LastRunDate,ticker]) \
                                            /float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.LastRunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"EURUSD_F160"])
                
                DividendCCY = float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.RunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"Dividend"]) \
                              /float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.RunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"EURUSD_F160"])
                
                ActualFundingRate = (float(self.IndexSpecificData.loc[self.IndexSpecificData["SpecificTicker"]==ticker,"FundingRate"])/100)*self.fn_CalculateDayFraction()
                ExcessReturn.loc[ExcessReturn["SpecificTicker"]==ticker, "ERComponentStrategy"] = UnderlyingUnits*(UnderlyingPriceCCYRun + DividendCCY - UnderlyingPriceCCYLastRun*(1+ActualFundingRate)) \
                                                                                          - float(self.IndexSpecificData.loc[self.IndexSpecificData["SpecificTicker"]==ticker,"RebalancingTransactionCost"])
                                                                                          
            InterestEarned = float(self.IndexSpecificData["IndexLevel"][0])\
                            *(1+(float(self.IndexSpecificData["FundingRate"][0])/100 \
                                 -float(self.IndexSpecificData["Fee"][0]))\
                                *self.fn_CalculateDayFraction())
            self.IndexLevel = InterestEarned+sum(ExcessReturn["ERComponentStrategy"])
            print("self.IndexLevel = ",self.IndexLevel)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    

    def fn_MRETCalculateCloseCashUnits(self):
        try:
            self.log.processname_change("MRETCalculateCloseCashUnits")
            sumproduct=0
            for ticker in self.NewCloseComposition["SpecificTicker"]:
                UnderlyingUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==ticker,"Units"])
                UnderlyingPriceCCYRun = float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,ticker]) \
                                        /float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.RunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"EURUSD_F160"])
                sumproduct+=UnderlyingUnits*UnderlyingPriceCCYRun
                
            CashUnits=self.IndexLevel-sumproduct

            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")])
            for instruments in self.NewCloseComposition["SpecificTicker"]:
                if instruments.lower()[:2]=="ca":
                    self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
        
    def fn_Average(self,n):
        df_temp = self.DailyPrices[self.DailyPrices["PriceDate"]<self.RunDate].sort_values(by = "PriceDate").iloc[-(n-1):,1:]
        average = pd.DataFrame(columns = ["SpecificTicker", "Average"]).set_index("SpecificTicker")
        for ticker in df_temp.columns:
            average.loc[ticker, "Average"] = (sum(df_temp[ticker])\
                                              +float(self.TickPrices.loc[np.array(self.TickPrices["PriceDate"]==self.RunDate) & np.array(self.TickPrices["SpecificTicker"]==ticker),"TickPrice"]))/n
        average=average.rename(columns = {"Average" : "Average"+str(n)})
        return average


    def fn_VolatilityInterim(self):
        df_tempt = self.DailyPrices[self.DailyPrices["PriceDate"]<self.RunDate].sort_values(by = "PriceDate").iloc[-29:,1:]
        df_temptminus = self.DailyPrices[self.DailyPrices["PriceDate"]<self.LastRunDate].sort_values(by = "PriceDate").iloc[-29:,1:]
        LogReturn = pd.DataFrame(columns = ["SpecificTicker", "LogReturn"]).set_index("SpecificTicker")
        for ticker in df_tempt.columns:
            LogReturn.loc[ticker, "LogReturn"] = sum((pd.Series(np.array(df_tempt[ticker])/np.array(df_temptminus[ticker])).apply(math.log)).pow(2))            
        return LogReturn  

                                                            
    def fn_MRETOpenComposition(self):
        try:
            self.log.processname_change("MRETOpenComposition")
            average10 = self.fn_Average(10)
            average200 = self.fn_Average(200)
            average200.sort_index(inplace = True)
            average10.sort_index(inplace = True)
            self.temp_df = average10.copy()
            self.temp_df["Average200"] = average200["Average200"]
            self.temp_df["TickPriceRun"] = np.array(self.TickPrices.sort_values(by='SpecificTicker').loc[self.TickPrices["PriceDate"]==self.RunDate,"TickPrice"])
            self.temp_df["PriceLastRun"] = np.array(np.transpose(self.DailyPrices.ix[self.DailyPrices["PriceDate"]==self.LastRunDate,1:].sort_index(axis =1)))
            self.temp_df["Indicator"] = (self.temp_df["TickPriceRun"]<self.temp_df["Average200"]).astype(int)
            VolatilityInterim = self.fn_VolatilityInterim().sort_index()
            self.temp_df["Volatility"]=((252/30)*(VolatilityInterim["LogReturn"]+(pd.Series(self.temp_df["TickPriceRun"]/self.temp_df["PriceLastRun"]).apply(math.log)).pow(2))).apply(math.sqrt)
            self.temp_df["Exposure"] = (((8/self.temp_df["Volatility"])*(self.temp_df["Average10"]-self.temp_df["TickPriceRun"])/self.temp_df["Average10"])/0.2).apply(round)*0.2
            self.temp_df["FinalExposure"] = np.maximum(np.array(self.temp_df["Exposure"].apply(lambda x: min(1, x))),-1*np.array(self.temp_df["Indicator"]))
            self.temp_df["EURUSD_F153"]=np.array(self.TickPrices.sort_values(by='SpecificTicker').loc[self.TickPrices["PriceDate"]==self.RunDate,"EURUSD_F153"])        
            self.temp_df["Units"]= float(self.IndexSpecificData["IndexLevel"][0])*np.array(self.temp_df["EURUSD_F153"])*np.array(self.temp_df["FinalExposure"])/(3*np.array(self.temp_df["TickPriceRun"]))
            self.temp_df["PriceRun"] = np.array(np.transpose(self.DailyPrices.ix[self.DailyPrices["PriceDate"]==self.RunDate,1:].sort_index(axis =1)))
            self.temp_df["EURUSD_F160"]=np.array(self.TickPrices.sort_values(by='SpecificTicker').loc[self.TickPrices["PriceDate"]==self.RunDate,"EURUSD_F160"])
            print(self.temp_df["Units"])
            self.NewOpenComposition = self.OpenComposition.copy()
            self.NewOpenComposition["Date"] = self.RunDate
            for ticker in self.NewOpenComposition["SpecificTicker"]:
                if "ca" in ticker.lower():
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==ticker,"Units"] = self.IndexLevel- sum(self.temp_df["Units"]*self.temp_df["PriceRun"]/self.temp_df["EURUSD_F160"])
                else:
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==ticker,"Units"] = self.temp_df.loc[ticker,"Units"]
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                                                                
                                                                
    def fn_MRETOpenIndexSpecificData(self):
        try:
            self.log.processname_change("MRETOpenIndexSpecificData")
            self.IndexSpecificData.sort_values(by = "SpecificTicker", inplace = True)
#            print(self.IndexSpecificData)
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            self.OpenIndexSpecificData["PriceDate"] = self.RunDate
            self.OpenIndexSpecificData["IndexLevel"] = self.IndexLevel
            self.OpenIndexSpecificData["SpecificTicker"] = self.temp_df.index
            self.OpenIndexSpecificData = self.OpenIndexSpecificData.set_index("SpecificTicker")
            self.OpenIndexSpecificData["FinalExposure"] = self.temp_df["FinalExposure"]
            self.OpenIndexSpecificData["RebalancingTransactionCost"]=self.IndexSpecificData.set_index("SpecificTicker")["IndexLevel"]\
                                                                *abs(self.OpenIndexSpecificData["FinalExposure"]-self.IndexSpecificData.set_index("SpecificTicker")["FinalExposure"])\
                                                                *np.array(0.0003/3)                                                                
                                                                            
            self.OpenIndexSpecificData.reset_index()
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
                                                            
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.fn_MRETCloseComposition()
        self.fn_MRETCalculateIndexLevel()
        self.fn_MRETCalculateCloseCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]

    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_MRETOpenComposition()
        self.fn_MRETOpenIndexSpecificData()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]



class RVOLIndexCalculation():
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]    
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.IndexLevel = 0
        self.temp_df = pd.DataFrame()
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.temp_df = pd.DataFrame()
        
    def fn_RVOLCloseComposition(self):
        try:    
            self.log.processname_change("RVOLCloseComposition")
            print(self.OpenComposition["SpecificTicker"])
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")       
            
            
    def fn_RVOLCalculateIndexLevel(self):
        try:
            self.log.processname_change("RVOLCalculateIndexLevel")
            LastIndexLevel = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
            LastRTC = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"RebalancingTransactionCost"].values[0]
            SumProduct = 0
            for ticker in self.OpenComposition["SpecificTicker"]:
                if ticker.lower() != "cash":
                    print(ticker)
                    print(self.OpenComposition.loc[self.OpenComposition["SpecificTicker"]==ticker, "Units"])
                    print(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"] == ticker) & np.array(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                    print(float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"] == ticker) & np.array(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"]))
                    SumProduct += float(self.OpenComposition.loc[self.OpenComposition["SpecificTicker"]==ticker, "Units"]) \
                                 *(float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"] == ticker) & np.array(self.DailyPrices["PriceDate"]==self.RunDate),"Price"]) \
                                    - float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"] == ticker) & np.array(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"]))
                                             
            self.IndexLevel = LastIndexLevel + SumProduct - LastRTC
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    def fn_RVOLCalculateCloseCashUnits(self):
        try:
            self.log.processname_change("RVOLCalculateCloseCashUnits")
            sumproduct=0
            for ticker in self.NewCloseComposition["SpecificTicker"]:
                UnderlyingUnits = float(self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==ticker,"Units"])
    #            print(float(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,ticker]))
                UnderlyingPriceCCYRun = float(self.DailyPrices.loc[np.array(self.DailyPrices["SpecificTicker"]==ticker) & np.array(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                sumproduct+=UnderlyingUnits*UnderlyingPriceCCYRun
                
            CashUnits=self.IndexLevel-sumproduct
            print(CashUnits)
            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("cash")])
            for instruments in self.NewCloseComposition["SpecificTicker"]:
                if instruments.lower()[:2]=="ca":
                    self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")              

                
                                                            
    def fn_RVOLOpenIndexSpecificData(self):
        try:
            self.log.processname_change("RVOLOpenIndexSpecificData")
            self.temp_df= self.IndexSpecificData.copy()
            self.temp_df["PriceDate"] = self.RunDate
            self.temp_df["IndexLevel"] = self.IndexLevel
            STSlope = float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == "UX2") & np.array(self.DailyPrices["PriceDate"]==self.RunDate), "Price"]) \
                      - float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == "UX1") & np.array(self.DailyPrices["PriceDate"]==self.RunDate), "Price"])
            MTSlope = float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == "UX7") & np.array(self.DailyPrices["PriceDate"]==self.RunDate), "Price"]) \
                      - float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == "UX4") & np.array(self.DailyPrices["PriceDate"]==self.RunDate), "Price"])
            self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Slope"] = STSlope
            self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Slope"] = MTSlope
            Signal = STSlope - MTSlope

#            self.temp_df.rename(columns = {"Weights" : "LastWeights", "Units" : "LastUnits"}, inplace = True)
            LastSignal = self.IndexSpecificData["Signal"].values[0]
            if LastSignal < -3:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = 0.3
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = -0.3
            elif LastSignal < -2:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = 0.2
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = -0.2
            elif LastSignal < 0:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = 0.1
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = -0.1
            elif LastSignal < 2:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = -0.1
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = 0.1
            elif LastSignal < 3:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = -0.2
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = 0.2
            else:
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXSP","Target"] = -0.3
                self.temp_df.loc[self.temp_df["GenericTicker"]=="SPVXMP","Target"] = 0.3

            self.temp_df.rename(columns = {"Weights" : "LastWeights", "Units" : "LastUnits", "Signal" : "LastSignal", "RebalancingTransactionCost" : "LastRebalancingTransactionCost"}, inplace = True)
            SumCost = 0    
            for instruments in self.temp_df["GenericTicker"]:
                
                self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Signal"] = Signal
                self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Weights"] = float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "LastWeights"]) \
                                                                                  + min(float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "CutOffWeight"]) \
                                                                                        , float(abs(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Target"] - self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "LastWeights"]))) \
                                                                                    *np.sign(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Target"] - self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "LastWeights"])
                self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Units"] = float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Weights"]) \
                                                                                          *self.IndexLevel \
                                                                                          / float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == instruments) & np.array(self.DailyPrices["PriceDate"] == self.RunDate), "Price"])
                self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Cost"] = float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Fee"]) \
                                                                                          * abs(float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == instruments) & np.array(self.DailyPrices["PriceDate"] == self.RunDate), "Price"]) \
                                                                                                 * float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Units"]) \
                                                                                                 - float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == instruments) & np.array(self.DailyPrices["PriceDate"] == self.LastRunDate), "Price"]) \
                                                                                                 * float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "LastUnits"]))
#                print(instruments)
#                print(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Cost"])
                SumCost += float(self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Cost"])
                self.temp_df.loc[self.temp_df["GenericTicker"] == instruments, "Price"] = float(self.DailyPrices.loc[np.array(self.DailyPrices["GenericTicker"] == instruments) & np.array(self.DailyPrices["PriceDate"] == self.RunDate), "Price"])                                                                                           
            self.temp_df["RebalancingTransactionCost"] = SumCost  
#            self.OpenIndexSpecificData["SpecificTicker"] = self.temp_df.index
            self.OpenIndexSpecificData = self.temp_df.loc[:, self.IndexSpecificData.columns]
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    


    def fn_RVOLOpenComposition(self):
        try:
            self.log.processname_change("RVOLOpenComposition")
            self.NewOpenComposition = self.OpenComposition.copy()
            self.NewOpenComposition["Date"] = self.RunDate
            
            for ticker in self.NewOpenComposition["SpecificTicker"]:
                if "ca" in ticker.lower():
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==ticker,"Units"] = self.IndexLevel- sum(self.temp_df["Units"]*self.temp_df["Price"])
                else:
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==ticker,"Units"] = float(self.temp_df.loc[self.temp_df["GenericTicker"]==ticker,"Units"])
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.fn_RVOLCloseComposition()
        self.fn_RVOLCalculateIndexLevel()
        self.fn_RVOLCalculateCloseCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]

    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_RVOLOpenIndexSpecificData()
        self.fn_RVOLOpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]                
            
            

class CUBESDIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"] = pd.to_datetime(self.DailyPrices["SettlementDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["RebalanceDate"] = pd.to_datetime(self.IndexSpecificData["RebalanceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])        
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.PotentialEligibleContracts =  cl_FileProcessor.dict_FileData["VTPotentialEligibleContracts"]
        self.PotentialEligibleContracts["Date"] = pd.to_datetime(self.PotentialEligibleContracts["Date"])
        self.PriorRollPrices = cl_FileProcessor.dict_FileData["VTPriorRollPrices"]
        self.PriorRollPrices["PriceDate"] = pd.to_datetime(self.PriorRollPrices["PriceDate"])
        self.PriorRollPrices["SettlementDate"] = pd.to_datetime(self.PriorRollPrices["SettlementDate"])
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.RebalFlag = 0
        self.RebalDate = self.IndexSpecificData["RebalanceDate"][0]
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.df_PEFC = pd.DataFrame()
        self.log = cl_FileProcessor.log


    def fn_CUBESDRebalCheck(self):
        try:
            self.log.processname_change("CUBESDRebalCheck")
            if self.RunDate.month != self.OpenDate.month:
                self.RebalFlag = 1
                self.RebalDate = self.RunDate
                print(self.RebalDate)
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")


    def fn_CUBESDCloseComposition(self):
        try:    
            self.log.processname_change("CUBESDCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

            
            
    def fn_HelperMonthCount(self, date1, date2):
        if date1.year == date2.year:
            MonthCount = date1.month -  date2.month
        else:
            MonthCount = 12 + date1.month -  date2.month
        return MonthCount
    
        
    def fn_HelperHypotheticalPrice(self, tempdf, SettlementDate, PriceFFC, HFCMonth, SubsetIndex):
        df = tempdf.copy()
        df.set_index("SpecificInstrumentVTId", inplace = True)
        PricePFC = float(df.loc[df["SettlementDate"]<SettlementDate,"Price"][-SubsetIndex])
        SettlementDatePFC = df.loc[df["SettlementDate"]<SettlementDate,"SettlementDate"][-SubsetIndex]
        
        SettlementDateHFC = SettlementDatePFC + relativedelta.relativedelta(months=HFCMonth)

        MonthsPFC = self.fn_HelperMonthCount(SettlementDateHFC, SettlementDatePFC)
        MonthFFC = self.fn_HelperMonthCount(SettlementDate, SettlementDatePFC)        
        PricePrevEFC = PricePFC + (MonthsPFC/MonthFFC)*(PriceFFC - PricePFC)
        return PricePrevEFC, SettlementDateHFC
        
    
    def fn_FinalPriceCalculator(self,df):
        df.sort_values(by = "SettlementDate", inplace = True)
        for date in df["SettlementDate"]:
            if date == min(df["SettlementDate"]):
                df.loc[df["SettlementDate"]==date,"IsNearest"] = 1
                NearestSettlement = date
            else:
                df.loc[df["SettlementDate"]==date,"IsNearest"] = 0
    
        df["MonthCount"] = df.apply(lambda x: self.fn_HelperMonthCount(x["SettlementDate"], NearestSettlement), axis = 1)
        df.reset_index(inplace = True)
        
        temp_df_PEFC = pd.DataFrame(columns = df.columns)
        temp_df_PEFC.loc[0,:] = df.loc[0,:]
        temp_df_PEFC.loc[0,"FinalPrice"] = float(df.loc[0,"Price"])
        temp_df_PEFC.loc[0,"FinalSettlement"] = df.loc[0,"SettlementDate"]
        
        for i in range(1,len(df)):
            MonthDifference = abs(df.ix[i-1,"MonthCount"]-df.ix[i,"MonthCount"])
            if MonthDifference != 1:
                SettlementDate = df.ix[i,"SettlementDate"]
                PriceFFC = df.ix[i,"Price"]
                for months in range(1,MonthDifference+1):
                    if months < MonthDifference:
                        length = len(temp_df_PEFC)
                        temp_df_PEFC.loc[length,"FinalPrice"],temp_df_PEFC.loc[length,"FinalSettlement"] = self.fn_HelperHypotheticalPrice(df, SettlementDate, PriceFFC, months, 1)
                        temp_df_PEFC.loc[length,"IsNearest"] = 0
                    else:
                        length = len(temp_df_PEFC)                                
                        temp_df_PEFC.loc[length,"FinalPrice"] = float(df.ix[i,"Price"])
                        temp_df_PEFC.loc[length,"FinalSettlement"] = df.ix[i,"SettlementDate"]
                        temp_df_PEFC.loc[length,"SpecificInstrumentVTId"] = df.ix[i,"SpecificInstrumentVTId"]
                        temp_df_PEFC.loc[length,"IsNearest"] = 0
            else:
                length = len(temp_df_PEFC)                                
                temp_df_PEFC.loc[length,"FinalPrice"] = float(df.ix[i,"Price"])
                temp_df_PEFC.loc[length,"FinalSettlement"] = df.ix[i,"SettlementDate"]
                temp_df_PEFC.loc[length,"SpecificInstrumentVTId"] = df.ix[i,"SpecificInstrumentVTId"]
                temp_df_PEFC.loc[length,"IsNearest"] = 0            
        return temp_df_PEFC
        
        
    def fn_CUBESDConstituentSelection(self):
        try:
            self.log.processname_change("CUBESDConstituentSelection")
            if self.RebalFlag == 1:
                self.PotentialEligibleContracts.set_index("Date", inplace = True)
                self.df_PEFC = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,:].copy()
                self.df_PEFC.set_index("SpecificInstrumentVTId", inplace = True)
                for instruments in self.df_PEFC.index:
                    if instruments in list(self.PotentialEligibleContracts["InstrumentVTId"]):
                        self.df_PEFC.loc[instruments,"IsPotential"] = True
                    else:
                        self.df_PEFC.loc[instruments,"IsPotential"] = False
                self.PriorRollPrices.set_index("SpecificInstrumentVTId", inplace = True)
                if len(self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]) <= 3:
                    self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,"IsActive"] = True
                else:
                    self.df_PEFC.sort_values(by = "SettlementDate", inplace = True)
                    for date in self.df_PEFC["SettlementDate"]:
                        if date == min(self.df_PEFC["SettlementDate"]):
                            self.df_PEFC.loc[self.df_PEFC["SettlementDate"]==date,"IsNearest"] = 1
                            NearestSettlement = date
                        else:
                            self.df_PEFC.loc[self.df_PEFC["SettlementDate"]==date,"IsNearest"] = 0
#                    print(self.df_PEFC)
                    temp_df_PEFC = self.fn_FinalPriceCalculator(self.df_PEFC)
                    temp_df_PEFC["MonthCount"] = temp_df_PEFC.apply(lambda x: self.fn_HelperMonthCount(x["FinalSettlement"], NearestSettlement), axis = 1)
                    temp_df_PEFC["CRY"] = temp_df_PEFC["FinalPrice"].shift(1)/temp_df_PEFC["FinalPrice"]-1
                    
                    temp_df_prior = self.fn_FinalPriceCalculator(self.PriorRollPrices)
                    temp_df_PEFC["IsPotential"] = False
                
                    for instruments in self.df_PEFC["SpecificInstrumentVTId"]:
                        if instruments in list(temp_df_PEFC["SpecificInstrumentVTId"]):
                            temp_df_PEFC.loc[temp_df_PEFC["SpecificInstrumentVTId"]==instruments,"IsPotential"]=self.df_PEFC.loc[self.df_PEFC["SpecificInstrumentVTId"]==instruments,"IsPotential"].values[0]

                    temp_df_PEFC = temp_df_PEFC.loc[temp_df_PEFC["MonthCount"]<=max(temp_df_PEFC.loc[temp_df_PEFC["IsPotential"]==True,"MonthCount"]),:]
                    self.df_PEFC = self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]
                    temp_df_prior["CRY"] = temp_df_prior["FinalPrice"].shift(1)/temp_df_prior["FinalPrice"]-1

                    for settlementdates in temp_df_PEFC["FinalSettlement"]:
                        priorsettlement = settlementdates+relativedelta.relativedelta(months=-1)
                        if len(temp_df_prior.loc[(pd.DatetimeIndex(temp_df_prior["FinalSettlement"]).month==priorsettlement.month)&(pd.DatetimeIndex(temp_df_prior["FinalSettlement"]).year==priorsettlement.year),"CRY"])==0:
                            PRY = float('nan')
                        else:
                            PRY = float(temp_df_prior.loc[(pd.DatetimeIndex(temp_df_prior["FinalSettlement"]).month==priorsettlement.month)&(pd.DatetimeIndex(temp_df_prior["FinalSettlement"]).year==priorsettlement.year),"CRY"])

                        temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"PRY"] = PRY
                        temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"ARY"] = np.nanmean([float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"CRY"]), float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"PRY"])])
                        
                        if float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"IsNearest"]) != 1:
                            temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"EYF"] = float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"FinalPrice"])*np.prod(1+temp_df_PEFC.loc[temp_df_PEFC["MonthCount"]<float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates, "MonthCount"]),"ARY"].fillna(0))
                        else:
                            temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"EYF"] = float(temp_df_PEFC.loc[temp_df_PEFC["FinalSettlement"]==settlementdates,"FinalPrice"])
#                    print(temp_df_PEFC)        
                    for instruments in self.df_PEFC["SpecificInstrumentVTId"]:
                        if instruments in list(temp_df_PEFC["SpecificInstrumentVTId"]):
                            self.df_PEFC.loc[self.df_PEFC["SpecificInstrumentVTId"]==instruments,"EYF"] = float(temp_df_PEFC.loc[temp_df_PEFC["SpecificInstrumentVTId"]==instruments,"EYF"])
                    self.df_PEFC["EYFRank"] = self.df_PEFC["EYF"].rank(ascending = 1)
                    self.df_PEFC.loc[self.df_PEFC["EYFRank"].isin([1,2,3]),"IsActive"] = True
                    self.df_PEFC.loc[~(self.df_PEFC["EYFRank"].isin([1,2,3])),"IsActive"] = False
                    self.df_PEFC.set_index("SpecificInstrumentVTId", inplace = True)
#                    print(self.df_PEFC)
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                            

                
    def fn_HelperAEOBDCount(self, date):
        try:
            self.log.processname_change("HelperAEOBDCount")
            FirstBusinessDay = pd.to_datetime({"year" : [date.year], "month" : [date.month], "day" : [1]})[0]
            ActualHoliday = self.HolidayCalendar.loc[self.HolidayCalendar["IsActualHoliday"]==True,:]
            while (FirstBusinessDay in list(ActualHoliday["Date"])) or ((FirstBusinessDay.weekday() == 5) or (FirstBusinessDay.weekday() == 6)):
                FirstBusinessDay = FirstBusinessDay + dt.timedelta(1)
            offset = BMonthEnd()
            LastBusinessDay = offset.rollforward(date)
            AllExchangeHoliday = self.HolidayCalendar.loc[(pd.DatetimeIndex(self.HolidayCalendar["Date"]).month==date.month) & (pd.DatetimeIndex(self.HolidayCalendar["Date"]).year==date.year),:]
            AllExchangeHoliday = list(set(AllExchangeHoliday["Date"]))
            MonthBusinessDay = pd.DataFrame(columns = ["Date"])
            for i in range(int((LastBusinessDay - FirstBusinessDay).days+1)):
                newdate = FirstBusinessDay+dt.timedelta(i)
                MonthBusinessDay.loc[i,"Date"]=newdate
            MonthBusinessDay = MonthBusinessDay.loc[~(MonthBusinessDay["Date"].isin(list(ActualHoliday["Date"])))]
            MonthBusinessDay = MonthBusinessDay.loc[~((MonthBusinessDay["Date"].dt.dayofweek==5)|(MonthBusinessDay["Date"].dt.dayofweek==6))]
            AEOBD = 0
            
            for dates in MonthBusinessDay["Date"]:
                if dates not in AllExchangeHoliday:
                    AEOBD+=1
                if date==dates:
                    break

            return(AEOBD)
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                            
        
    
    def fn_CUBESDCalculateIndexLevel(self):
        try:
            self.log.processname_change("CUBESDCalculateIndexLevel")
            LastIndexLevel = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
            AEOBDCount = self.fn_HelperAEOBDCount(self.RunDate)
            ActiveConstituents = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActive"]==1),"SpecificInstrumentVTId"]
            ActiveConstituentsLastRebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"]
            PriceSumRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituents)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            PriceSumRDLastRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituents)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"])
            PriceSumLastRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            PriceSumLastRDLastRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"])
            if AEOBDCount <= 1:
                DER = PriceSumLastRDRun/PriceSumLastRDLastRun-1
            elif AEOBDCount <= 2:
                DER = ((2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal))+(1/3*PriceSumRDRun/len(ActiveConstituents)))\
                        /((2/3*PriceSumLastRDLastRun/len(ActiveConstituentsLastRebal))+(1/3*PriceSumRDLastRun/len(ActiveConstituents)))\
                        -1
            elif AEOBDCount <= 3:
                DER = ((1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal))+(2/3*PriceSumRDRun/len(ActiveConstituents)))\
                        /((1/3*PriceSumLastRDLastRun/len(ActiveConstituentsLastRebal))+(2/3*PriceSumRDLastRun/len(ActiveConstituents)))\
                        -1
            elif AEOBDCount > 3:
                DER = PriceSumRDRun/PriceSumRDLastRun-1
            
            self.IndexLevel = LastIndexLevel*(1+DER)
            print("Index Level = "+str(self.IndexLevel))
            
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
 
            
    def fn_CUBESDCalculateCloseCashUnits(self):
        try:
            self.log.processname_change("CUBESDCalculateCloseCashUnits")
            SumProduct=0
            for ticker in self.NewCloseComposition["InstrumentVTId"]:
                SumProduct += float(self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==ticker, "Units"]) \
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"] == ticker) & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                
            CashUnits=self.IndexLevel-SumProduct
            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("cash")])
            for instruments in self.NewCloseComposition["SpecificTicker"]:
                if instruments.lower()=="cash":
                    self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")              
            

    def fn_CUBESDOpenIndexSpecificData(self):
        try:
            self.log.processname_change("CUBESDOpenIndexSpecificData")
            self.temp_df= self.IndexSpecificData.copy()
            for genericVTID in self.temp_df["GenericInstrumentVTId"]:
                self.temp_df.loc[self.temp_df["GenericInstrumentVTId"]==genericVTID,"SpecificInstrumentVTId"]=self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"]==genericVTID)&(self.DailyPrices["PriceDate"]==self.RunDate),"SpecificInstrumentVTId"].item()
                self.temp_df.loc[self.temp_df["GenericInstrumentVTId"]==genericVTID,"SpecificTicker"]=self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"]==genericVTID)&(self.DailyPrices["PriceDate"]==self.RunDate),"SpecificTicker"].item()
            self.temp_df["PriceDate"] = self.RunDate
            self.temp_df["IndexLevel"] = self.IndexLevel

            self.IndexSpecificData.set_index("SpecificInstrumentVTId", inplace = True)            
            self.temp_df.set_index("SpecificInstrumentVTId", inplace = True)
            
            if self.RebalFlag == 1:
                self.temp_df["RebalanceDate"] = self.RebalDate
                for instrument in self.temp_df.index:
                    if instrument in list(self.IndexSpecificData.index):
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = self.IndexSpecificData.loc[instrument,"IsActive"]
                    else:
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = False
                    if instrument in self.df_PEFC.index:
                        self.temp_df.loc[instrument,"IsActive"] = self.df_PEFC.loc[instrument,"IsActive"] 
                    else:
                        self.temp_df.loc[instrument,"IsActive"] = False
            else:
                for instrument in self.temp_df.index:
                    if instrument in list(self.IndexSpecificData.index):

                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = self.IndexSpecificData.loc[instrument,"IsActiveLastRebal"]
                        self.temp_df.loc[instrument,"IsActive"] = self.IndexSpecificData.loc[instrument,"IsActive"]
                    else:
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = False
                        self.temp_df.loc[instrument,"IsActive"] = False
                        
            self.OpenIndexSpecificData = self.temp_df.loc[:, self.IndexSpecificData.columns]
            self.OpenIndexSpecificData.reset_index(inplace = True)
            self.OpenIndexSpecificData = self.OpenIndexSpecificData.loc[~(self.OpenIndexSpecificData["SpecificInstrumentVTId"].str.contains("Dummy")),:]            
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")   

            
    def fn_CUBESDOpenComposition(self):
        try:
            self.log.processname_change("CUBESDOpenComposition")
            AEOBDCount = self.fn_HelperAEOBDCount(self.OpenDate)
            ActiveConstituents = list(self.OpenIndexSpecificData.loc[(self.OpenIndexSpecificData["PriceDate"]==self.RunDate)&(self.OpenIndexSpecificData["IsActive"]==1),"SpecificInstrumentVTId"])
            ActiveConstituentsLastRebal = list(self.OpenIndexSpecificData.loc[(self.OpenIndexSpecificData["PriceDate"]==self.RunDate)&(self.OpenIndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"])
            PriceSumRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(ActiveConstituents))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            PriceSumLastRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(ActiveConstituentsLastRebal))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
            if AEOBDCount <= 1:

                self.NewOpenComposition["InstrumentVTId"] = list(ActiveConstituentsLastRebal)
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]                    
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments, "Units"] = self.IndexLevel/PriceSumLastRDRun
                
            elif AEOBDCount <= 2:

                temp_list = ActiveConstituentsLastRebal.copy()
                temp_list.extend(ActiveConstituents)
                self.NewOpenComposition["InstrumentVTId"] = list(set(temp_list))
                
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]
                self.NewOpenComposition["Units"] = 0
                for instruments in list(ActiveConstituentsLastRebal):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*2/3*1/len(ActiveConstituentsLastRebal))/(2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+1/3*PriceSumRDRun/len(ActiveConstituents))
                for instruments in list(ActiveConstituents):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*1/3*1/len(ActiveConstituents))/(2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+1/3*PriceSumRDRun/len(ActiveConstituents))                
            elif AEOBDCount <= 3:
                temp_list = ActiveConstituentsLastRebal.copy()
                temp_list.extend(ActiveConstituents)
                
                self.NewOpenComposition["InstrumentVTId"] = list(set(temp_list))
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]
                self.NewOpenComposition["Units"] = 0
                for instruments in list(ActiveConstituentsLastRebal):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*1/3*1/len(ActiveConstituentsLastRebal))/(1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+2/3*PriceSumRDRun/len(ActiveConstituents))
                for instruments in list(ActiveConstituents):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*2/3*1/len(ActiveConstituents))/(1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+2/3*PriceSumRDRun/len(ActiveConstituents))                
            elif AEOBDCount > 3:
                self.NewOpenComposition["InstrumentVTId"] = list(ActiveConstituents)
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]                    
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments, "Units"] = self.IndexLevel/PriceSumRDRun


            SumProduct=0
            for ticker in self.NewOpenComposition["InstrumentVTId"]:
                SumProduct += float(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==ticker, "Units"]) \
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"] == ticker) & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                
            CashUnits=self.IndexLevel-SumProduct
            self.NewOpenComposition = self.NewOpenComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("cash")])
            for instruments in self.OpenComposition["SpecificTicker"]:
                if instruments.lower()=="cash":
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewOpenComposition["Date"] = self.RunDate
            self.NewOpenComposition["IndexVTId"] = self.IndexVTId
            
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])        
    def fn_VTOpenComp(self):
        self.fn_CUBESDRebalCheck()
        self.fn_CUBESDConstituentSelection()
        self.fn_CUBESDOpenIndexSpecificData()
        self.fn_CUBESDOpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
            
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_CUBESDCloseComposition()
        self.fn_CUBESDCalculateIndexLevel()
        self.fn_CUBESDCalculateCloseCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]             
                                               

class CUBESDTRIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"] = pd.to_datetime(self.DailyPrices["SettlementDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])        
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.UnderlyingCloseComposition = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.UnderlyingOpenComposition = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.df_PEFC = pd.DataFrame()
        self.log = cl_FileProcessor.log
            
 
    def fn_CUBESDTRCloseComposition(self):
        try:
            self.log.processname_change("CUBESDTRCloseComposition")
            self.NewCloseComposition = self.UnderlyingCloseComposition.loc[:,self.OpenComposition.columns].copy()
            self.UnderlyingCloseComposition.set_index("InstrumentVTId", inplace = True)
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            ERILLastRun = float(self.DailyPrices.loc[(self.DailyPrices["IndexVTId"]==self.IndexVTId)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"])
            print("ERILLastRun = "+str(ERILLastRun))
            print(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"])
            TRILLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"])
            RateLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"FundingRate"])
            for instruments in self.NewCloseComposition.index:
                self.NewCloseComposition.loc[instruments,"Units"] = self.UnderlyingCloseComposition.loc[instruments,"Units"]\
                                                                    *(TRILLastRun/ERILLastRun*(1/(1-91/360*RateLastRun/100))**((self.RunDate-self.LastRunDate).days/91))
            
            self.NewCloseComposition["Date"] = self.RunDate
            self.NewCloseComposition["IndexVTId"] = self.IndexVTId
            print(self.NewCloseComposition)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    

            
    def fn_CUBESDTRCalculateIndexLevel(self):
        try:
            self.log.processname_change("CUBESDTRCalculateIndexLevel")        
            ERILRun = float(self.DailyPrices.loc[(self.DailyPrices["IndexVTId"]==self.IndexVTId)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])            
            ERILLastRun = float(self.DailyPrices.loc[(self.DailyPrices["IndexVTId"]==self.IndexVTId)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"])
            TRILLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"])
            RateLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"FundingRate"])
            TBRd = ((1/(1-91/360*RateLastRun/100))**(1/91))-1
            self.IndexLevel = TRILLastRun*((ERILRun/ERILLastRun)+TBRd)*(1 + TBRd)**((self.RunDate-self.LastRunDate).days-1)
            SumProduct = 0
            for instruments in self.NewCloseComposition.index:
                if self.NewCloseComposition.loc[instruments,"SpecificTicker"].lower()!="cash":
                    SumProduct += self.NewCloseComposition.loc[instruments,"Units"]\
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==instruments)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            CashUnits = self.IndexLevel - SumProduct
            
            for instruments in  self.NewCloseComposition.index:
                if self.NewCloseComposition.loc[instruments,"SpecificTicker"].lower()=="cash":
                    self.NewCloseComposition.loc[instruments,"Units"] +=CashUnits            
#            print(self.NewCloseComposition)
            self.NewCloseComposition.reset_index(inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")    
            
            
    def fn_CUBESDTROpenIndexSpecificData(self):
        try:
            self.log.processname_change("CUBESDTROpenIndexSpecificData")
#            print(list(self.OpenIndexSpecificData["PriceDate"])==self.RunDate)
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"]==self.RunDate,"IndexLevel"] = self.IndexLevel
            self.OpenIndexSpecificData = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["PriceDate"]==self.RunDate,:]
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    def fn_CUBESDTROpenComposition(self):
        try:
            self.log.processname_change("CUBESDTROpenComposition")
            self.NewOpenComposition = self.UnderlyingOpenComposition.loc[:,self.OpenComposition.columns].copy()
            self.UnderlyingOpenComposition.set_index("InstrumentVTId", inplace = True)
            self.NewOpenComposition.set_index("InstrumentVTId", inplace = True)            
            ERILRun = float(self.DailyPrices.loc[(self.DailyPrices["IndexVTId"]==self.IndexVTId)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            TRILRun = self.IndexLevel
            RateRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.RunDate,"FundingRate"])
            for instruments in self.NewOpenComposition.index:
                self.NewOpenComposition.loc[instruments,"Units"] = self.UnderlyingOpenComposition.loc[instruments,"Units"]\
                                                                    *(TRILRun/ERILRun*(1/(1-91/360*RateRun/100))**((self.OpenDate-self.RunDate).days/91))
            self.NewOpenComposition["Date"] = self.RunDate
            self.NewOpenComposition["IndexVTId"] = self.IndexVTId
            SumProduct = 0
            for instruments in self.NewOpenComposition.index:
                if self.NewOpenComposition.loc[instruments,"SpecificTicker"].lower()!="cash":
                    SumProduct += self.NewOpenComposition.loc[instruments,"Units"]\
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==instruments)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            CashUnits = self.IndexLevel - SumProduct
            
            for instruments in  self.NewOpenComposition.index:
                if self.NewOpenComposition.loc[instruments,"SpecificTicker"].lower()=="cash":
                    self.NewOpenComposition.loc[instruments,"Units"] +=CashUnits            
            
            self.NewOpenComposition.reset_index(inplace = True)

            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
           
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.fn_CUBESDTRCloseComposition()
        self.fn_CUBESDTRCalculateIndexLevel()
        print("New Close Composition created")
        return [self.NewCloseComposition]        
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_CUBESDTROpenIndexSpecificData()
        self.fn_CUBESDTROpenComposition()
        return [self.NewOpenComposition, self.OpenIndexSpecificData]             
            

class CitiMomentumIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"] = pd.to_datetime(self.DailyPrices["SettlementDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["RebalanceDate"] = pd.to_datetime(self.IndexSpecificData["RebalanceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])        
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.PotentialEligibleContracts =  cl_FileProcessor.dict_FileData["VTPotentialEligibleContracts"]
        self.PotentialEligibleContracts["Date"] = pd.to_datetime(self.PotentialEligibleContracts["Date"])
        self.PriorRollPrices = cl_FileProcessor.dict_FileData["VTPriorRollPrices"]
        self.PriorRollPrices["PriceDate"] = pd.to_datetime(self.PriorRollPrices["PriceDate"])
        self.PriorRollPrices["SettlementDate"] = pd.to_datetime(self.PriorRollPrices["SettlementDate"])
        self.PriorRollPrices = self.PriorRollPrices.loc[(self.PriorRollPrices["PriceDate"]>=self.IndexSpecificData["RebalanceDate"].values[0]) & (self.PriorRollPrices["PriceDate"]<=self.RunDate) & (~(self.PriorRollPrices["PriceDate"].isin(list(self.HolidayCalendar.loc[self.HolidayCalendar["IsActualHoliday"]==True,:]["Date"])))),:]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.RebalFlag = 0
        self.RebalDate = self.IndexSpecificData["RebalanceDate"][0]
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.df_PEFC = pd.DataFrame()
        self.log = cl_FileProcessor.log
        self.IsMarket = self.PotentialEligibleContracts.loc[(self.PotentialEligibleContracts["IsMarketContract"]==True) & (self.PotentialEligibleContracts["Date"]==self.RunDate),:]["InstrumentVTId"].values[0]
        self.df_Market = self.PriorRollPrices.loc[self.PriorRollPrices["SpecificInstrumentVTId"]==self.IsMarket,:][["SpecificInstrumentVTId","PriceDate","Price"]]
        self.df_Market.set_index("SpecificInstrumentVTId", inplace = True)
        self.df_Market.sort_values(by=["PriceDate"],inplace=True)
#        print(self.df_Market)


    def fn_CitiMomentumRebalCheck(self):
        try:
            self.log.processname_change("CitiMomentumRebalCheck")
            if self.RunDate.month != self.OpenDate.month:
                self.RebalFlag = 1
                self.RebalDate = self.RunDate             
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")


    def fn_CitiMomentumCloseComposition(self):
        try:    
            self.log.processname_change("CitiMomentumCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

                    
    def fn_HelperRegressionAlpha(self,df_Candidate):
        self.df_Market["DailyReturn"]=self.df_Market["Price"]/self.df_Market["Price"].shift(1)-1        
        df_Candidate["DailyReturn"]=df_Candidate["Price"]/df_Candidate["Price"].shift(1)-1
                
        X =self.df_Market["DailyReturn"][1:]
        y = df_Candidate["DailyReturn"][1:]
        lm = linear_model.LinearRegression()
        lm.fit(X.values.reshape(X.shape[0],1),y)
        lm.intercept_        
        return lm.intercept_

        
    def fn_CitiMomentumConstituentSelection(self):
        try:
            self.log.processname_change("CitiMomentumConstituentSelection")
            if self.RebalFlag == 1:
                self.PotentialEligibleContracts.set_index("Date", inplace = True)
                self.df_PEFC = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,:].copy()
                self.df_PEFC.set_index("SpecificInstrumentVTId", inplace = True)
#                print(IsMarket)
                for instruments in self.df_PEFC.index:
                    if instruments in list(self.PotentialEligibleContracts["InstrumentVTId"]):
                        self.df_PEFC.loc[instruments,"IsPotential"] = True
                    else:
                        self.df_PEFC.loc[instruments,"IsPotential"] = False
                self.PriorRollPrices.set_index("SpecificInstrumentVTId", inplace = True)
#                print(len(self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]))
                self.df_PEFC["IsActive"]=False
                if len(self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]) == 1:
                    self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,"IsActive"] = True
                elif len(self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]) == 0:
                    for instruments in list(self.df_PEFC.index):
                        if instruments == self.IsMarket:
                            self.df_PEFC.loc[instruments,"IsActive"] = True
                        else:
                            self.df_PEFC.loc[instruments,"IsActive"] = False 
                elif len(self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]) > 1:
                    self.df_PEFC = self.df_PEFC.loc[self.df_PEFC["IsPotential"]==True,:]
                    for instruments in list(self.df_PEFC.index):
                        if len(self.PriorRollPrices.loc[self.PriorRollPrices.index==instruments,:])>=15:
                            df_Candidate = self.PriorRollPrices.loc[self.PriorRollPrices.index==instruments,:][["PriceDate","Price"]]
                            df_Candidate.sort_values(by=["PriceDate"],inplace=True)
                            self.df_PEFC.loc[self.df_PEFC.index==instruments,"Alpha"]=self.fn_HelperRegressionAlpha(df_Candidate)
                    self.df_PEFC["Rank"] = self.df_PEFC["Alpha"].rank(ascending = False)
                    self.df_PEFC.loc[self.df_PEFC["Rank"]==1,"IsActive"] = True
                    self.df_PEFC.loc[self.df_PEFC["Rank"]!=1,"IsActive"] = False
#                    self.df_PEFC.set_index("SpecificInstrumentVTId", inplace = True)
#                print(X)
#                print(y)
                
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                            

                
    def fn_HelperAEOBDCount(self, date):
        try:
            self.log.processname_change("HelperAEOBDCount")
            FirstBusinessDay = pd.to_datetime({"year" : [date.year], "month" : [date.month], "day" : [1]})[0]
            ActualHoliday = self.HolidayCalendar.loc[self.HolidayCalendar["IsActualHoliday"]==True,:]
            while (FirstBusinessDay in list(ActualHoliday["Date"])) or ((FirstBusinessDay.weekday() == 5) or (FirstBusinessDay.weekday() == 6)):
                FirstBusinessDay = FirstBusinessDay + dt.timedelta(1)
            offset = BMonthEnd()
            LastBusinessDay = offset.rollforward(date)
            AllExchangeHoliday = self.HolidayCalendar.loc[(pd.DatetimeIndex(self.HolidayCalendar["Date"]).month==date.month) & (pd.DatetimeIndex(self.HolidayCalendar["Date"]).year==date.year),:]
            AllExchangeHoliday = list(set(AllExchangeHoliday["Date"]))
            MonthBusinessDay = pd.DataFrame(columns = ["Date"])
            for i in range(int((LastBusinessDay - FirstBusinessDay).days+1)):
                newdate = FirstBusinessDay+dt.timedelta(i)
                MonthBusinessDay.loc[i,"Date"]=newdate
            MonthBusinessDay = MonthBusinessDay.loc[~(MonthBusinessDay["Date"].isin(list(ActualHoliday["Date"])))]
            MonthBusinessDay = MonthBusinessDay.loc[~((MonthBusinessDay["Date"].dt.dayofweek==5)|(MonthBusinessDay["Date"].dt.dayofweek==6))]
            AEOBD = 0
            
            for dates in MonthBusinessDay["Date"]:
                if dates not in AllExchangeHoliday:
                    AEOBD+=1
                if date==dates:
                    break

            return(AEOBD)
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                            
        
    
    def fn_CitiMomentumCalculateIndexLevel(self):
        try:
            self.log.processname_change("CitiMomentumCalculateIndexLevel")
            LastIndexLevel = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
            AEOBDCount = self.fn_HelperAEOBDCount(self.RunDate)
            ActiveConstituents = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActive"]==1),"SpecificInstrumentVTId"]
#            ActiveConstituentsLastRebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"]
#            print(ActiveConstituents)
            print(self.DailyPrices["SpecificInstrumentVTId"])
            print(ActiveConstituents)
            PriceSumRDRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituents)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
            PriceSumRDLastRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituents)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
#            PriceSumLastRDRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
#            PriceSumLastRDLastRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
            if AEOBDCount <= 1:
                ActiveConstituentsLastRebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"]                
                PriceSumLastRDRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
                PriceSumLastRDLastRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
                
                DER = PriceSumLastRDRun/PriceSumLastRDLastRun-1
            elif AEOBDCount == 2:
                ActiveConstituentsLastRebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"]                
                PriceSumLastRDRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
                PriceSumLastRDLastRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
                
                DER = ((2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal))+(1/3*PriceSumRDRun/len(ActiveConstituents)))\
                        /((2/3*PriceSumLastRDLastRun/len(ActiveConstituentsLastRebal))+(1/3*PriceSumRDLastRun/len(ActiveConstituents)))\
                        -1
            elif AEOBDCount == 3:
                ActiveConstituentsLastRebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"]                
                PriceSumLastRDRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
                PriceSumLastRDLastRun = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(list(ActiveConstituentsLastRebal)))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]                
                DER = ((1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal))+(2/3*PriceSumRDRun/len(ActiveConstituents)))\
                        /((1/3*PriceSumLastRDLastRun/len(ActiveConstituentsLastRebal))+(2/3*PriceSumRDLastRun/len(ActiveConstituents)))\
                        -1
            elif AEOBDCount > 3:
                DER = PriceSumRDRun/PriceSumRDLastRun-1
#            print(PriceSumRDRun)
#            print(AEOBDCount)
#            print(LastIndexLevel)
#            print(self.IndexLevel)
#            print(DER)
            self.IndexLevel = LastIndexLevel*(1+DER)
#            print("Heyyy" + str(len(self.IndexLevel)))
            print("Index Level = "+str(self.IndexLevel))
            
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
 
            
    def fn_CitiMomentumCalculateCloseCashUnits(self):
        try:
            self.log.processname_change("CitiMomentumCalculateCloseCashUnits")
            SumProduct=0
            for ticker in self.NewCloseComposition["InstrumentVTId"]:
                SumProduct += float(self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==ticker, "Units"]) \
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"] == ticker) & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                
            CashUnits=self.IndexLevel-SumProduct
            self.NewCloseComposition = self.NewCloseComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("cash")])
            for instruments in self.NewCloseComposition["SpecificTicker"]:
                if instruments.lower()=="cash":
                    self.NewCloseComposition.loc[self.NewCloseComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")              
            

    def fn_CitiMomentumOpenIndexSpecificData(self):
        try:
            self.log.processname_change("CitiMomentumOpenIndexSpecificData")
            self.temp_df= self.IndexSpecificData.copy()
            for genericVTID in self.temp_df["GenericInstrumentVTId"]:
                self.temp_df.loc[self.temp_df["GenericInstrumentVTId"]==genericVTID,"SpecificInstrumentVTId"]=self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"]==genericVTID)&(self.DailyPrices["PriceDate"]==self.RunDate),"SpecificInstrumentVTId"].item()
                self.temp_df.loc[self.temp_df["GenericInstrumentVTId"]==genericVTID,"SpecificTicker"]=self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"]==genericVTID)&(self.DailyPrices["PriceDate"]==self.RunDate),"SpecificTicker"].item()
            self.temp_df["PriceDate"] = self.RunDate
            self.temp_df["IndexLevel"] = self.IndexLevel

            self.IndexSpecificData.set_index("SpecificInstrumentVTId", inplace = True)            
            self.temp_df.set_index("SpecificInstrumentVTId", inplace = True)
            
            if self.RebalFlag == 1:
                self.temp_df["RebalanceDate"] = self.RebalDate
                for instrument in self.temp_df.index:
                    if instrument in list(self.IndexSpecificData.index):
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = self.IndexSpecificData.loc[instrument,"IsActive"]
                    else:
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = False
                    if instrument in self.df_PEFC.index:
                        self.temp_df.loc[instrument,"IsActive"] = self.df_PEFC.loc[instrument,"IsActive"] 
                    else:
                        self.temp_df.loc[instrument,"IsActive"] = False
            else:
                for instrument in self.temp_df.index:
                    if instrument in list(self.IndexSpecificData.index):

                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = self.IndexSpecificData.loc[instrument,"IsActiveLastRebal"]
                        self.temp_df.loc[instrument,"IsActive"] = self.IndexSpecificData.loc[instrument,"IsActive"]
                    else:
                        self.temp_df.loc[instrument,"IsActiveLastRebal"] = False
                        self.temp_df.loc[instrument,"IsActive"] = False
                        
            self.OpenIndexSpecificData = self.temp_df.loc[:, self.IndexSpecificData.columns]
            self.OpenIndexSpecificData.reset_index(inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")   

            
    def fn_CitiMomentumOpenComposition(self):
        try:
            self.log.processname_change("CitiMomentumOpenComposition")
            AEOBDCount = self.fn_HelperAEOBDCount(self.OpenDate)
            ActiveConstituents = list(self.OpenIndexSpecificData.loc[(self.OpenIndexSpecificData["PriceDate"]==self.RunDate)&(self.OpenIndexSpecificData["IsActive"]==1),"SpecificInstrumentVTId"])
            ActiveConstituentsLastRebal = list(self.OpenIndexSpecificData.loc[(self.OpenIndexSpecificData["PriceDate"]==self.RunDate)&(self.OpenIndexSpecificData["IsActiveLastRebal"]==1),"SpecificInstrumentVTId"])
            print(ActiveConstituentsLastRebal)
            PriceSumRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(ActiveConstituents))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            PriceSumLastRDRun = sum(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"].isin(ActiveConstituentsLastRebal))&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
            self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
            if AEOBDCount <= 1:

                self.NewOpenComposition["InstrumentVTId"] = list(ActiveConstituentsLastRebal)
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]                    
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments, "Units"] = self.IndexLevel/PriceSumLastRDRun
                
            elif AEOBDCount == 2:

                temp_list = ActiveConstituentsLastRebal.copy()
                temp_list.extend(ActiveConstituents)
                self.NewOpenComposition["InstrumentVTId"] = list(set(temp_list))
                
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]
                self.NewOpenComposition["Units"] = 0
                for instruments in list(ActiveConstituentsLastRebal):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*2/3*1/len(ActiveConstituentsLastRebal))/(2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+1/3*PriceSumRDRun/len(ActiveConstituents))
                for instruments in list(ActiveConstituents):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*1/3*1/len(ActiveConstituents))/(2/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+1/3*PriceSumRDRun/len(ActiveConstituents))                
            elif AEOBDCount == 3:
                temp_list = ActiveConstituentsLastRebal.copy()
                temp_list.extend(ActiveConstituents)
                
                self.NewOpenComposition["InstrumentVTId"] = list(set(temp_list))
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]
                self.NewOpenComposition["Units"] = 0
                for instruments in list(ActiveConstituentsLastRebal):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*1/3*1/len(ActiveConstituentsLastRebal))/(1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+2/3*PriceSumRDRun/len(ActiveConstituents))
                for instruments in list(ActiveConstituents):
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"Units"] += (self.IndexLevel*2/3*1/len(ActiveConstituents))/(1/3*PriceSumLastRDRun/len(ActiveConstituentsLastRebal)+2/3*PriceSumRDRun/len(ActiveConstituents))                
            elif AEOBDCount > 3:
                self.NewOpenComposition["InstrumentVTId"] = list(ActiveConstituents)
                for instruments in self.NewOpenComposition["InstrumentVTId"]:
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"SpecificTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"SpecificTicker"]
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"GenericTicker"] = self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["SpecificInstrumentVTId"]==instruments,"GenericTicker"]                    
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments, "Units"] = self.IndexLevel/PriceSumRDRun


            SumProduct=0
            for ticker in self.NewOpenComposition["InstrumentVTId"]:
                SumProduct += float(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==ticker, "Units"]) \
                                *float(self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"] == ticker) & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"])
                
            CashUnits=self.IndexLevel-SumProduct
            self.NewOpenComposition = self.NewOpenComposition.append(self.OpenComposition[self.OpenComposition["SpecificTicker"].str.lower().str.contains("cash")])
            for instruments in self.OpenComposition["SpecificTicker"]:
                if instruments.lower()=="cash":
                    self.NewOpenComposition.loc[self.NewOpenComposition["SpecificTicker"]==instruments,"Units"] = CashUnits
            self.NewOpenComposition["Date"] = self.RunDate
            self.NewOpenComposition["IndexVTId"] = self.IndexVTId
            
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")      
            
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_CitiMomentumCloseComposition()
        self.fn_CitiMomentumCalculateIndexLevel()
        self.fn_CitiMomentumCalculateCloseCashUnits()
        print("New Close Composition created")
        return [self.NewCloseComposition]        
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_CitiMomentumRebalCheck()
        self.fn_CitiMomentumConstituentSelection()
        self.fn_CitiMomentumOpenIndexSpecificData()
        self.fn_CitiMomentumOpenComposition() 
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
                                               
            

class CitiBCOMIndexCalculation:
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["RebalanceDate"] = pd.to_datetime(self.IndexSpecificData["RebalanceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])        
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.BCOMWeights = cl_FileProcessor.dict_FileData["VTBCOMWeights"]
        self.BCOMWeights["Date"] = pd.to_datetime(self.BCOMWeights["Date"])
        self.PriorRollPrices = cl_FileProcessor.dict_FileData["VTPriorRollPrices"]
        self.PriorRollPrices = self.PriorRollPrices.pivot_table(index = "PriceDate", columns = "SpecificInstrumentVTId", values = "Price").reset_index()
        self.PriorRollPrices["PriceDate"] = pd.to_datetime(self.PriorRollPrices["PriceDate"])
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.RebalFlag = 0
        self.RebalDate = self.IndexSpecificData["RebalanceDate"][0]
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.IndexLevel = 0
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
            

    def fn_CitiBCOMRebalCheck(self):
        try:
            self.log.processname_change("CitiBCOMRebalCheck")
            if self.RunDate.month != self.LastRunDate.month:
                self.RebalDate = self.RunDate
                while (self.RebalDate in list(set(self.HolidayCalendar["Date"]))) or ((self.RebalDate.weekday() == 5) or (self.RebalDate.weekday() == 6)):
                    self.RebalDate = self.RebalDate + dt.timedelta(1)
                    
            if self.RebalDate == self.RunDate:
                self.RebalFlag = 1
            print(self.RebalDate)
            print(self.RebalFlag)
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            

    def fn_CitiBCOMCloseComposition(self):
        try:    
            self.log.processname_change("CitiBCOMCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            PricesRun = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,:].set_index("GenericInstrumentVTId")
            PricesLastRun = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.LastRunDate,:].set_index("GenericInstrumentVTId")
            IndexLevelLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"])

            self.IndexLevel = IndexLevelLastRun+sum(self.NewCloseComposition["Units"]*(PricesRun["Price"]-PricesLastRun["Price"]))
            print("self.IndexLevel = ",self.IndexLevel)
            
            CashUnits = self.IndexLevel - sum(self.NewCloseComposition["Units"]*PricesRun["Price"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca"),:].copy()
            df_Cash.loc[:,"Units"] = CashUnits
            self.NewCloseComposition.reset_index(inplace = True)
            self.NewCloseComposition = self.NewCloseComposition.append(df_Cash)
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")



    def fn_CitiBCOMRebalComposition(self):
        try:    
            self.log.processname_change("CitiBCOMRebalComposition")
            self.PriorRollPrices = self.PriorRollPrices.loc[(pd.DatetimeIndex(self.PriorRollPrices["PriceDate"]).month != self.RunDate.month),:]
            temp_df = pd.DataFrame(columns = self.PriorRollPrices.columns)
            self.PriorRollPrices = self.PriorRollPrices.sort_values("PriceDate").reset_index(drop = True)
            temp_df["PriceDate"] = self.PriorRollPrices["PriceDate"].copy()
            weight_df = self.BCOMWeights.pivot_table(index = "Date", columns = "AssociatedCommodity", values = "BankWeight").reset_index().copy()
            new_weight_df = pd.DataFrame(columns = weight_df.columns)
            new_weight_df["Date"] = list(self.PriorRollPrices.loc[~(self.PriorRollPrices["PriceDate"].isin(list(weight_df["Date"]))),"PriceDate"])
            new_weight_df = new_weight_df.append(weight_df)
            new_weight_df = new_weight_df.sort_values("Date").reset_index(drop = True)
            new_weight_df.fillna(method = 'pad', inplace = True)
            for ticker in self.PriorRollPrices.columns:
                if ticker != "PriceDate":
                    commodity = list(self.DailyPrices.loc[self.DailyPrices["GenericInstrumentVTId"]==ticker,"UnderlyingCommodity"]).pop(0)
                    if commodity not in list(new_weight_df.columns):
                        new_weight_df.loc[:,commodity]=0
                    temp_df[ticker] = (self.PriorRollPrices[ticker]/self.PriorRollPrices[ticker].shift(1) - 1)*new_weight_df[commodity].shift(1)
                    
            
            BenchmarkGroup = temp_df.columns[temp_df.columns.str.contains("OM")]
            CitiGroup = temp_df.columns[(~(temp_df.columns.str.contains("OM")))&(~(temp_df.columns.str.contains("PriceDate")))]
            temp_df.to_csv("1234.csv")
            regression_df = pd.DataFrame(columns = ["PriceDate", "CitiGroup", "BenchmarkGroup"])
            temp_df.dropna(inplace = True)
            regression_df["PriceDate"] = temp_df.loc[:,"PriceDate"]
            regression_df["CitiGroup"] = temp_df[CitiGroup].sum(axis = 1)
            regression_df["BenchmarkGroup"] = temp_df[BenchmarkGroup].sum(axis = 1)
            X =regression_df["BenchmarkGroup"]
            y = regression_df["CitiGroup"]
#            regression_df.to_csv("regression_df.csv")
            lm = linear_model.LinearRegression()
            lm.fit(X.values.reshape(X.shape[0],1),y)
            RealizedBeta = min(max(float(lm.coef_),0.7),1)*-1
            print(regression_df)
            
            RebalCompostition = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["IndexVTId"]==self.IndexVTId),:].copy()
            RebalCompostition["BetaMultiplier"] = RebalCompostition["GenericInstrumentVTId"].map(lambda x: RealizedBeta if "OM" in x else 1)
#            RebalCompostition.to_csv("RebalCompostition.csv")
#            new_weight_df.to_csv("new_weight_df.csv")
            for commodity in RebalCompostition["UnderlyingCommodity"]:
#                print(new_weight_df.iloc[-1,:][commodity])
                RebalCompostition.loc[RebalCompostition["UnderlyingCommodity"]==commodity,"AssociatedCommodityWeight"] = float(new_weight_df.iloc[-1,:][commodity])
            RebalCompostition["Weight"] = RebalCompostition["AssociatedCommodityWeight"]*RebalCompostition["BetaMultiplier"]
            RebalCompostition["Units"] = RebalCompostition["Weight"]*self.IndexLevel/RebalCompostition["Price"]

            self.NewOpenComposition = self.OpenComposition.loc[~(self.OpenComposition["InstrumentVTId"].str.contains("Ca:")),:]
            self.NewOpenComposition.set_index("InstrumentVTId", inplace = True)
            RebalCompostition.set_index("GenericInstrumentVTId", inplace = True)
            self.NewOpenComposition["Units"] = RebalCompostition["Units"]
            self.NewOpenComposition["Price"] = RebalCompostition["Price"]
            self.NewOpenComposition.reset_index(inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    def fn_CitiBCOMOpenComposition(self):
        try:    
            self.log.processname_change("CitiBCOMOpenComposition")
            if self.RebalFlag == 1:
                self.fn_CitiBCOMRebalComposition()
            else:
                self.NewOpenComposition = self.OpenComposition.loc[~(self.OpenComposition["InstrumentVTId"].str.contains("Ca:")),:]
                
            df_Cash = self.OpenComposition.loc[self.OpenComposition["InstrumentVTId"].str.contains("Ca:"),:]
            df_Cash.loc[:,"Units"] = self.IndexLevel - sum(self.NewOpenComposition["Price"]*self.NewOpenComposition["Units"])
            self.NewOpenComposition = self.NewOpenComposition.append(df_Cash)
            self.NewOpenComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
    
    def fn_CitiBCOMOpenIndexSpecificData(self):
        try:    
            self.log.processname_change("CitiBCOMOpenIndexSpecificData")
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            self.OpenIndexSpecificData.loc[:,"RebalanceDate"] = self.RebalDate
            self.OpenIndexSpecificData.loc[:,"IndexLevel"] = self.IndexLevel
            self.OpenIndexSpecificData.loc[:,"PriceDate"] = self.RunDate

        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
        
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_CitiBCOMRebalCheck()
        self.fn_CitiBCOMCloseComposition()
        print("New Close Composition created")
        return [self.NewCloseComposition]        
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_CitiBCOMOpenComposition()
        self.fn_CitiBCOMOpenIndexSpecificData()
        print("New Open Composition created") 
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
            

class EquityIndexCalculation:
    
    def __init__(self,cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenCompositions = cl_FileProcessor.dict_FileData["OpenCompositions"]        
        self.CloseComposition = cl_FileProcessor.dict_FileData["CloseCompositions"]
        self.CorporateActions = cl_FileProcessor.dict_FileData["CorporateActions"]
        self.IndexCalculationDetails = cl_FileProcessor.dict_FileData["IndexCalculationDetails"]
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenCompositions.columns)
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenCompositions.columns)
        self.NewIndexCalculationDetails = self.IndexCalculationDetails
        self.IndexLevel = 0
        self.temp_df_CA = pd.DataFrame()
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        

    def fn_rebalCheckCloseComp(self):
        if self.IndexCalculationDetails.IsRebalance.values[0]:
            newComp = self.CloseComposition[~(self.CloseComposition.Units == 0)].InstrumentVTId
            newCompAdded = newComp[~(newComp.isin(self.NewCloseComposition.InstrumentVTId))]                                     
            df_newCompAdded = pd.DataFrame(columns = self.OpenCompositions.columns)
            df_newCompAdded.InstrumentVTId = newCompAdded
            df_newCompAdded.Units = 0
            df_newCompAdded.PriceDate = self.RunDate
            df_newCompAdded.IndexVTId = self.IndexVTId
            df_newCompAdded.IsDelisted = 0
            """To get price from Bank Composition for newly added instruments"""
            for instruments in newCompAdded:
                df_newCompAdded.loc[df_newCompAdded.InstrumentVTId==instruments, "PriceInInstrumentCurrency"] = float(self.CloseComposition.loc[self.CloseComposition.InstrumentVTId==instruments, "PriceInInstrumentCurrency"])
                df_newCompAdded.loc[df_newCompAdded.InstrumentVTId==instruments, "OpenPriceLocalCurrency"] = float(self.CloseComposition.loc[self.CloseComposition.InstrumentVTId==instruments, "PriceInInstrumentCurrency"])
                df_newCompAdded.loc[df_newCompAdded.InstrumentVTId==instruments, "FxRate"] = float(self.CloseComposition.loc[self.CloseComposition.InstrumentVTId==instruments, "FxRate"])
            self.NewCloseComposition = self.NewCloseComposition.append(df_newCompAdded).reset_index()
        else:
            pass
               

    def fn_rebalCheckOpenComp(self):
        if self.IndexCalculationDetails.IsRebalance.values[0]:
            oldComp = self.CloseComposition[~(self.CloseComposition.Units == 0)].InstrumentVTId
            oldCompDeleted = self.OpenCompositions[~(self.OpenCompositions.InstrumentVTId.isin(oldComp))].InstrumentVTId                                    
            df_newCompAdded = pd.DataFrame(columns = self.OpenCompositions.columns)
            df_newCompAdded.InstrumentVTId = oldCompDeleted
            df_newCompAdded.Units = 0
            df_newCompAdded.ShareAdjustmentFactor = 1
            df_newCompAdded.PriceAdjustmentFactor = 1
            df_newCompAdded.PriceDate = self.RunDate
            df_newCompAdded.IndexVTId = self.IndexVTId
            df_newCompAdded.IsDelisted = 0
            """To get price from Bank Composition for newly added instruments"""
            for instruments in oldCompDeleted:
                df_newCompAdded.loc[df_newCompAdded.InstrumentVTId==instruments, "PriceInInstrumentCurrency"] = float(self.OpenCompositions.loc[self.OpenCompositions.InstrumentVTId==instruments, "PriceInInstrumentCurrency"].values[0])
                df_newCompAdded.loc[df_newCompAdded.InstrumentVTId==instruments, "FxRate"] = float(self.OpenCompositions.loc[self.OpenCompositions.InstrumentVTId==instruments, "FxRate"].values[0])
            self.NewOpenComposition = self.NewOpenComposition.append(df_newCompAdded).reset_index()
            self.NewOpenComposition.set_index("InstrumentVTId", inplace = True)
            self.CloseComposition.set_index("InstrumentVTId", inplace = True)
            self.NewOpenComposition.Units = self.NewOpenComposition.Units*self.NewOpenComposition.ShareAdjustmentFactor
            self.NewOpenComposition.OpenPriceLocalCurrency = self.NewOpenComposition.PriceInInstrumentCurrency*self.NewOpenComposition.PriceAdjustmentFactor
            self.NewOpenComposition.reset_index(inplace = True)
            self.CloseComposition.reset_index(inplace = True)
            self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"].str.contains("Ca:EU"),"Units"]=0

        else:
            self.NewOpenComposition.Units = self.NewOpenComposition.Units*self.NewOpenComposition.ShareAdjustmentFactor
            self.NewOpenComposition.OpenPriceLocalCurrency = self.NewOpenComposition.PriceInInstrumentCurrency*self.NewOpenComposition.PriceAdjustmentFactor
            

    def fn_CalculateIndexLevel(self):
        CloseDivisor = self.IndexCalculationDetails.OpenDivisor.values[0]
        TransactionCost = self.IndexCalculationDetails.OpenTransactionCost.values[0]
        self.NewCloseComposition[["InstrumentVTId","Units","PriceInInstrumentCurrency","FxRate"]].to_csv("Unitss.csv")       
        self.IndexLevel = sum(self.NewCloseComposition["Units"]*self.NewCloseComposition["PriceInInstrumentCurrency"]*self.NewCloseComposition["FxRate"])/CloseDivisor-TransactionCost
        print(self.IndexLevel)
        
    
    def fn_IndexCalculationDetails(self):
        CloseDivisor = self.IndexCalculationDetails.OpenDivisor.values[0]
        CloseTransactionCost = self.IndexCalculationDetails.OpenTransactionCost.values[0]
        CloseSumproduct = sum(self.NewCloseComposition["Units"]*self.NewCloseComposition["PriceInInstrumentCurrency"]*self.NewCloseComposition["FxRate"])
        OpenSumproduct = sum(self.NewOpenComposition["Units"]*self.NewOpenComposition["OpenPriceLocalCurrency"]*self.NewOpenComposition["FxRate"])
        if self.IndexCalculationDetails.IsRebalance.values[0]:
            OpenDivisor = (CloseDivisor * (self.IndexLevel + CloseTransactionCost) + (OpenSumproduct - CloseSumproduct)) / self.IndexLevel            
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            self.NewOpenComposition.set_index("InstrumentVTId", inplace = True)
            Fee = self.IndexCalculationDetails.Fee.values[0]
            OpenTransactionCost = sum(abs(self.NewCloseComposition["Units"]-self.NewOpenComposition["Units"])*self.NewCloseComposition["PriceInInstrumentCurrency"]*self.NewCloseComposition["FxRate"])*Fee
            self.NewCloseComposition.to_csv("NewCloseComposition.csv")
            self.NewOpenComposition.to_csv("NewOpenComposition.csv")
        else:
            OpenDivisor = (CloseDivisor * (self.IndexLevel + CloseTransactionCost) + (OpenSumproduct - CloseSumproduct)) / (self.IndexLevel + CloseTransactionCost)
            OpenTransactionCost = CloseTransactionCost

        temp_df = self.IndexCalculationDetails.copy()
        temp_df.PriceDate = self.RunDate
        temp_df.Divisor = CloseDivisor
        temp_df.IndexLevel = self.IndexLevel
        temp_df.OpenDivisor = OpenDivisor
        temp_df.OpenTransactionCost = OpenTransactionCost
        temp_df.TransactionCost = CloseTransactionCost
        self.NewIndexCalculationDetails = temp_df.loc[:,self.IndexCalculationDetails.columns]
        self.NewOpenComposition.reset_index(inplace = True)


    def fn_CAHandling(self):
        self.temp_df_CA = pd.DataFrame(columns = ["InstrumentVTId", "PriceAdjustmentFactor", "ShareAdjustmentFactor"])
        self.temp_df_CA["InstrumentVTId"] = list(set(self.NewCloseComposition["InstrumentVTId"]))
        self.temp_df_CA.set_index("InstrumentVTId", inplace = True)
        self.CorporateActions = self.CorporateActions.loc[self.CorporateActions["InstrumentVTId"].isin(list(set(self.NewCloseComposition["InstrumentVTId"])))]
        self.CorporateActions.reset_index(inplace = True)
        if len(self.CorporateActions) != 0:
            for i in self.CorporateActions.index:
                if (np.isnan(self.CorporateActions.loc[i,"ShareAdjustmentFactor"]) & np.isnan(self.CorporateActions.loc[i,"PriceAdjustmentFactor"])):
                    if (self.CorporateActions.loc[i,"DividendType"] == "Cash Dividend") | (self.CorporateActions.loc[i,"DividendType"]=="Special Cash Dividend"):
                        ClosingPrice = float(self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==self.CorporateActions.loc[i,"InstrumentVTId"],"PriceInInstrumentCurrency"])
                        self.CorporateActions.loc[i, "ShareAdjustmentFactor"] = 1
                        self.CorporateActions.loc[i, "PriceAdjustmentFactor"] =  1 - float(self.CorporateActions.loc[i,"CashAmount"])*(1 - float(self.CorporateActions.loc[i,"WithHoldingTax"]))/ClosingPrice
                    elif (self.CorporateActions.loc[i,"DividendType"] == "Stock Split"):
                        self.CorporateActions.loc[i, "ShareAdjustmentFactor"] = float(self.CorporateActions.loc[i,"Factor"])
                        self.CorporateActions.loc[i, "PriceAdjustmentFactor"] =  1/(float(self.CorporateActions.loc[i,"Factor"]))
                    elif (self.CorporateActions.loc[i,"DividendType"] == "Stock Dividend"):
                        self.CorporateActions.loc[i, "ShareAdjustmentFactor"] = 1 + float(self.CorporateActions.loc[i,"Factor"])
                        self.CorporateActions.loc[i, "PriceAdjustmentFactor"] =  1/(1 + float(self.CorporateActions.loc[i,"Factor"]))
                    elif (self.CorporateActions.loc[i,"DividendType"] == "Rights Offering"):
                        ClosingPrice = float(self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==self.CorporateActions.loc[i,"InstrumentVTId"],"PriceInInstrumentCurrency"])
                        self.CorporateActions.loc[i, "ShareAdjustmentFactor"] = 1 + float(self.CorporateActions.loc[i,"Factor"])
                        self.CorporateActions.loc[i, "PriceAdjustmentFactor"] =  (ClosingPrice + float(self.CorporateActions.loc[i,"IssuePrice"])*float(self.CorporateActions.loc[i,"Factor"]))/((1 + float(self.CorporateActions.loc[i,"Factor"]))*ClosingPrice)                

                else:
                    pass
            print(self.CorporateActions)                
            for instruments in self.NewOpenComposition["InstrumentVTId"]:
                df_CaInstrument = self.CorporateActions.loc[self.CorporateActions["InstrumentVTId"]==instruments,:]
#                print(df_CaInstrument)
                if len(df_CaInstrument)!= 0:
                    df_CaWithoutCashDiv = df_CaInstrument.loc[~(df_CaInstrument["DividendType"].isin(["Cash Dividend", "Special Cash Dividend"])),:]
#                    print(df_CaWithoutCashDiv)
                    if len(df_CaWithoutCashDiv) !=0:
                        SAFWithoutCashDiv = np.prod(df_CaWithoutCashDiv["ShareAdjustmentFactor"])
                        PAFWithoutCashDiv = np.prod(df_CaWithoutCashDiv["PriceAdjustmentFactor"])
                    else:
                        SAFWithoutCashDiv  = 1
                        PAFWithoutCashDiv = 1
                        
                    df_CaWithCashDiv = df_CaInstrument.loc[df_CaInstrument["DividendType"].isin(["Cash Dividend", "Special Cash Dividend"]),:]
                    if len(df_CaWithCashDiv) !=0:
                        print(df_CaWithCashDiv)
                        SAFWithCashDiv = np.prod(df_CaWithCashDiv["ShareAdjustmentFactor"])
                        PAFWithCashDiv = sum(df_CaWithCashDiv["PriceAdjustmentFactor"]-1)+1
                        print(PAFWithCashDiv)
                    else:
                        SAFWithCashDiv = 1
                        PAFWithCashDiv = 1
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"ShareAdjustmentFactor"] = SAFWithCashDiv*SAFWithoutCashDiv
                    self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==instruments,"PriceAdjustmentFactor"] = PAFWithCashDiv*PAFWithoutCashDiv
                else:
                    pass
        else:
            pass
                        
        
    @hp.output_file(["VTCloseComposition"])    
    def fn_VTCloseComp(self): 
        pattern = r'Ca:*' #regex to identify VT Id for CASH
        tempCloseComposition = self.OpenCompositions[ (self.OpenCompositions.Units != 0) & (~ (self.OpenCompositions.InstrumentVTId.str.contains(pattern)))]
        tempCloseComposition.PriceDate = self.RunDate
        self.NewCloseComposition = tempCloseComposition[~ tempCloseComposition.IsDelisted]
        _delisted_rows = tempCloseComposition[tempCloseComposition.IsDelisted]
        df_Cash = self.OpenCompositions[self.OpenCompositions.InstrumentVTId.str.contains(pattern)]
        if np.isnan(df_Cash.Units.values[0]):
            df_Cash.Units = 0
            df_Cash.PriceInInstrumentCurrency = 1
        df_Cash.PriceDate = self.RunDate
        if not len(_delisted_rows) == 0:
            _additional_cash = sum(_delisted_rows.OpenPriceLocalCurrency * _delisted_rows.Units * _delisted_rows.FxRate)  
            df_Cash.loc[df_Cash.InstrumentVTId.str.contains(pattern),"Units"] += _additional_cash
        self.NewCloseComposition = self.NewCloseComposition.append(df_Cash)       
        self.fn_rebalCheckCloseComp()
        self.NewCloseComposition.PriceAdjustmentFactor = 1
        self.NewCloseComposition.ShareAdjustmentFactor = 1
        
        print("New Close Composition created")
        return [self.NewCloseComposition]
            
        
    @hp.output_file(["VTIndexCalculationDetails", "VTOpenComposition"])
    def fn_VTOpenComp(self):
        self.NewOpenComposition = self.CloseComposition.copy()
        self.NewOpenComposition.ShareAdjustmentFactor = 1    
        self.NewOpenComposition.PriceAdjustmentFactor = 1
        self.fn_CAHandling()
        self.fn_rebalCheckOpenComp()
        self.fn_CalculateIndexLevel()
        self.fn_IndexCalculationDetails()
        return [self.NewIndexCalculationDetails, self.NewOpenComposition]

         
class NomuraMomentumValueIndexCalculation:
    
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.IndexSpecificData["RebalanceDate"] = pd.to_datetime(self.IndexSpecificData["RebalanceDate"])
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])        
        self.IndexLevel = 0
        self.RebalFlag = 0
        self.IndexVolatilityScale = float(self.IndexSpecificData.loc[0,"VolatilityScaleLeverage"])
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log    


    def fn_NomuraMVRebalCheck(self):
        try:
            self.log.processname_change("NomuraMVRebalCheck")
            if self.RunDate.weekday()==0:
                Weekday1 = self.RunDate
            else:
                Weekday1 = self.RunDate - dt.timedelta(days = self.RunDate.weekday())
            DayOfWeek = 0
            HolidayList = self.HolidayCalendar.loc[self.HolidayCalendar["Center_Name"]=="London"]
            while (Weekday1 in list(HolidayList["Date"])) or (Weekday1.weekday() in [5,6]) or (DayOfWeek !=1):
                if (Weekday1 not in list(HolidayList["Date"])) and (Weekday1.weekday() not in [5,6]):
                    DayOfWeek += 1
                else:
                    Weekday1 = Weekday1 + dt.timedelta(1)
            Weekday2 = Weekday1 + dt.timedelta(1)
            while (Weekday2 in list(HolidayList["Date"])) or (Weekday2.weekday() in [5,6]) or (DayOfWeek !=2):
                if (Weekday2 not in list(HolidayList["Date"])) and (Weekday2.weekday() not in [5,6]):
                    DayOfWeek += 1
                    self.RebalDate = Weekday2
                else:
                    Weekday2 = Weekday2 + dt.timedelta(1)            
                    self.RebalDate = Weekday2
            if self.RebalDate == self.RunDate:
                self.RebalFlag = 1
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

            
    def fn_NomuraMVCloseComposition(self):
        try:    
            self.log.processname_change("NomuraMVCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            PricesRun = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,:].set_index("GenericInstrumentVTId")
            PricesLastRun = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.LastRunDate,:].set_index("GenericInstrumentVTId")
            IndexLevelLastRun = float(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"])
            PricesRun["ComponentReturn"] = PricesRun["Price"]/PricesLastRun["Price"]-1
            Return = np.mean(PricesRun["ComponentReturn"])*self.IndexVolatilityScale
            FxReturn = PricesRun.ix[0,"FxRate"]/PricesLastRun.ix[0,"FxRate"]
            print(IndexLevelLastRun)
            self.IndexLevel = IndexLevelLastRun*(1+Return*FxReturn)
            
            print("self.IndexLevel = ",self.IndexLevel)
            CashUnits = self.IndexLevel - sum(self.NewCloseComposition["Units"]*PricesRun["Price"]*PricesRun["FxRate"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca"),:].copy()
            df_Cash.loc[:,"Units"] = CashUnits
            self.NewCloseComposition.reset_index(inplace = True)
            self.NewCloseComposition = self.NewCloseComposition.append(df_Cash)
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            


    def fn_NomuraMVOpenComposition(self):
        try:    
            self.log.processname_change("NomuraMVOpenComposition")
            if self.RebalFlag == 1:
                df_VolScale = self.DailyPrices.loc[self.DailyPrices["PriceDate"]<=self.LastRunDate,:]
                df_VolScale = df_VolScale.pivot_table(index = "PriceDate", columns = "GenericInstrumentVTId", values = "Price").reset_index()
                
                for instrument in list(set(self.DailyPrices["GenericInstrumentVTId"])):
                    df_VolScale["Return"+instrument] = df_VolScale[instrument]/df_VolScale[instrument].shift(1)-1
                df_VolScale.set_index("PriceDate", inplace = True)
                df_VolScale["RawReturn"] = df_VolScale[list(df_VolScale.columns[df_VolScale.columns.str.contains("Return")])].mean(axis = 1)
                df_VolScale = df_VolScale.iloc[-60:,:]
                VolatilityTarget = float(self.IndexSpecificData.loc[0,"VolatilityTarget"])
                
                Leverage = VolatilityTarget/(np.std(df_VolScale["RawReturn"], ddof = 1)*(252**0.5))
                VolatilityFactorCap = float(self.IndexSpecificData.loc[0,"VolatilityFactorCap"])
                self.IndexVolatilityScale = min(Leverage,VolatilityFactorCap)
                self.NewOpenComposition = self.OpenComposition.loc[~(self.OpenComposition["InstrumentVTId"].str.contains("Ca:")),:]
            else:
                self.NewOpenComposition = self.OpenComposition.loc[~(self.OpenComposition["InstrumentVTId"].str.contains("Ca:")),:]
            self.NewOpenComposition["Units"] = self.IndexLevel*self.IndexVolatilityScale/(2*self.NewOpenComposition["Price"]*self.NewOpenComposition["FxRate"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["InstrumentVTId"].str.contains("Ca:"),:]
            df_Cash.loc[:,"Units"] = self.IndexLevel - sum(self.NewOpenComposition["Price"]*self.NewOpenComposition["Units"]*self.NewOpenComposition["FxRate"])
            self.NewOpenComposition = self.NewOpenComposition.append(df_Cash)
            self.NewOpenComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            

    def fn_NomuraMVOpenIndexSpecificData(self):
        try:    
            self.log.processname_change("NomuraMVOpenIndexSpecificData")
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            self.OpenIndexSpecificData.loc[:,"RebalanceDate"] = self.RebalDate
            self.OpenIndexSpecificData.loc[:,"IndexLevel"] = self.IndexLevel
            self.OpenIndexSpecificData.loc[:,"PriceDate"] = self.RunDate
            self.OpenIndexSpecificData.loc[:,"VolatilityScaleLeverage"] = self.IndexVolatilityScale
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_NomuraMVRebalCheck()
        self.fn_NomuraMVCloseComposition()
        print("New Close Composition created")
        return [self.NewCloseComposition]        
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_NomuraMVOpenComposition()
        self.fn_NomuraMVOpenIndexSpecificData()
        print("New Open Composition created")
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
            
            
class NomuraMomentumIndexCalculation:
    
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.IndexSpecificData["RebalanceDate"] = pd.to_datetime(self.IndexSpecificData["RebalanceDate"])
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])        
        self.IndexLevel = 0
        self.RebalFlag = 0
        self.ReceiverRoll = pd.DataFrame(index = self.IndexSpecificData["GenericInstrumentVTId"], columns = ["RollDate"] )
        self.IndexVolatilityScale = float(self.IndexSpecificData.loc[0,"VolatilityScaleLeverage"])
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log    


    def fn_NomuraMRebalCheck(self):
        try:
            self.log.processname_change("NomuraMRebalCheck")
            if self.RunDate.weekday()==0:
                Weekday1 = self.RunDate
            else:
                Weekday1 = self.RunDate - dt.timedelta(days = self.RunDate.weekday())
            DayOfWeek = 0
            HolidayList = self.HolidayCalendar.loc[self.HolidayCalendar["Center_Name"]=="London",:]
            while (Weekday1 in list(HolidayList["Date"])) or (Weekday1.weekday() in [5,6]) or (DayOfWeek !=1):
                if (Weekday1 not in list(HolidayList["Date"])) and (Weekday1.weekday() not in [5,6]):
                    DayOfWeek += 1
                else:
                    Weekday1 = Weekday1 + dt.timedelta(1)
            Weekday2 = Weekday1 + dt.timedelta(1)
            while (Weekday2 in list(HolidayList["Date"])) or (Weekday2.weekday() in [5,6]) or (DayOfWeek !=2):
                if (Weekday2 not in list(HolidayList["Date"])) and (Weekday2.weekday() not in [5,6]):
                    DayOfWeek += 1
                    self.RebalDate = Weekday2
                else:
                    Weekday2 = Weekday2 + dt.timedelta(1)            
                    self.RebalDate = Weekday2
            if self.RebalDate == self.RunDate:
                self.RebalFlag = 1
            print(self.RebalFlag)
            print(self.RebalDate)
            
            """Receiver Roll Date"""
            
            for instruments in self.ReceiverRoll.index:
                RollDate = pd.to_datetime(pd.to_datetime({'year': [self.RunDate.year], 'month':[self.RunDate.month], 'day':[15]}).values[0])
                if "US" in instruments:
                    CenterList = ["London", "New York"]
                elif "EU" in instruments:
                    CenterList = ["TARGET (Euro)"]
                elif "GB" in instruments:
                    CenterList = ["London"]
                elif "JP" in instruments:
                    CenterList = ["London", "Tokyo"]
                
                HolidayList = self.HolidayCalendar.loc[self.HolidayCalendar["Center_Name"].isin(CenterList),:]

                while (RollDate in list(HolidayList["Date"])) or (RollDate.weekday() in [5,6]):
                    RollDate = RollDate + dt.timedelta(1)            
                self.ReceiverRoll.loc[instruments,"RollDate"] = RollDate
            
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")

            
    
    def fn_HelperMovingStats(self,n, df, stat):
        calc_df = df.iloc[-n:,:].apply(stat)
        return calc_df
            
            
    def fn_NomuraMCloseComposition(self):
        try:    
            self.log.processname_change("NomuraMCloseComposition")
            self.NewCloseComposition = self.OpenComposition.loc[~(self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca")),:].copy()
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")


    def fn_NomuraMCalculateIndexLevel(self):
        try:    
            self.log.processname_change("NomuraMCalculateIndexLevel")
            self.prices_df = self.DailyPrices.pivot_table(index = "PriceDate", columns = "GenericInstrumentVTId", values = "Price").reset_index()
            self.prices_df.set_index("PriceDate", inplace = True)
            self.IndexSpecificData.set_index("GenericInstrumentVTId", inplace = True)
            
            """InsRet(t) calculation"""
            self.InsRet = (self.prices_df.loc[self.RunDate,~(self.prices_df.columns.str.contains("GIMO"))]-self.prices_df.loc[self.LastRunDate,~(self.prices_df.columns.str.contains("GIMO"))])*self.IndexSpecificData["Signal"]
            self.InsRet = self.InsRet.to_frame()
            self.InsRet.rename(columns = {0: "Return"}, inplace = True)
            self.InsRet["CurrencyVTId"] = self.IndexSpecificData["CurrencyVTId"]
            self.DailyPrices.set_index("GenericInstrumentVTId", inplace = True)
            self.InsRet["CountryReturn"] = self.InsRet["Return"]*self.IndexSpecificData["Weights"]*self.IndexVolatilityScale-self.IndexSpecificData["RebalancingTransactionCost"]\
                                      *self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(~(self.DailyPrices.index.str.contains("GIMO"))),"FxRate"]\
                                      /self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.LastRunDate)&(~(self.DailyPrices.index.str.contains("GIMO"))),"FxRate"]
            print(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(~(self.DailyPrices.index.str.contains("GIMO"))),"FxRate"])
            print(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.LastRunDate)&(~(self.DailyPrices.index.str.contains("GIMO"))),"FxRate"])
            CountryRet = self.InsRet.groupby(["CurrencyVTId"])[["CountryReturn"]].sum()
            if self.RunDate in self.HolidayCalendar.loc[self.HolidayCalendar["Center_Name"]=="Tokyo","Date"]:
                CountryRet.loc[CountryRet.index.str.contains("JP"),"CountryReturn"] = 0
            
            
            IndexLevelLastRun = float(self.IndexSpecificData["IndexLevel"].values[0])
            self.IndexLevel = IndexLevelLastRun*(1+sum(CountryRet["CountryReturn"]))
            print("self.IndexLevel = ",self.IndexLevel)
            
            self.DailyPrices.reset_index(inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
            
    def fn_NomuraMCalculateCash(self):
        try:
            self.log.processname_change("NomuraMCalculateCash")            
            PricesRun = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(~(self.DailyPrices["GenericInstrumentVTId"].str.contains("GIMO"))),:].set_index("GenericInstrumentVTId")
            
            CashUnits = self.IndexLevel - sum(self.NewCloseComposition["Units"]*PricesRun["Price"]*PricesRun["FxRate"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["SpecificTicker"].str.lower().str.contains("ca"),:].copy()
            df_Cash.loc[:,"Units"] = CashUnits
            self.NewCloseComposition.reset_index(inplace = True)
            self.NewCloseComposition = self.NewCloseComposition.append(df_Cash)
            self.NewCloseComposition["Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")                        
            
            
    def fn_NomuraMOpenIndexSpecificData(self):
        try:
            self.log.processname_change("NomuraMOpenIndexSpecificData")
            
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            self.OpenIndexSpecificData.loc[:,"IndexLevel"] = self.IndexLevel
            
            MovingAverage = {'ST': float(self.IndexSpecificData["ShortTermAverage"].values[0])
                             ,'MT': float(self.IndexSpecificData["MidTermAverage"].values[0])
                             ,'LT': float(self.IndexSpecificData["LongTermAverage"].values[0])}

            temp_df = self.prices_df.loc[self.prices_df.index<=self.LastRunDate,~(self.prices_df.columns.str.contains("GIMO"))].copy()
    
            """Signal(t) calculation"""
            SignalData = {}
            for key in MovingAverage.keys():
                SignalData[key+"LB"] = self.fn_HelperMovingStats(int(MovingAverage[key]),temp_df,np.mean) - self.fn_HelperMovingStats(int(MovingAverage[key]),temp_df,partial(np.std, ddof = 1))
                SignalData[key+"UB"] = self.fn_HelperMovingStats(int(MovingAverage[key]),temp_df,np.mean) + self.fn_HelperMovingStats(int(MovingAverage[key]),temp_df,partial(np.std, ddof = 1))
#                SignalData[key+"UB"].to_csv("SignalData"+key+"UB.csv")
#                SignalData[key+"LB"].to_csv("SignalData"+key+"LB.csv")
                
            Signal_t = pd.DataFrame(index = self.IndexSpecificData.index, columns = ["Signal"])
            for instrument in Signal_t.index:
                if ((SignalData["STLB"][instrument]>SignalData["MTUB"][instrument]) & (SignalData["STLB"][instrument]>SignalData["LTUB"][instrument])):
                    Signal_t.loc[instrument,"Signal"] = 1
                elif ((SignalData["STUB"][instrument]<SignalData["MTLB"][instrument]) & (SignalData["STUB"][instrument]<SignalData["LTLB"][instrument])):
                    Signal_t.loc[instrument,"Signal"] = -1
                elif ((self.IndexSpecificData.loc[instrument,"Signal"]==1) & ((SignalData["STUB"][instrument]<SignalData["MTLB"][instrument]) | (SignalData["STUB"][instrument]<SignalData["LTLB"][instrument]))):
                    Signal_t.loc[instrument,"Signal"] = 0
                elif ((self.IndexSpecificData.loc[instrument,"Signal"]==-1) & ((SignalData["STLB"][instrument]>SignalData["MTUB"][instrument]) | (SignalData["STLB"][instrument]>SignalData["LTUB"][instrument]))):
                    Signal_t.loc[instrument,"Signal"] = 0
                else:
                    Signal_t.loc[instrument,"Signal"] = self.IndexSpecificData.loc[instrument,"Signal"]
            self.OpenIndexSpecificData["Signal"] = Signal_t

            """Gross Index(t) calculation"""
            self.GrossIndex = pd.DataFrame(index = self.prices_df.columns[self.prices_df.columns.str.contains("GIMO")], columns = ["IndexLevel"])
                        
#            if np.any(np.isnan(self.prices_df.loc[max(self.prices_df.index),self.prices_df.columns.str.contains("GIMO")])):                
            for instruments in self.GrossIndex.index:
                FxRateRun = np.array(self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"].str.contains(instruments[2:6]))&(~(self.DailyPrices["GenericInstrumentVTId"].str.contains("GIMO")))&(self.DailyPrices["PriceDate"]==self.RunDate),"FxRate"])
                FxRateLastRun = np.array(self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"].str.contains(instruments[2:6]))&(~(self.DailyPrices["GenericInstrumentVTId"].str.contains("GIMO")))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"FxRate"])
                self.GrossIndex.loc[instruments,"IndexLevel"] = float(self.DailyPrices.loc[(self.DailyPrices["GenericInstrumentVTId"].str.contains(instruments))&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"]) \
                                        +sum(self.InsRet.loc[self.InsRet.index.str.contains(instruments[2:6]),"Return"]*self.IndexSpecificData.loc[self.IndexSpecificData.index.str.contains(instruments[2:6]),"Weights"]*FxRateRun/FxRateLastRun)
            
            self.GrossIndex["Date"] = self.RunDate
            self.GrossIndex.reset_index(inplace = True)
            self.GrossIndex.rename(columns = {"GenericInstrumentVTId": "IndexVTId"}, inplace = True)

            """Index Volatility Scale(t-1) calculation"""
            if self.RebalFlag ==1:                
                GrossReturn = self.prices_df.loc[self.prices_df.index<=self.LastRunDate,self.prices_df.columns.str.contains("GIMO")].dropna()-self.prices_df.loc[self.prices_df.index<=self.LastRunDate,self.prices_df.columns.str.contains("GIMO")].dropna().shift(1)
                GrossReturn = GrossReturn.iloc[-60:,:]
                GrossReturn["Total"]= GrossReturn.sum(axis = 1)
                VolatilityTarget = float(self.IndexSpecificData["VolatilityTarget"].values[0])
                Leverage = VolatilityTarget/(np.std(GrossReturn["Total"], ddof = 1)*(252**0.5))
                VolatilityFactorCap = float(self.IndexSpecificData["VolatilityFactorCap"].values[0])
                self.IndexVolatilityScale = min(Leverage,VolatilityFactorCap)
                self.OpenIndexSpecificData.loc[:,"VolatilityScaleLeverage"] = self.IndexVolatilityScale

            """Final Signal calculation"""
            FinalSignal_t = Signal_t["Signal"]*self.IndexVolatilityScale*100*self.IndexSpecificData["Weights"]/self.IndexSpecificData["PV01Fwd"]
            FinalSignal_t_1 = self.IndexSpecificData["Signal"]*self.IndexSpecificData["VolatilityScaleLeverage"]*100*self.IndexSpecificData["Weights"]/self.IndexSpecificData["PV01FwdLastRun"]
           
            """Delta Change(t) Calculation"""
            FinalSignal_Multiplier = pd.Series(index = FinalSignal_t.index)
            
            for instruments in FinalSignal_Multiplier.index:                    
                if self.RunDate == self.ReceiverRoll.loc[instruments,"RollDate"]:
                    FinalSignal_Multiplier.loc[instruments] = self.IndexSpecificData.loc[instruments,"PV01Spot"]
                else:
                    FinalSignal_Multiplier.loc[instruments] = self.IndexSpecificData.loc[instruments,"PV01Fwd"]
            
            DeltaChange = FinalSignal_t*self.IndexSpecificData["PV01Fwd"] - FinalSignal_t_1*FinalSignal_Multiplier
            
            DeltaChange = DeltaChange.to_frame()
            DeltaChange.rename(columns = {0: "Change"}, inplace = True)
            DeltaChange["CurrencyVTId"] = self.IndexSpecificData["CurrencyVTId"].copy()
            
            """OutrightTC(t) calculation"""
            DeltaChange["Cost"] = abs(DeltaChange["Change"]*self.IndexSpecificData["Fee"])
            OutrightTC = DeltaChange.groupby(["CurrencyVTId"])[["Cost"]].sum()
            
            """Switch Delta(t) calculation"""

            DeltaChange["Max"] = DeltaChange["Change"].apply(lambda x: max(x,0))
            DeltaChange["Min"] = DeltaChange["Change"].apply(lambda x: min(x,0))
            SwitchDelta = np.minimum(DeltaChange.groupby(["CurrencyVTId"])[["Max","Min"]].sum()["Max"],-1*DeltaChange.groupby(["CurrencyVTId"])[["Max","Min"]].sum()["Min"])

            """SwitchTC(t) calculation"""
            SwitchTC = pd.DataFrame(index = OutrightTC.index)
            for currency in SwitchTC.index:
                SwitchTC.loc[currency,"Cost"] =  SwitchDelta[currency]*float(self.IndexSpecificData.loc[self.IndexSpecificData["CurrencyVTId"]==currency,"Fee"][0])*1.5

            """Roll Delta(t) calculation"""
            RollDelta = pd.DataFrame(index = self.ReceiverRoll.index)
            for instruments in RollDelta.index:
                if (self.RunDate==self.ReceiverRoll.loc[instruments,"RollDate"]) & (np.sign(Signal_t.loc[instruments,"Signal"])==np.sign(self.IndexSpecificData.loc[instruments,"Signal"])):
                    RollDelta.loc[instruments, "Change"] = np.minimum(np.abs(FinalSignal_t.loc[instruments]*self.IndexSpecificData.loc[instruments,"PV01Fwd"]),np.abs(FinalSignal_t_1.loc[instruments]*self.IndexSpecificData.loc[instruments,"PV01Spot"]))
                else:
                    RollDelta.loc[instruments, "Change"] = 0


            """RollTC(t) calculation"""
            RollDelta["Cost"] = RollDelta["Change"]*self.IndexSpecificData["RollCost"]
            RollDelta["CurrencyVTId"] =self.IndexSpecificData["CurrencyVTId"]

            RollTC = RollDelta.groupby(["CurrencyVTId"])[["Cost"]].sum()
                        
            """TC(t) Calculation"""
            TC = OutrightTC["Cost"] + SwitchTC["Cost"] + RollTC["Cost"]
            

            for currency in self.OpenIndexSpecificData["CurrencyVTId"]:
                self.OpenIndexSpecificData.loc[self.OpenIndexSpecificData["CurrencyVTId"]==currency,"RebalancingTransactionCost"] = TC.loc[currency]
            
            self.OpenIndexSpecificData.loc[:,"RebalanceDate"] = self.RebalDate
            self.OpenIndexSpecificData.loc[:,"PriceDate"] = self.RunDate
                    
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
            
            
    def fn_NomuraMOpenComposition(self):
        try:    
            self.log.processname_change("NomuraMOpenComposition")
            self.NewOpenComposition = self.OpenComposition.loc[~(self.OpenComposition["InstrumentVTId"].str.contains("Ca:")),:]
            self.NewOpenComposition.set_index("InstrumentVTId", inplace = True)
            self.NewOpenComposition["Units"] = self.IndexLevel*self.OpenIndexSpecificData["Signal"]*self.OpenIndexSpecificData["Weights"]*self.OpenIndexSpecificData["VolatilityScaleLeverage"]/self.NewOpenComposition["FxRate"]
                                                                                   
            df_Cash = self.OpenComposition.loc[self.OpenComposition["InstrumentVTId"].str.contains("Ca:"),:]
            df_Cash.set_index("InstrumentVTId", inplace = True)
            df_Cash.loc[:,"Units"] = self.IndexLevel - sum(self.NewOpenComposition["Price"]*self.NewOpenComposition["Units"]*self.NewOpenComposition["FxRate"])
            self.NewOpenComposition = self.NewOpenComposition.append(df_Cash)
            self.NewOpenComposition["Date"] = self.RunDate
            self.OpenIndexSpecificData.reset_index(inplace = True)
            self.NewOpenComposition.reset_index(inplace = True)
#            print(self.NewOpenComposition)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_NomuraMRebalCheck()
        self.fn_NomuraMCloseComposition()
        self.fn_NomuraMCalculateIndexLevel()
        self.fn_NomuraMCalculateCash()
        print("New Close Composition created")
        return [self.NewCloseComposition]        
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData", "VTGrossIndexLevel"])
    def fn_VTOpenComp(self):
        self.fn_NomuraMOpenIndexSpecificData()
        self.fn_NomuraMOpenComposition()
        print("New Open Composition created") 
        return [self.NewOpenComposition, self.OpenIndexSpecificData, self.GrossIndex]                       
            
class MRUEIndexCalculation:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        
        self.ChildOpenComp = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.ChildCloseComp = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.IndexOpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
        print(self.LastRunDate)
        self.data_for_RunDate = self.data_for_date(self.RunDate)
        self.data_for_LastRunDate = self.data_for_date(self.LastRunDate)
        self.child_FX = self.ChildCloseComp[self.ChildCloseComp['Currency']=='EUR']['FxRate'].values[0]

#        print(self.IndexSpecificData.loc[0,'IndexLevel'])
    
    def data_for_date(self, date):
        return {
                'funding_rate': self.DailyPrices.loc[self.DailyPrices["PriceDate"]==date,'FundingRate'].values[0]
                ,'IndexLevel': (lambda date: self.IndexSpecificData.loc[0,'IndexLevel'] if date==self.LastRunDate else 0)(date)
                ,'EURrate': self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)
                                & (self.DailyPrices["GenericInstrumentVTId"]=="Ei:EU:DT:XX:X:XX:MRET:XXXX:ML") ,"FxRate"].values[0]
                ,'ChildLevel': np.round((self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)
                                & (self.DailyPrices["GenericInstrumentVTId"]=="Ei:EU:DT:XX:X:XX:MRET:XXXX:ML") ,"Price"]).values[0],2)
                ,'funding_days': (lambda date: hdd.no_of_days(pd.to_datetime(self.LastRunDate), pd.to_datetime(self.RunDate)) if date==self.LastRunDate else hdd.no_of_days(pd.to_datetime(self.RunDate), pd.to_datetime(self.OpenDate)))(date)
                ,'ChildAdjFac': np.round((self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)
                                & (self.DailyPrices["GenericInstrumentVTId"]=="Ei:EU:DT:XX:X:XX:MRET:XXXX:ML") ,"Price"]).values[0],2)/(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)
                                & (self.DailyPrices["GenericInstrumentVTId"]=="Ei:EU:DT:XX:X:XX:MRET:XXXX:ML") ,"Price"]).values[0]
                }

    def OpenClose_data(self,state):
        _unit_data = (lambda state: self.data_for_LastRunDate if state=='close' else self.data_for_RunDate)(state)
        _child_cash_data = (lambda state: self.ChildCloseComp if state=='close' else self.ChildOpenComp)(state)
#        print(type(float(_child_cash_data(state).loc[_child_cash_data(state)['InstrumentVTId'].str.contains('Ca:EU'),'Units'].values[0])))
        _child_cash = _child_cash_data.loc[_child_cash_data['InstrumentVTId'].str.contains('Ca:EU'),"Units"]
        print(_child_cash)        
        units_child = hu.std_unit_calc(1, _unit_data['IndexLevel'],_unit_data['ChildLevel'],_unit_data['EURrate'])
#        print(_child_cash, state)
        EUR_child = units_child * _child_cash * _unit_data["ChildAdjFac"]
        EUR_hedge = hu.std_unit_calc(-1, _unit_data['IndexLevel'],1,_unit_data['EURrate'])
        EUR_funding = EUR_hedge*hf.simple_funding_cost_actbyX(_unit_data['funding_rate'],_unit_data['funding_days'],360)
        return {
                'units_child': units_child
                ,'EUR_child': EUR_child
                ,'EUR_hedge': EUR_hedge
                ,'EUR_funding': EUR_funding
                }     
    
    def IndexLevel_RunDate(self):
        self.data_for_RunDate['IndexLevel'] = self.NewCloseComposition[['Units','Price','FxRate']].product(axis=1).sum()
        print("Index Level  ---> "+str(self.data_for_RunDate['IndexLevel']))
        return self.data_for_RunDate['IndexLevel']
    
    @property
    def FXAdjFactor(self):
        return self.data_for_RunDate['EURrate']/self.child_FX
        
    @property
    def NewCloseComposition(self):
        t_NewCloseComposition = self.IndexOpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        _close_data = self.OpenClose_data('close')
        t_NewCloseComposition.loc[t_NewCloseComposition['Currency']=="USD","Units"] *= self.FXAdjFactor
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:US'),'Units'] = self.data_for_LastRunDate['IndexLevel']
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:EU'),'Units'] = _close_data['EUR_child'] + _close_data['EUR_hedge']+_close_data['EUR_funding']
        print(t_NewCloseComposition[['InstrumentVTId','Units']]) 
        return t_NewCloseComposition
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        IndexLevel_RunDate = self.IndexLevel_RunDate()
        t_NewOpenComposition = self.IndexOpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        _open_data = self.OpenClose_data('open')
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:EU'),'Units'] = _open_data['EUR_child']+_open_data['EUR_hedge']+_open_data['EUR_funding']
        t_NewOpenComposition.set_index("InstrumentVTId", inplace = True)
        self.ChildOpenComp.set_index("InstrumentVTId", inplace = True)
        t_NewOpenComposition.loc[~t_NewOpenComposition.index.str.contains('Ca:EU'),'Units'] = self.ChildOpenComp.loc[~self.ChildOpenComp.index.str.contains('Ca:EU'),'Units']*_open_data['units_child']*self.data_for_RunDate['ChildAdjFac']
        t_NewOpenComposition.loc[t_NewOpenComposition.index.str.contains('Ca:US'),'Units'] = IndexLevel_RunDate - t_NewOpenComposition.loc[~t_NewOpenComposition.index.str.contains('Ca:US'),['Units','Price','FxRate']].product(axis=1).sum()
        t_NewOpenComposition.reset_index(inplace = True)
        self.ChildOpenComp.reset_index(inplace = True)       
        t_OpenIndexSpecificData = self.IndexSpecificData.copy()
        t_OpenIndexSpecificData["PriceDate"] = self.RunDate
        t_OpenIndexSpecificData["IndexLevel"] = IndexLevel_RunDate
        t_OpenIndexSpecificData["FundingRate"] = self.data_for_RunDate['funding_rate']
        return [t_NewOpenComposition, t_OpenIndexSpecificData]


class DSVIXTRIndexCalculation:
    
     def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.ChildOpenComp = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.ChildCloseComp = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.IndexOpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
        self.data_for_RunDate = self.data_for_date(self.RunDate)
        self.data_for_LastRunDate = self.data_for_date(self.LastRunDate)
#        print(self.IndexSpecificData.loc[0,'IndexLevel'])
    
     def data_for_date(self, date):
         return {
                'funding_rate': self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)&(self.DailyPrices['SpecificInstrumentVTId']=='Ir:US:XX:XX:X:XXXX:FED1:XXXX:XX  '),'Price'].values[0]
                ,'IndexLevel': (lambda date: self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)&(self.DailyPrices['SpecificInstrumentVTId']=='Vi:US:XX:XX:X:XX:SVIX:XXXX:SG'),'Price'].values[0] if date==self.LastRunDate else 0)(date)
                ,'ChildLevel': (self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)
                                & (self.DailyPrices["GenericInstrumentVTId"]=="Vi:US:XX:XX:X:XX:IXER:XXXX:SG") ,"Price"]).values[0]
                ,'funding_days': (lambda date: hdd.no_of_days(pd.to_datetime(self.LastRunDate), pd.to_datetime(self.RunDate)) if date==self.LastRunDate else hdd.no_of_days(pd.to_datetime(self.RunDate), pd.to_datetime(self.OpenDate)))(date)
                }

     def OpenClose_data(self,state):
        _unit_data = (lambda state: self.data_for_LastRunDate if state=='close' else self.data_for_RunDate)(state)
        _child_cash_data = (lambda state: self.ChildCloseComp if state=='close' else self.ChildOpenComp)(state)
#        print(type(float(_child_cash_data(state).loc[_child_cash_data(state)['InstrumentVTId'].str.contains('Ca:EU'),'Units'].values[0])))
#        print(_child_cash_data.loc[_child_cash_data['InstrumentVTId'].str.contains('Ca:'),"Units"])
        _child_cash = _child_cash_data.loc[_child_cash_data['InstrumentVTId'].str.contains('Ca:'),"Units"]     
        units_child = hu.std_unit_calc(1, _unit_data['IndexLevel'],_unit_data['ChildLevel'],1)
#        print("_child_cash = ",_child_cash)
        Cash_child = units_child * _child_cash
        FED_funding = hf.simple_funding_cost_actbyX(_unit_data['funding_rate'],_unit_data['funding_days'],360)
        return {
                'units_child': units_child
                ,'Cash_child': Cash_child
                ,'FED_funding': FED_funding
                }     
    
     def IndexLevel_RunDate(self):
        self.data_for_RunDate['IndexLevel'] = self.NewCloseComposition[['Units','Price','FxRate']].product(axis=1).sum()
        print("Index Level  ---> "+str(self.data_for_RunDate['IndexLevel']))
        return self.data_for_RunDate['IndexLevel']
        
     @property
     def NewCloseComposition(self):
        t_NewCloseComposition = self.ChildCloseComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        _close_data = self.OpenClose_data('close')
#        print(_close_data['Cash_child'])
        t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units']*_close_data['units_child']
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = _close_data['Cash_child'] +_close_data['FED_funding']*self.data_for_LastRunDate["IndexLevel"]
        t_NewCloseComposition.loc[:,"IndexVTId"] = self.IndexVTId
#        print("_close_data['units_child'] ",_close_data['units_child']) 
#        t_NewCloseComposition.to_csv("close_composition.csv")
        return t_NewCloseComposition
     
     @hp.output_file(["VTCloseComposition"])
     def fn_VTCloseComp(self):
        return [self.NewCloseComposition]
    
     @hp.output_file(["VTOpenComposition"])
     def fn_VTOpenComp(self):
        IndexLevel_RunDate = self.IndexLevel_RunDate()
        t_NewOpenComposition = self.ChildOpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        _open_data = self.OpenClose_data('open')
#        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = _open_data['Cash_child']+_open_data['FED_funding']
        t_NewOpenComposition.set_index("InstrumentVTId", inplace = True)
        self.ChildOpenComp.set_index("InstrumentVTId", inplace = True)
#        print("_open_data['units_child'] = ",_open_data['units_child'])
        t_NewOpenComposition.loc[~t_NewOpenComposition.index.str.contains('Ca:'),'Units'] = self.ChildOpenComp.loc[~self.ChildOpenComp.index.str.contains('Ca:'),'Units']*_open_data['units_child']
        t_NewOpenComposition.loc[t_NewOpenComposition.index.str.contains('Ca:'),'Units'] = IndexLevel_RunDate - t_NewOpenComposition.loc[~t_NewOpenComposition.index.str.contains('Ca:'),['Units','Price','FxRate']].product(axis=1).sum()
        t_NewOpenComposition.reset_index(inplace = True)
        self.ChildOpenComp.reset_index(inplace = True)       
        t_NewOpenComposition.loc[:,"IndexVTId"] = self.IndexVTId
        return [t_NewOpenComposition]


class SILIndexCalculation:
    
    def __init__(self, cl_FileProcessor):
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"] = pd.to_datetime(self.DailyPrices["SettlementDate"])
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])        
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        self.NewCloseComposition = pd.DataFrame(columns = self.OpenComposition.columns)
        self.NewOpenComposition = pd.DataFrame(columns = self.OpenComposition.columns)        
        self.OpenIndexSpecificData = pd.DataFrame(columns = self.IndexSpecificData.columns)
        self.IndexLevel = 0
        self.RebalFlag = 0
        self.RebalDate = pd.to_datetime(self.IndexSpecificData["RebalanceDate"][0])
        self.NextRebalDate = pd.to_datetime(self.IndexSpecificData["NextRebalanceDate"][0])
        self.temp_df = pd.DataFrame()
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        
    
    def fn_HelperActiveContract(self):
        try:    
            self.log.processname_change("HelperActiveContract")
            
            if self.RunDate == self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX1"),"SettlementDate"].values[0]:
                self.Active1Run = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX2"),"SpecificInstrumentVTId"].values[0]
                self.Active2Run = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX3"),"SpecificInstrumentVTId"].values[0]
                self.Active1LastRun = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX1"),"SpecificInstrumentVTId"].values[0]
                self.Active2LastRun = self.Active1Run
            else:
                self.Active1Run = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX1"),"SpecificInstrumentVTId"].values[0]
                self.Active2Run = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX2"),"SpecificInstrumentVTId"].values[0]
                self.Active1LastRun = self.Active1Run
                self.Active2LastRun = self.Active2Run                
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
        
        
    def fn_SILRebalCheck(self):
        try:    
            self.log.processname_change("SILRebalCheck")
            if self.RunDate == self.NextRebalDate:
                self.RebalFlag = 1
                self.RebalDate = self.RunDate
    #            print(type(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SpecificInstrumentVTId"]==self.Active2Run),"SettlementDate"].values[0]))
                self.NextRebalDate = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SpecificInstrumentVTId"]==self.Active2Run),"SettlementDate"].values[0]) - dt.timedelta(1)
                while (self.NextRebalDate in list(self.HolidayCalendar["Date"])) or (self.NextRebalDate.weekday() in [5,6]):
                    self.NextRebalDate = self.NextRebalDate - dt.timedelta(1)

            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")  
            
        
    def fn_SILCloseComposition(self):
        try:    
            self.log.processname_change("SILCloseComposition")        
            print(self.OpenComposition["GenericTicker"].str.contains("Ca"))
            self.NewCloseComposition = self.OpenComposition.loc[~self.OpenComposition["GenericTicker"].str.contains("Ca"),:]
            self.NewCloseComposition.set_index("InstrumentVTId", inplace = True)
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")          
            
        
    def fn_SILCalculateIndexLevel(self):
        try:    
            self.log.processname_change("SILCalculateIndexLevel")
            df_tempRun = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SpecificInstrumentVTId"].isin([self.Active1LastRun, self.Active2LastRun])),:]
            df_tempLastRun = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.LastRunDate)&(self.DailyPrices["SpecificInstrumentVTId"].isin([self.Active1LastRun, self.Active2LastRun])),:]
            df_tempRun.set_index("SpecificInstrumentVTId", inplace = True)
            df_tempLastRun.set_index("SpecificInstrumentVTId", inplace = True)
            TransactionCost = self.IndexSpecificData["RebalancingTransactionCost"][0]
            IndexLevelLastRun = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.LastRunDate)&(self.DailyPrices["SpecificInstrumentVTId"]==self.IndexVTId),"Price"].values[0]
            self.IndexLevel = IndexLevelLastRun+sum(self.NewCloseComposition["Units"]*(-df_tempRun["Price"]+df_tempLastRun["Price"]))-TransactionCost
            print("self.IndexLevel = ",self.IndexLevel)
            CashUnits = self.IndexLevel-sum(self.NewCloseComposition["Units"]*self.NewCloseComposition["Price"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["GenericTicker"].str.contains("Ca"),:]
            df_Cash.loc[:,"Units"]= CashUnits
            self.NewCloseComposition.reset_index(inplace = True)
            self.NewCloseComposition = self.NewCloseComposition.append(df_Cash)
            self.NewCloseComposition.reset_index(inplace =  True, drop = True)
            self.NewCloseComposition.loc[:,"Date"] = self.RunDate
            self.log.process_success()

        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
    
    
    def fn_HelperDayCount(self, start_date, end_date):
        return(np.busday_count(start_date, end_date, weekmask='1111100', holidays=list(self.HolidayCalendar["Date"])))
        

    def fn_WeightCalculation(self):
        try:
            self.log.processname_change("WeightCalculation")
            self.temp_df = pd.DataFrame(index = [self.Active1Run, self.Active2Run])
            if self.RebalFlag==1:
                self.temp_df.loc[self.Active1Run,"Weight"] = 0
            else:
                self.temp_df.loc[self.Active1Run,"Weight"] = self.fn_HelperDayCount(self.RunDate, self.NextRebalDate)/self.fn_HelperDayCount(self.RebalDate, self.NextRebalDate) 
                
            self.temp_df.loc[self.Active2Run,"Weight"] = 1-self.temp_df.loc[self.Active1Run,"Weight"]
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
    
            
    def fn_SILIndexSpecificData(self):
        try:
            self.log.processname_change("SILIndexSpecificData")        
            self.OpenIndexSpecificData = self.IndexSpecificData.copy()
            if self.RebalFlag ==1:
                self.OpenIndexSpecificData.loc[:,"RebalanceDate"] = self.RebalDate
                self.OpenIndexSpecificData.loc[:,"NextRebalanceDate"] = self.NextRebalDate
            
            self.OpenIndexSpecificData.loc[:,"PriceDate"] = self.RunDate
            self.log.process_success()

        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
            
        
    def fn_SILOpenComposition(self):
        try:
            self.log.processname_change("SILOpenComposition")        
            
            for instrument in self.temp_df.index:
                self.temp_df.loc[instrument,"Price"] = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==instrument)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
                self.temp_df.loc[instrument,"GenericTicker"] = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==instrument)&(self.DailyPrices["PriceDate"]==self.RunDate),"GenericTicker"].values[0]
                self.temp_df.loc[instrument,"SpecificTicker"] = self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==instrument)&(self.DailyPrices["PriceDate"]==self.RunDate),"SpecificTicker"].values[0]

            self.temp_df["Units"] = self.IndexLevel*self.OpenIndexSpecificData["FinalExposure"].values[0]*self.OpenIndexSpecificData["Leverage"].values[0]*self.temp_df["Weight"]/sum(self.temp_df["Weight"]*self.temp_df["Price"])
            self.temp_df.reset_index(inplace = True)
            self.temp_df.rename(columns = {"index": "InstrumentVTId"}, inplace = True)
            self.NewOpenComposition = self.OpenComposition.loc[~self.OpenComposition["GenericTicker"].str.contains("Ca"),:].reset_index(drop = True)
            for columns in self.temp_df.columns:
                self.NewOpenComposition.loc[:,columns] = self.temp_df.loc[:, columns]
            CashUnits = self.IndexLevel - sum(self.NewOpenComposition["Price"]*self.NewOpenComposition["Units"])
            df_Cash = self.OpenComposition.loc[self.OpenComposition["GenericTicker"].str.contains("Ca"),:]
            df_Cash.loc[:,"Units"]= CashUnits
            self.NewOpenComposition = self.NewOpenComposition.append(df_Cash)
            self.NewOpenComposition.reset_index(inplace =  True, drop = True)
            self.NewOpenComposition.loc[:,"Date"] = self.RunDate
            self.log.process_success()
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
        

        
    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_HelperActiveContract()
        self.fn_SILRebalCheck()
        self.fn_SILCloseComposition()
        self.fn_SILCalculateIndexLevel()
        print("New Close Composition created")
        return [self.NewCloseComposition]        

    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_WeightCalculation()
        self.fn_SILIndexSpecificData()
        self.fn_SILOpenComposition()
        print("New Open Composition created") 
        return [self.NewOpenComposition, self.OpenIndexSpecificData] 


class IXERIndexCalculation(SILIndexCalculation):
    
    def __init__(self, cl_FileProcessor):
        super().__init__(cl_FileProcessor)
        self.UnderlyingCloseComposition = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.UnderlyingOpenComposition = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]


    def fn_DailyVariationCalculation(self):
        TermStructure = (self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==self.Active2LastRun)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0] - self.DailyPrices.loc[(self.DailyPrices["SpecificInstrumentVTId"]==self.Active1LastRun)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0])/20
        HistoricalPrices = self.DailyPrices.loc[(self.DailyPrices["IndexVTId"]==self.IndexVTId)&(~(self.DailyPrices["SpecificInstrumentVTId"]==self.IndexVTId)),:].pivot_table(index = "PriceDate", columns = "SpecificInstrumentVTId", values = "Price")
        LogReturn = HistoricalPrices.apply(lambda x: np.log(x/x.shift(1)), axis = 0)
        print(HistoricalPrices)
#        print(np.var(LogReturn.iloc[-10:,LogReturn.columns.str.contains("VIX")], ddof = 0).values[0])
#        print(np.cov(np.array(LogReturn[-10:]).transpose(), ddof=0)[0,1])
        Beta = np.cov(np.array(LogReturn[-10:]).transpose(), ddof=0)[0,1]/np.var(LogReturn.iloc[-10:,LogReturn.columns.str.contains("VIX")], ddof = 0).values[0]
        MomentumComponent = (HistoricalPrices.iloc[-16,LogReturn.columns.str.contains("VIX")].values[0] - HistoricalPrices.iloc[-1,LogReturn.columns.str.contains("VIX")].values[0])*Beta/15
#        print(TermStructure)
        print(MomentumComponent)
        print(Beta)
        return(TermStructure+MomentumComponent)

        
    def fn_LeverageCalculation(self):
        EDV_LastRun = self.IndexSpecificData.loc[:,"DailyUnitChange"].values[0]
        Leverage_LastRun = self.IndexSpecificData.loc[:,"Leverage"].values[0]
        Signal_LastRun = self.IndexSpecificData.loc[:,"Signal"].values[0]
#        print(EDV_LastRun)
#        print(Leverage_LastRun)
#        print(Signal_LastRun)
        if EDV_LastRun*Signal_LastRun > 0:
            return(min(1,max(0,Leverage_LastRun + 0.2)))
        else:
            return(min(1,max(0,Leverage_LastRun - 0.5)))
            
            
    def fn_SignalCalculation(self):
        Fut_t_1 = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SpecificInstrumentVTId"]==self.Active1Run),"Price"].values[0]
        Fut_t_2 =  self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["SpecificInstrumentVTId"]==self.Active2Run),"Price"].values[0]
        return(int(Fut_t_1<Fut_t_2))
            
    
    
    def fn_IXERIndexSpecificData(self):
        try:
            self.log.processname_change("IXERIndexSpecificData")        
        
            self.fn_SILIndexSpecificData()
            self.OpenIndexSpecificData.loc[:,"DailyUnitChange"] = self.fn_DailyVariationCalculation()
            self.OpenIndexSpecificData.loc[:,"Leverage"] = self.fn_LeverageCalculation()
            self.OpenIndexSpecificData.loc[:,"Signal"] = self.fn_SignalCalculation()
#            print(self.OpenIndexSpecificData.loc[:,"DailyUnitChange"])
#            print(self.OpenIndexSpecificData.loc[:,"Leverage"])
#            print(self.OpenIndexSpecificData.loc[:,"Signal"])
            self.log.process_success()            
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            


    def fn_IXERTransactionCost(self):
        try:
            self.log.processname_change("IXERTransactionCost")        
        
            if self.RunDate == self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="UX1"),"SettlementDate"].values[0]:
                RebalancingTransactionCost = self.OpenIndexSpecificData.loc[:,"Fee"].values[0]\
                                            * (abs(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==self.Active2Run,"Units"].values[0])\
                                               + abs(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==self.Active1Run,"Units"].values[0]\
                                                     - self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==self.Active1Run, "Units"].values[0]))
            else:
                RebalancingTransactionCost = self.OpenIndexSpecificData.loc[:,"Fee"].values[0]\
                                            * (abs(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==self.Active2Run,"Units"].values[0]\
                                                   - self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==self.Active2Run, "Units"].values[0])\
                                               + abs(self.NewOpenComposition.loc[self.NewOpenComposition["InstrumentVTId"]==self.Active1Run,"Units"].values[0]\
                                                     - self.NewCloseComposition.loc[self.NewCloseComposition["InstrumentVTId"]==self.Active1Run, "Units"].values[0]))
            self.OpenIndexSpecificData.loc[:,"RebalancingTransactionCost"] = RebalancingTransactionCost
            self.log.process_success()            
        except Exception as e:
            self.log.process_failure()
            self.log.error_reporting(e)
            raise Exception("Process ended with Failure")            
            


    @hp.output_file(["VTCloseComposition"])        
    def fn_VTCloseComp(self):
        self.fn_HelperActiveContract()
        self.fn_SILRebalCheck()
        self.fn_SILCloseComposition()
        self.fn_SILCalculateIndexLevel()
        print("New Close Composition created")
        return [self.NewCloseComposition]        

    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        self.fn_WeightCalculation()
        self.fn_IXERIndexSpecificData()
        self.fn_SILOpenComposition()
        self.fn_IXERTransactionCost()
        print("New Open Composition created") 
        return [self.NewOpenComposition, self.OpenIndexSpecificData]
    
    
class SGUSUICalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
#        print(self.HolidayCalendar["Date"])
#        in self.HolidayCalendar["Date"])
        
    def AnnDate(self):
        if self.RunDate in list(self.HolidayCalendar["Date"]):
            return True
        else:
            return False
        
    def Signal(self):
        if self.AnnDate():
            print(True)
            if self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FDTR Index") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] > self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FDTR Index") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]:
                return -1
            else:
                return 1
        else:
            return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Signal"].values[0]
    
    def MomLev(self):
        LastSig = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Signal"].values[0]
        LastML =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        print(LastSig)
        return  max(-4,min(4,LastML+0.5*LastSig))
    
    def IndexLevel(self):
        LastIL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOUS_UI") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        RunUL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLUS") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        LastRunUL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLUS") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        LastML =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        IndexLevel = round(LastIL*(((RunUL/LastRunUL)-1)*LastML+1),3)
        self.ULOpenUnits = (IndexLevel)/RunUL*self.MomLev()
#        print(LastML)
        return IndexLevel     

    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.ULOpenUnits
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Signal"] = self.Signal()
        print(self.Signal())
        OpenIndexSpecificData["Leverage"] = self.MomLev()
        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(),self.OpenIndexSpecificData()]
 
       
class SGMOUSCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.UnderlyingIndexSpecificData = cl_FileProcessor.dict_FileData["VTUnderlyingIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.UnderlyingOpenComp = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.UnderlyingCloseComp = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.UnderlyingIndexSpecificData["PriceDate"] = pd.to_datetime(self.UnderlyingIndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
         
    def RV(self):
        Fiftydays = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOUS_UI"),["PriceDate","Price"]]
        Fiftydays.sort_values(by = ["PriceDate"], ascending = True)
#        Fiftydays.dropna(axis=0,inplace=True)
        df = Fiftydays.tail(51)
        Return = df["Price"]/df["Price"].shift(1)-1
        print(df)
        print(round(np.std(np.array(Return[1:]),ddof = 1)*np.sqrt(250),7))
        return (round(np.std(np.array(Return[1:]),ddof = 1)*np.sqrt(250),7))
    
    def FThu(self,Date):
        y = pd.to_datetime(Date).year
        m = pd.to_datetime(Date).month
        c = calendar.Calendar(firstweekday=calendar.SATURDAY)
        monthcal = c.monthdatescalendar(y,m)
        first_thursday = [day for week in monthcal for day in week if day.weekday() == calendar.THURSDAY and day.month == m][0]
        return first_thursday + dt.timedelta(1)
        
    def Signal(self):
        curr_date = dt.datetime.strptime(str(self.RunDate),"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        prev_date = dt.datetime.strptime(str(self.LastRunDate),"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        if self.RunDate.month in [3,6,9,12]:
            if (str(self.FThu(self.RunDate)) <= curr_date) & (str(self.FThu(curr_date)) > prev_date):
                return 1
            elif self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] in [0,5]:
#                print("2")
                return 0
            else:
                return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] + 1
        else:
            if self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] in [0,5] :
                return 0
            else:
                return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] + 1

    def Leverage(self):
        TargetVol = 0.03
        RV_Two = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"VolatilityScaleLeverage"].values[0]
        LevO = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        Templv = min(TargetVol/RV_Two,2)
        return min(max(LevO-0.1,Templv),LevO+0.1)
    
    def RepCost(self):
        LastSig = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Signal"].values[0]
        Lev1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        Lev2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        IL1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0]
        IL2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"IndexLevel"].values[0]
        MomLev1 = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        MomLev2 = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
#        print(LastSig)
        if LastSig == 0:
            return abs(MomLev1*Lev1-(IL2/IL1)*MomLev2*Lev2)*0.00007
        else:
            return (((abs(Lev1*MomLev1)+abs(Lev2*MomLev2)*IL2/IL1)*(1/5)+abs(Lev1*MomLev1-Lev2*MomLev2*IL2/IL1)*(4/5))*0.00007)
    
    def PBCost(self):
        MomLev = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        Lev = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        FED = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FEDL01 Index") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        
        return (abs(MomLev)*Lev*0.0035*(int((self.RunDate-self.LastRunDate).days))/36000)*FED
       
    def UnderlyingIndexLevel(self) :
        return self.UnderlyingCloseComp[['Units','Price','FxRate']].product(axis=1).sum()
    
    def IndexLevel(self):
        IL = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0]
        Lev = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        UL = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0]
#        print(self.RepCost())
        IndexLevel = round(IL,3)*(1+Lev*((self.UnderlyingIndexLevel()/UL)-1)-float(self.RepCost())-float(self.PBCost()))
        self.ULOpenUnits = (IndexLevel)/(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOUS_UI") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0])*self.Leverage()
#        print(self.ULOpenUnits)
        return IndexLevel
                        
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.ULOpenUnits
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Signal"] = self.Signal()
        OpenIndexSpecificData["Leverage"] = self.Leverage()
        OpenIndexSpecificData["VolatilityScaleLeverage"] = self.RV()
        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(),self.OpenIndexSpecificData()]
        

    
class SGE2UICalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
#        print(self.HolidayCalendar["Date"])
        
    def AnnDate(self):
        if self.RunDate in list(self.HolidayCalendar["Date"]):
            return True
        else:
            return False
        
    def MomSignal(self):
        if self.AnnDate():
            AnnDatesList = self.HolidayCalendar.sort_values(by = ["Date"], ascending = True)["Date"]
            Prev_AnnDate = AnnDatesList[int(np.where(AnnDatesList==self.RunDate)[0])-1]
            Prev_AnnDate_Price = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLE2") & (self.DailyPrices["PriceDate"]== Prev_AnnDate),"Price"]
            Price_Run = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLE2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"]
            if (math.log(Price_Run)-math.log(Prev_AnnDate_Price)) > 0.0025:
                return -1
            else:
                return 1
        else:
            return 0
        
    def Signal(self):
        if self.AnnDate():
           if self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "EURR002W Index") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] > self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "EURR002W Index") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]:
               return -1
           else:
               return min(1, self.MomSignal())
        else:
            return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Signal"].values[0]
    
    def MomLev(self):
        LastSig = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Signal"].values[0]
        LastML =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
#        print(LastSig,LastML)
        return  max(-4,min(4,LastML+0.5*LastSig))
    
    def IndexLevel(self):
        LastIL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOE2_UI") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        RunUL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLE2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        LastRunUL = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXFLE2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        LastML =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        IndexLevel = round(LastIL*(((RunUL/LastRunUL)-1)*LastML+1),3)
        self.ULOpenUnits = (IndexLevel)/RunUL*self.MomLev()
        print(IndexLevel)
        return IndexLevel     

    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.ULOpenUnits
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Signal"] = self.Signal()
#        print(self.Signal())
        OpenIndexSpecificData["Leverage"] = self.MomLev()
        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(),self.OpenIndexSpecificData()]
 
       
class SGMOE2Calculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.UnderlyingIndexSpecificData = cl_FileProcessor.dict_FileData["VTUnderlyingIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.UnderlyingOpenComp = cl_FileProcessor.dict_FileData["VTUnderlyingOpenComposition"]
        self.UnderlyingCloseComp = cl_FileProcessor.dict_FileData["VTUnderlyingCloseComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.UnderlyingIndexSpecificData["PriceDate"] = pd.to_datetime(self.UnderlyingIndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
         
    def RV(self):
        Fiftydays = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOE2_UI"),["PriceDate","Price"]]
        Fiftydays.sort_values(by = ["PriceDate"], ascending = True)
        df = Fiftydays.tail(51)
        Return = df["Price"]/df["Price"].shift(1)-1

        return (round(np.std(np.array(Return[1:]),ddof = 1)*np.sqrt(250),7))
    
    def FThu(self,Date):
        y = pd.to_datetime(Date).year
        m = pd.to_datetime(Date).month
        c = calendar.Calendar(firstweekday=calendar.SATURDAY)
        monthcal = c.monthdatescalendar(y,m)
        first_thursday = [day for week in monthcal for day in week if day.weekday() == calendar.THURSDAY and day.month == m][0]
        return first_thursday + dt.timedelta(1)
        
    def Signal(self):
        curr_date = dt.datetime.strptime(str(self.RunDate),"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        prev_date = dt.datetime.strptime(str(self.LastRunDate),"%Y-%m-%d %H:%M:%S").strftime('%Y-%m-%d')
        if self.RunDate.month in [3,6,9,12]:
            if (str(self.FThu(self.RunDate)) <= curr_date) & (str(self.FThu(curr_date)) > prev_date):
                return 1
            elif self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] in [0,20]:
#                print("2")
                return 0
            else:
                return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] + 1
        else:
            if self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] in [0,20] :
                return 0
            else:
                return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'Signal'].values[0] + 1

    def Leverage(self):
        TargetVol = 0.03
        RV_Two = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"VolatilityScaleLeverage"].values[0]
        LevO = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        Templv = min(TargetVol/RV_Two,2)
        return round(min(max(LevO-0.1,Templv),LevO+0.1),5)
    
    def RepCost(self):
        LastSig = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Signal"].values[0]
        Lev1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        Lev2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        IL1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0]
        IL2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"IndexLevel"].values[0]
        MomLev1 = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        MomLev2 = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
#        print(LastSig,Lev1,Lev2,IL1,IL2,MomLev1,MomLev2)
#        print(abs(Lev1*MomLev1-Lev2*MomLev2*(IL2/IL1)*(4/5)*0.007))
        if LastSig == 0:
            return abs(MomLev1*Lev1-(IL2/IL1)*MomLev2*Lev2)*0.00007
        else:
            return (((abs(Lev1*MomLev1)+abs(Lev2*MomLev2)*IL2/IL1)*(1/20)+abs(Lev1*MomLev1-Lev2*MomLev2*IL2/IL1)*(19/20))*0.00007)
    
    def PBCost(self):
        MomLev = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        Lev = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] != self.LastRunDate),"Leverage"].values[0]
        FED = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "EONIA") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        
        return (abs(MomLev)*Lev*0.0035*(int((self.RunDate-self.LastRunDate).days))/36000)*FED
       
    def UnderlyingIndexLevel(self) :
        return self.UnderlyingCloseComp[['Units','Price','FxRate']].product(axis=1).sum()
    
    def IndexLevel(self):
        IL = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0]
        Lev = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Leverage"].values[0]
        UL = self.UnderlyingIndexSpecificData.loc[(self.UnderlyingIndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0]
#        print(IL,Lev,self.UnderlyingIndexLevel(),UL,self.RepCost(),self.PBCost())
        IndexLevel = round(IL,3)*(1+Lev*((self.UnderlyingIndexLevel()/UL)-1)-float(self.RepCost())-float(self.PBCost()))
        self.ULOpenUnits = (IndexLevel)/(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "SGIXMOE2_UI") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0])*self.Leverage()
        print(IndexLevel)
        return IndexLevel
                        
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.ULOpenUnits
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Signal"] = self.Signal()
        OpenIndexSpecificData["Leverage"] = self.Leverage()
        OpenIndexSpecificData["VolatilityScaleLeverage"] = self.RV()
        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(),self.OpenIndexSpecificData()]



class IXMGCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexOpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
    
    @property
    def NewCloseComposition(self):
        t_NewCloseComposition = self.IndexOpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        return t_NewCloseComposition
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        IndexLevel_RunDate = np.round(sum(self.NewCloseComposition['Units']*self.NewCloseComposition['Price']),3)
        t_NewOpenComposition = self.NewCloseComposition.copy()
        t_NewOpenComposition['Units'] = IndexLevel_RunDate*0.5/t_NewOpenComposition['Price']
        return [t_NewOpenComposition]

class MEUUCalculation:
            
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.RebalFlag=0
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.RunDate = cl_FileProcessor.RunDate    
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.ExRebal=hp.workday(dt.datetime(self.RunDate.year,self.RunDate.month,1),-1,list(self.HolidayCalendar["Date"]))
        self.rundate_pre=hp.workday(self.RunDate,-1,list(self.HolidayCalendar["Date"]))
        self.rundateprepre=hp.workday(self.RunDate,-2,list(self.HolidayCalendar["Date"]))
        OpenDate = self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0]
        self.OpenDate=pd.to_datetime(dt.datetime.strptime(str(OpenDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')) 
        self.NextRebalDate=hp.workday(dt.datetime(self.RunDate.year,self.RunDate.month,1)+dateutil.relativedelta.relativedelta(months=+1),-1,list(self.HolidayCalendar["Date"]))
        self.BondMCAP = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy','USD WMCO Curncy'],
             'InstrumentID': ['I02920US','I05486US','LSFATRUU','LBEATRUU','LC58TRUU','LG24TRUU','I02705US','I02915US','I02702US','LBUSTRUU']})
        self.EquityMCAP = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','EUR WMCO Curncy','EUR WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy','USD WMCO Curncy'],
             'InstrumentID': ['WCAUAUST','WCAUCAN','WCAUSWIT','WCAUFRAN','WCAUGERM','WCAUITAL','WCAUSPAI','WCAUUK','WCAUJAPA','WCAUNORW','WCAUNEWZ','WCAUSWED','WCAUUS']})
        
        self.EquityIL = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy','USD WMCO Curncy'],
             'InstrumentID': ['MSDLAS','MSDLCA','MSDLSZ','MSELEU','MSDLUK','MSDLJN','MSDLNO','MSDLNZ','MSDLSW','MSDLUS']})
        
        self.BondMTD = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy','USD WMCO Curncy'],
             'InstrumentID': ['I02920JP','I05486CA','LSFATRCU','LBEATREU','LC58TRGU','LG24TRJU','I02705EU','I02915JP','I02702EU','LBUSTRUU']})
        
        self.TOMNext = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy'],
             'InstrumentID': ['AUDTN BGN Curncy','CADTN BGN Curncy','CHFTN BGN Curncy','EURTN BGN Curncy','GBPTN BGN Curncy','JPYTN BGN Curncy','NOKTN BGN Curncy','NZDTN BGN Curncy','SEKTN BGN Curncy']})
        
        self.SpotNext = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy'],
             'InstrumentID': ['AUDSN BGN Curncy','CADSN BGN Curncy','CHFSN BGN Curncy','EURSN BGN Curncy','GBPSN BGN Curncy','JPYSN BGN Curncy','NOKSN BGN Curncy','NZDSN BGN Curncy','SEKSN BGN Curncy']})
        
        self.CurParam = pd.DataFrame({'GenericTicker': ['AUD WMCO Curncy','CAD WMCO Curncy','CHF WMCO Curncy','EUR WMCO Curncy','GBP WMCO Curncy','JPY WMCO Curncy','NOK WMCO Curncy','NZD WMCO Curncy','SEK WMCO Curncy','USD WMCO Curncy'],
             'Mult_Factor': [-1,1,1,-1,-1,1,1,-1,1,1],
             'Fwd_Mult_Factor':[0.0001,0.0001,0.0001,0.0001,0.0001,0.01,0.0001,0.0001,0.0001,0.0001],
             'Settlement_Conv':[1,0,1,1,1,1,1,1,1,1]})
        
    def ExtractInputs(self,InputDF,Date_List,Header_List,ColumnName):
        DF=pd.DataFrame(columns=Header_List)
        DF['PriceDate']=Date_List
        DF=DF.set_index('PriceDate')
        for x in Date_List:
            temp1=InputDF.loc[x,:]
            temp1=temp1.set_index('GenericTicker')
            for y in Header_List:
                try:
                    DF.loc[x,y]=temp1.loc[y,ColumnName]
                except:
                    pass
        DF.reset_index(inplace=True)
        DF.sort_values(by='PriceDate', ascending=True,inplace=True)
        DF.set_index('PriceDate',inplace=True) 
        DF.fillna(method='ffill',inplace=True)
        return DF         
    def DFStructuring(self):
        
        List1=list(self.IndexSpecificData['GenericTicker'])
        self.Interim=pd.DataFrame({'GenericTicker':List1})
        MCAP_List=list(self.BondMCAP['InstrumentID'])+list(self.EquityMCAP['InstrumentID'])
        MTD_List=list(self.BondMTD['InstrumentID'])
        PXLAST_List=list(self.EquityIL['InstrumentID'])+list(self.TOMNext['InstrumentID'])+list(self.SpotNext['InstrumentID'])+List1
        Dates=list(set(self.DailyPrices['PriceDate']))
        self.InputDF=self.DailyPrices
        self.ISpecific=self.IndexSpecificData.copy()
        self.CloseCompTemp=self.CloseComp.copy()
        
        self.EquityMCAP.set_index('InstrumentID',inplace=True)
        self.BondMCAP.set_index('InstrumentID',inplace=True)
        self.EquityIL.set_index('InstrumentID',inplace=True)
        self.BondMTD.set_index('InstrumentID',inplace=True)
        self.TOMNext.set_index('InstrumentID',inplace=True)
        self.SpotNext.set_index('InstrumentID',inplace=True)
        self.Interim.set_index('GenericTicker',inplace=True)
        self.CurParam.set_index('GenericTicker',inplace=True)
        self.ISpecific.set_index('GenericTicker',inplace=True)
        self.CloseCompTemp.set_index('SpecificTicker',inplace=True)
        self.InputDF.set_index('PriceDate',inplace=True)
        
        self.IndexID=self.ISpecific.loc['USD WMCO Curncy','IndexVTId']
        
        if self.IndexID=='Fi:US:XX:XX:X:XX:MEU4:XXXX:BR':
            Lev_Factor=4
        else:
            Lev_Factor=1
        
        MCAP=self.ExtractInputs(self.DailyPrices,Dates,MCAP_List,'MCAP')
        MTD=self.ExtractInputs(self.DailyPrices,Dates,MTD_List,'MTD')
        PXLAST=self.ExtractInputs(self.DailyPrices,Dates,PXLAST_List,'Price')
        
        self.EquityMCAP['MarketCap']=[MCAP.loc[self.ExRebal,x] for x in self.EquityMCAP.index]
        self.BondMCAP['MarketCap']=[MCAP.loc[self.ExRebal,x] for x in self.BondMCAP.index]
        self.BondMTD['MTD']=[MTD.loc[self.rundateprepre,x]/100 for x in self.BondMTD.index]
        self.EquityIL['Returns']=[(PXLAST.loc[self.rundateprepre,x]/PXLAST.loc[self.ExRebal,x]-1) for x in self.EquityIL.index]
        self.TOMNext['ForwardPoints']=[PXLAST.loc[self.RunDate,x] for x in self.TOMNext.index]
        self.SpotNext['ForwardPoints-1']=[PXLAST.loc[self.rundate_pre,x] for x in self.SpotNext.index]
        self.Interim['Spot-1']=[PXLAST.loc[self.rundate_pre,x] for x in self.Interim.index]
        self.Interim['Spot']=[PXLAST.loc[self.RunDate,x] for x in self.Interim.index]
        self.Interim.loc['USD WMCO Curncy','Spot']=1
        self.Interim.loc['USD WMCO Curncy','Spot-1']=1
        
        
        self.EquityMCAP.set_index('GenericTicker',inplace=True)
        self.BondMCAP.set_index('GenericTicker',inplace=True)
        self.EquityIL.set_index('GenericTicker',inplace=True)
        self.BondMTD.set_index('GenericTicker',inplace=True)
        self.TOMNext.set_index('GenericTicker',inplace=True)
        self.SpotNext.set_index('GenericTicker',inplace=True)
        
        self.CloseCash=self.OpenComp.loc[self.OpenComp['SpecificTicker'].str.contains('CON'),"Units"].values[0]
        if (self.OpenDate==self.NextRebalDate) or (self.RunDate==self.NextRebalDate):
            self.OpenCash=self.ISpecific.loc['USD WMCO Curncy','IndexLevel']
        else:
            self.OpenCash=self.CloseComp.loc[self.CloseComp['SpecificTicker'].str.contains('CON'),"Units"].values[0]
        
        if self.RunDate==self.NextRebalDate:
            
            self.RebalFlag=1
            TotSum=sum(self.BondMCAP['MarketCap'])+sum(self.EquityMCAP['MarketCap'])
            for y in self.Interim.index:
                self.Interim.loc[y,'Signal']=(self.EquityMCAP.apply(lambda x: x[x.index == y])['MarketCap'].sum()/TotSum)*self.EquityIL.loc[y,'Returns']+(self.BondMCAP.loc[y,'MarketCap']/TotSum)*self.BondMTD.loc[y,'MTD']
            for y in self.Interim.index:
                self.Interim.loc[y,'Long/Short']=np.sign(self.Interim.loc[y,'Signal']-self.Interim.loc['USD WMCO Curncy','Signal'])*-1
                try:
                    self.Interim.loc[y,'TomNext']=self.TOMNext.loc[y,'ForwardPoints']
                except:
                    self.Interim.loc[y,'TomNext']=1
                try:
                    self.Interim.loc[y,'SpotNext']=self.SpotNext.loc[y,'ForwardPoints-1']
                except:
                    self.Interim.loc[y,'SpotNext']=1
            
            for y in self.Interim.index:
                if self.CurParam.loc[y,'Settlement_Conv']==1:
                    self.Interim.loc[y,'Exposure']=((self.Interim.loc[y,'Spot-1']/(self.Interim.loc[y,'Spot']-self.Interim.loc[y,'TomNext']*self.CurParam.loc[y,'Fwd_Mult_Factor']))**self.CurParam.loc[y,'Mult_Factor']-1)*self.Interim.loc[y,'Long/Short']
                else:
                    self.Interim.loc[y,'Exposure']=(((self.Interim.loc[y,'Spot-1']+self.Interim.loc[y,'SpotNext']*self.CurParam.loc[y,'Fwd_Mult_Factor'])/(self.Interim.loc[y,'Spot']))**self.CurParam.loc[y,'Mult_Factor']-1)*self.Interim.loc[y,'Long/Short']
        else:
            for y in self.Interim.index:
                self.Interim.loc[y,'Exposure']=self.ISpecific.loc[y,'CurrencyExposure']
        for y in self.Interim.index:
            self.Interim.loc[y,'FX_Rate']=self.CloseCompTemp.loc[y,'FxRate']
            
        for y in self.Interim.index:
            self.Interim.loc[y,'Units']=Lev_Factor*self.CloseCash*(self.Interim.loc[y,'Exposure']/(self.Interim.loc[y,'Spot']*self.Interim.loc[y,'FX_Rate']*9))
        for y in self.Interim.index:
            self.Interim.loc[y,'Net Exposure']=self.Interim.loc[y,'Units']*(self.Interim.loc[y,'Spot']*self.Interim.loc[y,'FX_Rate'])
        self.IndexLevel=round(sum(list(self.Interim['Net Exposure']))+self.CloseCash,4)
        print(self.Interim)
        return self.Interim
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel
        OpenIndexSpecificData["Rebalance Flag"] = self.RebalFlag
        OpenIndexSpecificData.set_index('GenericTicker',inplace=True)
        for y in self.Interim.index:
            OpenIndexSpecificData.loc[y,"CurrencyExposure"] = self.Interim.loc[y,'Exposure']
        OpenIndexSpecificData.reset_index(inplace=True)
        return OpenIndexSpecificData
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        t_NewCloseComposition.set_index('SpecificTicker',inplace=True)
        for x in self.Interim.index:
            t_NewCloseComposition.loc[x,'Units']=self.Interim.loc[x,'Units']
        t_NewCloseComposition.loc[t_NewCloseComposition.index.str.contains('CON'),"Units"]=self.CloseCash    
        t_NewCloseComposition.reset_index(inplace=True)
        print(t_NewCloseComposition[["SpecificTicker","Price","FxRate"]])
        return t_NewCloseComposition
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.DFStructuring()
        return [self.NewCloseComposition()]
    

    
    @hp.output_file(["VTOpenComposition","VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        t_NewOpenComposition = self.NewCloseComposition().copy()
        t_NewOpenComposition.set_index('SpecificTicker',inplace=True)
        t_NewOpenComposition.loc[t_NewOpenComposition.index.str.contains('CON'),"Units"]=self.OpenCash         
        t_NewOpenComposition.reset_index(inplace=True)
        return [t_NewOpenComposition,self.OpenIndexSpecificData()]
        

class NomuraSelectIndexCalculation_Cmdty:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.Indicators = cl_FileProcessor.dict_FileData["VTIndicatorData"]
        self.LongReturns = cl_FileProcessor.dict_FileData["VTReturnData"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.Indicators["Date"] = pd.to_datetime(self.Indicators["Date"])
        self.LongReturns["Date"] = pd.to_datetime(self.LongReturns["Date"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
        self.PrevRoll_1 = hp.workday(dt.datetime(self.RunDate.year,self.RunDate.month,1)+dateutil.relativedelta.relativedelta(months=+1),-1,list(self.HolidayCalendar["Date"]))

    @property
    def IsWeekSecondDay(self):
        LastRunDate_1 = hp.workday(self.LastRunDate,-1,list(self.HolidayCalendar["Date"]))
        if LastRunDate_1.weekday() > self.LastRunDate.weekday():
            return True
        else:
            return False
    
    @property
    def RollFlag(self):
        LastBDay = hp.workday(dt.datetime(self.RunDate.year,self.RunDate.month,1)+dateutil.relativedelta.relativedelta(months=+1),-1,list(self.HolidayCalendar["Date"]))
        LastBDay_2 = hp.workday(LastBDay,-2,list(self.HolidayCalendar["Date"]))
        if self.RunDate == LastBDay_2:
            return 1
        else:
            return 0
        
    def RollWeight(self, date):
        LastBDay = hp.workday(dt.datetime(date.year,date.month,1)+dateutil.relativedelta.relativedelta(months=+1),-1,list(self.HolidayCalendar["Date"]))
        LastBDay_1 = hp.workday(LastBDay,-1,list(self.HolidayCalendar["Date"]))
        LastBDay_2 = hp.workday(LastBDay,-2,list(self.HolidayCalendar["Date"]))
        if date == LastBDay_2:
            return 0
        elif date == LastBDay_1:
            return (1/3)
        elif date == LastBDay:
            return (2/3)
        else:
            return 1
    
    def LongReturn(self, Cmdty):
        RunDate_1 = hp.workday(self.RunDate,-1)
        RollDate = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate) & (self.IndexSpecificData["CommodityTicker"]==Cmdty),"RollDate"].values[0]
        RollDate = pd.to_datetime(dt.datetime.strptime(str(RollDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d'))
#        print(RollDate)
        RollDate_1 = hp.workday(RollDate,-1)
        CurrRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        CurrLastRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RunDate_1) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        CurrRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        NextRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        NextLastRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RunDate_1) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        NextRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        if self.RollWeight(self.RunDate) == 1:
            return (CurrRunPrice-CurrLastRunPrice)/CurrRoll_1Price
        else:
            return ((1-self.RollWeight(self.RunDate))*((CurrRunPrice-CurrLastRunPrice)/CurrRoll_1Price) + self.RollWeight(self.RunDate)*((NextRunPrice-NextLastRunPrice)/NextRoll_1Price))

    def LongReturn_Daily(self, Cmdty):
        RollDate = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate) & (self.IndexSpecificData["CommodityTicker"]==Cmdty),"RollDate"].values[0]
        RollDate = pd.to_datetime(dt.datetime.strptime(str(RollDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d'))
        RollDate_1 = hp.workday(RollDate,-1,list(self.HolidayCalendar["Date"]))
        CurrRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        CurrLastRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.LastRunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        CurrRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        NextRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        NextLastRunPrice = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.LastRunDate) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        NextRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        if self.RollWeight(self.RunDate) == 1:
            return (CurrRunPrice-CurrLastRunPrice)/CurrRoll_1Price
        else:
            return ((1-self.RollWeight(self.RunDate))*((CurrRunPrice-CurrLastRunPrice)/CurrRoll_1Price) + self.RollWeight(self.RunDate)*((NextRunPrice-NextLastRunPrice)/NextRoll_1Price))

    def Charges(self, Cmdty):
        RollDate = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate) & (self.IndexSpecificData["CommodityTicker"]==Cmdty),"RollDate"].values[0]
        RollDate = pd.to_datetime(dt.datetime.strptime(str(RollDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d'))
#        print(RollDate)
        RollDate_1 = hp.workday(RollDate,-1,list(self.HolidayCalendar["Date"]))
        CurrRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
        NextRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==RollDate_1) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
        TradeSignal_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "Signal"].values[0]
        TradeSignal_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]!=self.LastRunDate), "Signal"].values[0]
        VolScale_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "Leverage"].values[0]
        VolScale_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]!=self.LastRunDate), "Leverage"].values[0]
        Charge_Prev = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "FundingRate"].values[0]
        Charge_Curr = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "Fee"].values[0]
        RollWeight = self.RollWeight(self.RunDate)
        RollWeight_1 = self.RollWeight(self.LastRunDate)
        if RollWeight == 0:
            RollWeight=1
        if round(Charge_Prev,10) == round(Charge_Curr,10):
            return abs(TradeSignal_1*VolScale_1-TradeSignal_2*VolScale_2)*Charge_Prev
        else:
#            print(Charge_Prev,Charge_Curr,Cmdty)
            return abs(TradeSignal_1*VolScale_1*RollWeight-TradeSignal_2*VolScale_2*RollWeight_1)* + Charge_Prev +\
                   abs(TradeSignal_1*VolScale_1*(1-RollWeight)-TradeSignal_2*VolScale_2*(1-RollWeight_1))*Charge_Curr            
                
    def ExpStdDev(self, Cmdty):
        LongRetCmdty = self.LongReturns.loc[(self.LongReturns["Ticker"]==Cmdty) & (self.LongReturns["Date"]<self.RunDate) & (self.LongReturns["IsLongReturn"]==True),["Date","ReturnValue"]]
        LongRetCmdty.sort_values(by = ["Date"], ascending = True, inplace = True)
        LongRetCmdty = LongRetCmdty.tail(750)
        LongRetCmdty.reset_index(inplace = True)
        LongRetCmdty["ExpWeight"] = [(0.5)**((749-x)/249)/315.138843534707 for x in LongRetCmdty.index]
        LongRetCmdty["StdDev"] = LongRetCmdty["ExpWeight"]*((LongRetCmdty["ReturnValue"] - sum(LongRetCmdty["ReturnValue"]*LongRetCmdty["ExpWeight"]))**2) 
        return math.sqrt(sum(LongRetCmdty["StdDev"]))
    
    def VolScale(self, Cmdty):
        VolCap = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"] == self.LastRunDate,["VolScaleCap","CommodityTicker"]]
        VolCap_Comdty = VolCap.loc[VolCap["CommodityTicker"]==Cmdty,"VolScaleCap"].values[0]
        return min((0.0175)/self.ExpStdDev(Cmdty),VolCap_Comdty)

    def Indicator(self, days, Cmdty):
        Returns = self.LongReturns.loc[(self.LongReturns["Ticker"]==Cmdty) & (self.LongReturns["Date"]<self.RunDate) & (self.LongReturns["IsLongReturn"]==True),["Date","ReturnValue"]]
        Returns.sort_values(by = ["Date"], ascending = True, inplace = True)
#        LastDayReturn = Returns.tail(1).copy()
#        LastDayReturn["Date"] = self.RunDate
#        LastDayReturn.loc[:,"ReturnValue"] = self.LongReturn(Cmdty)
#        Returns = Returns.append(LastDayReturn)
        V_Indicator = Returns.tail(days)
        Lamda = math.exp(math.log(0.5)/(days-1))
        V_Indicator.set_index("Date",inplace = True)
        V_Indicator.reset_index(inplace = True)
        V_Indicator.loc[:,"ExpW"] = [(V_Indicator.loc[x,"ReturnValue"])*((Lamda)**(days-x-1)) for x in V_Indicator.index]      
        return (sum(V_Indicator["ExpW"]))

    def Percentile(self,ser, q):
        ser_sorted = ser.sort_values()
        rank = q * (len(ser) - 1)
        assert rank > 0
        rank_l = int(rank)
        return ser_sorted.iat[rank_l] + (ser_sorted.iat[rank_l + 1] -ser_sorted.iat[rank_l]) * (rank - rank_l)
    
    def LowerUpperBound(self,indicator, Cmdty):
        v_indicators = self.Indicators.loc[(self.Indicators["Ticker"]==Cmdty) & (self.Indicators["Date"]<=self.RunDate),indicator].dropna()
        LowerBound = self.Percentile(v_indicators, .05)
        UpperBound = self.Percentile(v_indicators, .95)
        return (LowerBound,UpperBound)
    
    def TermSignal(self,indicator, Cmdty):
        V_indicators = self.Indicators.loc[(self.Indicators["Ticker"]==Cmdty) & (self.Indicators["Date"]<self.RunDate),["Date", indicator]].dropna()
        LastDayIndicator = V_indicators.tail(1).copy()
        LastDayIndicator["Date"] = self.RunDate
        if indicator == "Indicator1Value":
            LastDayIndicator[indicator] = self.Indicator(66,Cmdty)
        elif indicator == "Indicator2Value":
            LastDayIndicator[indicator] = self.Indicator(132,Cmdty)
        elif indicator == "Indicator3Value":
            LastDayIndicator[indicator] = self.Indicator(250,Cmdty)
        V_indicators = V_indicators.append(LastDayIndicator)
        latest_V = V_indicators.sort_values(by = ["Date"], ascending = True)[indicator].tail(1).values[0]
        LowerBound = self.LowerUpperBound(indicator, Cmdty)[0]
        UpperBound = self.LowerUpperBound(indicator, Cmdty)[1]
        final_indicators = [x for x in V_indicators[indicator] if x>LowerBound and x<UpperBound]
        return ((latest_V-np.average(final_indicators))/np.std(final_indicators,ddof=1))
    
    def MomSignal(self, Cmdty):
        return((min(1,max(-1,self.TermSignal("Indicator1Value", Cmdty)))+min(1,max(-1,self.TermSignal("Indicator2Value", Cmdty)))+min(1,max(-1,self.TermSignal("Indicator3Value", Cmdty))))/3)
    
    def MomTradeSignal(self, Cmdty):
        if abs(self.MomSignal(Cmdty)) >= 0.65 :
            return self.MomSignal(Cmdty)
        else:
            return 0
                
    def DailyReturn(self, Cmdty):
        TradeSignal_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "Signal"].values[0]
        VolScale_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "Leverage"].values[0]
        print(Cmdty,self.Charges(Cmdty))
        return TradeSignal_1*VolScale_1*self.LongReturn_Daily(Cmdty)-self.Charges(Cmdty)
    
    def IndexLevel(self):
        RebalDate = self.IndexSpecificData["RebalanceDate"].values[0]
        RebalDate = pd.to_datetime(dt.datetime.strptime(str(RebalDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d'))
        RunDatePlus1 = hp.workday(RebalDate,1,list(self.HolidayCalendar["Date"]))
#        print(RunDatePlus1)
        self.ILevel = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,["CommodityTicker","Weights"]]
        PrevDailyReturns = self.LongReturns.loc[(self.LongReturns["IsLongReturn"]==False),["Date","Ticker","ReturnValue"]]
        self.ILevel["DailyReturn_Prev"] = [sum(PrevDailyReturns.loc[PrevDailyReturns["Ticker"]==Cmdty,"ReturnValue"]) for Cmdty in self.ILevel["CommodityTicker"]]
        self.ILevel["DailyReturn_Run"] = [self.DailyReturn(Cmdty) for Cmdty in self.ILevel["CommodityTicker"]]
        if self.RunDate == RunDatePlus1:
            self.ILevel["DailyReturn_Final"] = self.ILevel["DailyReturn_Run"]
        else:
            self.ILevel["DailyReturn_Final"] = self.ILevel["DailyReturn_Prev"] + self.ILevel["DailyReturn_Run"]
        ILRebal = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevelRebal"].values[0]
#        print(ILRebal)
        return ILRebal*(1+self.ILevel[["Weights", "DailyReturn_Final"]].product(axis=1).sum())
        
    def NewCloseComposition(self):
        ILRebal = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevelRebal"].values[0]
        IndexLevelRun = self.IndexLevel()
        print("IndexLevel ---->"+str(IndexLevelRun))
        self.ILevel["Price"] = [self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0] for Cmdty in self.ILevel["CommodityTicker"]]
        self.ILevel["SpecificTicker"] = [self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"SpecificTicker"].values[0] for Cmdty in self.ILevel["CommodityTicker"]]
        self.ILevel["SpecificInstrumentVTId"] = [self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"SpecificInstrumentVTId"].values[0] for Cmdty in self.ILevel["CommodityTicker"]]
        self.ILevel["Units"] = ILRebal*self.ILevel["Weights"]*self.ILevel["DailyReturn_Final"]/self.ILevel["Price"]
        t_NewCloseComposition = self.OpenComposition.loc[~self.OpenComposition["SpecificTicker"].str.contains("Ca"),:].copy().reset_index()
        t_NewCloseComposition["SpecificTicker"] = self.ILevel["SpecificTicker"]
        t_NewCloseComposition["InstrumentVTId"] = self.ILevel["SpecificInstrumentVTId"]
        t_NewCloseComposition["Price"] = self.ILevel["Price"]
        t_NewCloseComposition["Units"] = self.ILevel["Units"]
        Cash = self.OpenComposition.loc[self.OpenComposition["SpecificTicker"].str.contains("Ca"),:].copy().reset_index()
        Cash["Units"] = ILRebal
        t_NewCloseComposition = t_NewCloseComposition.append(Cash)
        t_NewCloseComposition["Date"] = self.RunDate
        return t_NewCloseComposition
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        if self.IsWeekSecondDay:
            for Cmdty in set(list(OpenIndexSpecificData["CommodityTicker"])):
                OpenIndexSpecificData.loc[OpenIndexSpecificData["CommodityTicker"]==Cmdty,"Leverage"] = Decimal(self.VolScale(Cmdty))
                OpenIndexSpecificData.loc[OpenIndexSpecificData["CommodityTicker"]==Cmdty,"Signal"] = Decimal(self.MomTradeSignal(Cmdty))
        if self.LastRunDate.month != self.RunDate.month:
            OpenIndexSpecificData["RebalanceDate"] = self.RunDate
        if self.RollFlag == 1:
            for Cmdty in set(list(OpenIndexSpecificData["CommodityTicker"])):
#                RollDate = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate) & (self.IndexSpecificData["CommodityTicker"]==Cmdty),"RollDate"].values[0]
#                RollDate = pd.to_datetime(dt.datetime.strptime(str(RollDate)[:19],"%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d'))
#                RollDate_1 = hp.workday(RollDate,-1,list(self.HolidayCalendar["Date"]))
                CurrRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.LastRunDate) & (self.DailyPrices["IsCurrentMonth"]==True),"Price"].values[0]
                NextRoll_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"].str.contains(Cmdty)) & (self.DailyPrices["PriceDate"]==self.LastRunDate) & (self.DailyPrices["IsCurrentMonth"]!=True),"Price"].values[0]
                ChargeA = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "ChargeA"].values[0]
                ChargeB = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "ChargeB"].values[0]
                ContractSize = self.IndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (self.IndexSpecificData["PriceDate"]==self.LastRunDate), "ContractSize"].values[0]
                OpenIndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (OpenIndexSpecificData["PriceDate"]==self.RunDate), "Fee"] = Decimal(ChargeA+(ChargeB/(NextRoll_1Price*ContractSize)))
                OpenIndexSpecificData.loc[(self.IndexSpecificData["CommodityTicker"]==Cmdty) & (OpenIndexSpecificData["PriceDate"]==self.RunDate), "FundingRate"] = Decimal(ChargeA+(ChargeB/(CurrRoll_1Price*ContractSize)))
                OpenIndexSpecificData["RollDate"] = self.RunDate   
#                print(Cmdty,CurrRoll_1Price,NextRoll_1Price)    
        return OpenIndexSpecificData
        
    def OpenIndicators(self):
        LatestDate = list(self.Indicators.sort_values(by = ["Date"], ascending = True)["Date"])[-1]
        OpenIndicators = self.Indicators.loc[self.Indicators["Date"]==LatestDate,:].copy()
        if self.IsWeekSecondDay:
            OpenIndicators["Date"] = self.RunDate
            for Cmdty in set(list(OpenIndicators["Ticker"])): 
                OpenIndicators.loc[OpenIndicators["Ticker"]==Cmdty,"Indicator1Value"] = Decimal(self.Indicator(66, Cmdty))
                OpenIndicators.loc[OpenIndicators["Ticker"]==Cmdty,"Indicator2Value"] = Decimal(self.Indicator(132, Cmdty))
                OpenIndicators.loc[OpenIndicators["Ticker"]==Cmdty,"Indicator3Value"] = Decimal(self.Indicator(250, Cmdty))
        return OpenIndicators        
    
    def OpenLongReturns(self):
        OpenLR = self.LongReturns.loc[self.LongReturns["Date"]==self.LastRunDate,:].copy()
        OpenLR["Date"] = self.RunDate
        for Cmdty in set(list(OpenLR["Ticker"])):
            OpenLR.loc[(OpenLR["IsLongReturn"]==True) & (OpenLR["Ticker"]==Cmdty),"ReturnValue"] = Decimal(self.LongReturn(Cmdty))
            OpenLR.loc[(OpenLR["IsLongReturn"]==False) & (OpenLR["Ticker"]==Cmdty),"ReturnValue"] = Decimal(self.DailyReturn(Cmdty))
        return OpenLR
        
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
#        self.LongReturn_Daily("LC")
#        Charges = [self.Charges(x) for x in set(list(self.IndexSpecificData["CommodityTicker"]))]
#        Charges = pd.DataFrame(Charges,index = set(list(self.IndexSpecificData["CommodityTicker"])),columns=["Charges"])
#        Charges.to_csv("NomuraCharges.csv")

#        print(self.OpenIndicators())
        return [self.NewCloseComposition()]

        
    @hp.output_file(["VTOpenComposition","VTReturnData", "VTIndicatorData", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
#        pass
        t_NewOpenComposition = self.NewCloseComposition()
        t_Returns = self.OpenLongReturns()
        t_indicators = self.OpenIndicators()
        t_indexspecificdata = self.OpenIndexSpecificData()
        return [t_NewOpenComposition, t_Returns, t_indicators, t_indexspecificdata]

class SGIF4FXCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexCloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.IndexOpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        #        self.IndexCloseComp.to_csv("soft_gen_tan.csv")
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate

    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        self.IndexCloseComp["Date"] = self.RunDate
        return [self.IndexCloseComp]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        self.IndexOpenComp["Date"] = self.RunDate
        return [self.IndexOpenComp]

class JGCTRCCECalculations:
    def __init__(self, cl_FileProcessor):
        self.directory=cl_FileProcessor.directory
#        print("Directory name",self.directory)
        self.IndexVTId=cl_FileProcessor.IndexVTId
#        print("IndexVTId is",self.IndexVTId)
        self.log=cl_FileProcessor.log
        self.DailyPrices=cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.IndexSpecificData["PriceDate"]=pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
#        print("RunDate is ",self.RunDate)
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        print("LastRunDate is",self.LastRunDate)
#        print("LastRunDate type:",type(self.LastRunDate))
    
    def Return(self):
        t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="JGCTRCCU") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        t_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="JGCTRCCU") & (self.DailyPrices["PriceDate"]!=self.RunDate),"Price"].values[0]
        return  t_Price/t_1Price-1
       
    def Fx_Return(self):
        Fx_t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        Fx_t_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]!=self.RunDate),"Price"].values[0]
        return  Fx_t_1Price/Fx_t_Price
                               
    def IndexLevel(self):
        IL = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexLevel"].values[0]
#        print("IndexLevel Value:",(1+self.Return()*self.Fx_Return())*IL)
        return (1+self.Return()*self.Fx_Return())*IL

    def CloseUnits(self):
        
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0]
#        print("In close units value of IndexLevel is:",IL_1)
        t_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="JGCTRCCU") & (self.DailyPrices["PriceDate"]!=self.RunDate),"Price"].values[0]
#        print("t_1Price",t_1Price)
        Fx_t_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]!=self.RunDate),"Price"].values[0]
        Fx_t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
#        print("Fx_t_1Price",Fx_t_1Price)
#        print("Fx_t_Price",Fx_t_Price)
#        print("Close Units value:",IL_1/t_1Price*Fx_t_1Price/Fx_t_Price**2)
        return ((IL_1/t_1Price)*(Fx_t_1Price/Fx_t_Price**2))
#        return (IL_1/t_1Price*Fx_t_1Price/Fx_t_Price)*Fx_t_Price                 
    
    def OpenUnits(self):
        t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="JGCTRCCU") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        Fx_t_1Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]!=self.RunDate),"Price"].values[0]
        Fx_t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
#        print("Open Units Value:",(self.IndexLevel()/t_Price)*(Fx_t_1Price/Fx_t_Price**2))
        return (self.IndexLevel()/t_Price)*(Fx_t_1Price/Fx_t_Price**2)
        
        
       
       
    def CashUnits(self,IL_1,df):
        Fx_t_Price = self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="EUR WMCO Curncy") & (self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
#        print("Cash Units",(IL_1-df[['Units','Price','FxRate']].product(axis=1).sum())/Fx_t_Price)
#        print("Cash df",df)
        return (IL_1-df[['Units','Price','FxRate']].product(axis=1).sum())/Fx_t_Price
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CloseUnits()
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.OpenUnits()
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
#    def OpenIndexSpecificData(self):
#        OpenIndexSpecificData = self.IndexSpecificData.copy()
#        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
#        OpenIndexSpecificData["PriceDate"] = self.RunDate
#        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()

#        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition()]
        
class CIIRMAGMCalculation:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
        
    def IndexLevel(self):
        PD1 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACE") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        PD2 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACG") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACG") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        PD3 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACU") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIIRMACU") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        IL_1 = (self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0])        
        IL = PD1 * (self.OpenComp.loc[(self.OpenComp["GenericTicker"] == "CIIRMACE") ,["FxRate","Units"]].product(axis=1).values[0]) \
            + PD2 * (self.OpenComp.loc[(self.OpenComp["GenericTicker"] == "CIIRMACG") ,["FxRate","Units"]].product(axis=1).values[0]) \
            + PD3 * (self.OpenComp.loc[(self.OpenComp["GenericTicker"] == "CIIRMACU") ,["FxRate","Units"]].product(axis=1).values[0]) \
            + IL_1

        return IL
    
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum() 
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        IL_1 = (self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"IndexLevel"].values[0])        
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = IL_1/(3* t_NewOpenComposition[['Price','FxRate']].product(axis=1))
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition()]

class JTRDX2BUCalculation:
    def __init__(self,cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.Returns = cl_FileProcessor.dict_FileData["VTReturnData"]
        self.Returns["Date"]=pd.to_datetime(self.Returns["Date"])
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]    
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
    
    def Ret(self,GenTicker):
        P_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        P_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
        return (P_t/P_t_1-1)*1/7

    def AggregateRetrn(self):
        Sum_1=map(self.Ret,["JTRDAFXU","JTRDCFXU","JTRDEFXU","JTRDGFXU","JTRDHFXU","JTRDJFXU","JTRDNFXU"])
        a=list(Sum_1)
        return np.sum(a)
        
    def stdDev(self,val):
        st_1=[j for i,j in enumerate(self.Returns.sort_values(by="Date",ascending=True).tail(val+5).head(val)["ReturnValue"])]
        return np.std(st_1,ddof=1)*np.sqrt(252)
    
    def TargetExp(self):
        Min_Exp=0
        Max_Exp=2
        VT=0.04
        return max(Min_Exp,min(Max_Exp,VT/(max(self.stdDev(66),self.stdDev(22)))))
    def RollDate(self):
        return 1 if self.LastRunDate.month!=self.RunDate.month else 0
    
    def IndexNotional(self):
        if self.RollDate==1:
            return self.IndexSpecificData.sort_values(by="PriceDate",ascending=True).tail(4).head(1)["IndexLevel"].values[0]
        else:
            return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexNotional"].values[0]

    def IndexLevel(self):
        IL_1=self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexLevel"].values[0]
        return IL_1+self.IndexNotional()*self.AggregateRetrn()*self.TargetExp()
        
    def Price(self,GenTicker):
        return self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
        
    def NewCloseComposition(self):
        IL_1=self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"IndexLevel"].values[0]
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        #UnderLying Index Units apart from Cash
        for i in list(t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),"SpecificTicker"]):
            t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:') & (t_NewCloseComposition["SpecificTicker"]==i),'Units']=self.TargetExp()*self.IndexNotional()/(7*self.Price(i))
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] =IL_1-self.TargetExp()*self.IndexNotional()
        return t_NewCloseComposition
        
    def OpenLongReturns(self):
        OpenLR = self.Returns.loc[self.Returns["Date"]==self.LastRunDate,:].copy()
        OpenLR["Date"] = self.RunDate
        OpenLR.loc[(OpenLR["Date"]==self.RunDate),["ReturnValue"]]=self.AggregateRetrn()
        return OpenLR
        
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["IndexNotional"]=self.IndexNotional()
        return OpenIndexSpecificData
       
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition","VTReturnData", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
#        pass
        t_NewOpenComposition = self.NewCloseComposition()
        t_Returns = self.OpenLongReturns()
        t_indexspecificdata = self.OpenIndexSpecificData()
        return [t_NewOpenComposition, t_Returns, t_indexspecificdata]

class CIEQVUIDCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
        
    def IndexLevel(self):
        PD1 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI1") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI1") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        PD2 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        PD3 = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI3") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]) - (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "CIEQVUI3") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0])
        
        IL_1 = (self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0])        
        IL = PD1 * (self.OpenComp.loc[(self.OpenComp["SpecificTicker"] == "CIEQVUI1") ,"Units"].values[0]) \
            + PD2 * (self.OpenComp.loc[(self.OpenComp["SpecificTicker"] == "CIEQVUI2") ,"Units"].values[0]) \
            + PD3 * (self.OpenComp.loc[(self.OpenComp["SpecificTicker"] == "CIEQVUI3") ,"Units"].values[0]) \
            + IL_1
        
        return IL
    
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price']].product(axis=1).sum() 
    
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
        
    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        if self.OpenDate.year != self.RunDate.year :
            t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] =  self.IndexLevel/(3* t_NewOpenComposition[['Price']])
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition()]
        
class SGIXUSGC_IndexCal:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
#        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.IndexSpecificData["RollDate"] = pd.to_datetime(self.IndexSpecificData["RollDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])

    def HolidayList(self):
        try:
            return list(pd.to_datetime(self.HolidayCalendar["Date"]))
        except:
            return list()
            
    def NextRoll(self):
        '''Need to include Holidays'''
        RollDates = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,["RollIdentifier","RollDate","FinalExposure"]]
        RollDates["VS"],RollDates["int"] = RollDates.RollIdentifier.str.split('_',1).str
        RollDates["NextRollDate"] = RollDates["RollDate"]-pd.to_timedelta(RollDates["RollDate"].dt.dayofweek-pd.to_numeric(RollDates["int"])-14, unit='d')
        for i in list(RollDates["RollDate"]):
            RollDates.loc[RollDates["RollDate"]==i,"NextRollDate"]=hp.workday(hp.workday(pd.to_datetime(RollDates.loc[RollDates["RollDate"]==i,"NextRollDate"].values[0]),-1,self.HolidayList()),1,self.HolidayList())
            if pd.to_datetime(RollDates.loc[RollDates["RollDate"]==i,"NextRollDate"].values[0])==self.RunDate:
                RollDates.loc[RollDates["RollDate"]==i,"RollDate"]=self.RunDate
#                rollDates.loc[rollDates["RollDate"]==i,"IndexNotional"] = (0.1*self.IIL())/(6*0.15*(self.Volatility()**2))
        return RollDates
    
    def SumOfReturns(self, degree, ticker, pricecolumn, datecolumn, start, end, nbd):
        ticker["Return"] = (ticker[pricecolumn]/ticker[pricecolumn].shift(1)-1)**degree
        ticker.reset_index(inplace=True)
        if nbd != 0:
            endIndex = ticker.index[ticker[datecolumn]==end].values.astype(int)[0]
            tsum=np.sum(ticker.loc[endIndex-nbd+1:endIndex,'Return'])
        else:
            startIndex = ticker.index[ticker[datecolumn]==start].values.astype(int)[0]+1
            endIndex = ticker.index[ticker[datecolumn]==end].values.astype(int)[0]
            tsum=np.sum(ticker.loc[startIndex:endIndex,'Return'])
#        return ticker.loc[startIndex:endIndex,'Return']
        return tsum
    
    def Volatility(self):
        ticker = self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="SPX",["PriceDate","Price"]].sort_values(by = "PriceDate", ascending = True)
        RV = math.sqrt(self.SumOfReturns(2,ticker,"Price","PriceDate",100,self.RunDate,21)*252/21)
        vix = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="VIX"),"Price"].values[0]
        return (RV+(vix/100))/2
    
    def Notional(self):
        Vol_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"Volatility"].tail(1).values[0]
        IIL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"InterIndexLevel"].tail(1).values[0]
        return (0.1*IIL_1)/(6*0.15*(Vol_1**2))

    def VarSpread(self, VS):
        ticker = self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="SPX",["PriceDate","Price"]].sort_values(by = "PriceDate", ascending = True)
#        rollDates = self.NextRoll()
        RollDate = self.IndexSpecificData.loc[self.IndexSpecificData["RollIdentifier"]==VS, "RollDate"].values[0]
        Notional = self.IndexSpecificData.loc[self.IndexSpecificData["RollIdentifier"]==VS, "IndexNotional"].values[0]
        ReturnSum = self.SumOfReturns(1,ticker,"Price","PriceDate",RollDate,self.RunDate,0)
        ReturnSqSum = self.SumOfReturns(2,ticker,"Price","PriceDate",RollDate,self.RunDate,0)
#        print(Notional)
        return Notional*(((0.97**2)*ReturnSqSum)-(ReturnSum**2))
    
    def FinalVarSpread(self):
        rollDates = self.NextRoll()
        for i in list(rollDates["RollDate"]):
            rollDates.loc[rollDates["RollDate"]==i,"FinalExposureCurr"]= self.VarSpread(rollDates.loc[rollDates["RollDate"]==i,"RollIdentifier"].values[0])
        for i in list(rollDates["RollDate"]):
            if pd.to_datetime(rollDates.loc[rollDates["RollDate"]==i,"RollDate"].values[0])==self.LastRunDate:
                rollDates.loc[rollDates["RollDate"]==i,"Flag"]= 1
            else:
                rollDates.loc[rollDates["RollDate"]==i,"Flag"]= 0
        return rollDates

    def IIL(self):
        ticker = self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="SPX",["PriceDate","Price"]].sort_values(by = "PriceDate", ascending = True)
        Spreads = self.FinalVarSpread()
        Return = self.SumOfReturns(1,ticker,"Price","PriceDate",self.LastRunDate,self.RunDate,0)
        IIL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"InterIndexLevel"].tail(1).values[0]
#        print(Spreads["FinalExposureCurr"])
        return max(-2*abs(Return)*IIL_1,min(2*abs(Return)*IIL_1,sum(Spreads["FinalExposureCurr"])-sum(Spreads["FinalExposure"]*(1-Spreads["Flag"])))) + IIL_1
    
    def IndexLevel(self):
        IIL = self.IIL()
        IIL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"InterIndexLevel"].tail(1).values[0]
        IL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].tail(1).values[0]
        return IL_1*(IIL/IIL_1)

    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
    
    def NewCloseComposition(self):
        IIL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"InterIndexLevel"].tail(1).values[0]
        IL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].tail(1).values[0]
        spx = self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate) & (self.DailyPrices["GenericTicker"]=="SPX"), "Price"].values[0]
        t_NewCloseComposition = self.OpenComposition.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = (self.IIL() - IIL_1)*(IL_1/IIL_1)/spx
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        Spreads = self.FinalVarSpread()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["RollIdentifier"] = Spreads["RollIdentifier"]
        t_indexspecificdata["FinalExposure"] = Spreads["FinalExposureCurr"]
        t_indexspecificdata["IndexLevel"] = self.IndexLevel()
        t_indexspecificdata["RollDate"] = Spreads["RollDate"]
        for i in list(Spreads["RollDate"]):
            if pd.to_datetime(Spreads.loc[Spreads["RollDate"]==i,"RollDate"].values[0])==self.RunDate:
                t_indexspecificdata.loc[t_indexspecificdata["RollDate"]==i,"IndexNotional"]= self.Notional()
        t_indexspecificdata["Volatility"] = self.Volatility()
        t_indexspecificdata["InterIndexLevel"] = self.IIL()
        return t_indexspecificdata
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]

        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewCloseComposition(), self.OpenIndexSpecific()]

class JPZMFTUSCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"]).values[0]
#        print(self.HolidayCalendar["Date"])
#        in self.HolidayCalendar["Date"])
        
    def STAvg(self):
        return np.average(self.DailyPrices.sort_values(by="PriceDate",ascending=True).tail(5)["Price"].values[0])
    
    def LTAvg(self):
        return np.average(self.DailyPrices.sort_values(by="PriceDate",ascending=True).tail(260)["Price"])
    
    def Flip(self):
        if self.STAvg() >= self.LTAvg():
            return 1
        else:
            return 0
        
    def Rebal(self):        
        Flip_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"Flip"].values[0]        
        if self.Flip() != Flip_1:
            return 1
        else:
            return 0
        
    def UnderlyingRebalLevel(self):
        Rebal_1 =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"RebalanceFlag"].values[0]
        ULR_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"UnderlyingRebalLevel"].values[0]
        Price_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "FTJGUSEE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        
        if Rebal_1:
            return Price_1
        else:
            return ULR_1
        
    def Exposure(self):
        PR = self.DailyPrices.sort_values(by="PriceDate",ascending=True).tail(261).head(260)["Price"] 
        STAvg_1 = np.average(PR.tail())
        LTAvg_1 = np.average(PR)
        Rebal_1 =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"RebalanceFlag"].values[0]
        
        if Rebal_1:
            if STAvg_1 >= LTAvg_1:
                return 1
            else:
                return -1
        else:
            return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"FinalExposure"].values[0]
    
    def Return(self):
        UP = (self.DailyPrices.loc[(self.DailyPrices["GenericTicker"] == "FTJGUSEE") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0])
        
        if self.Rebal():
            return (self.Exposure()*(UP/self.UnderlyingRebalLevel()-1)) - 0.0006
        else:
            return self.Exposure()*((UP/self.UnderlyingRebalLevel())-1)
            
    def IndexRebalLevel(self):
        Rebal_1 =  self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),"RebalanceFlag"].values[0]
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0]
        UL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevelRebal"].values[0]
        
        if Rebal_1:
            return IL_1
        else:
            return UL_1
        
    def IndexLevel(self):
        return self.IndexRebalLevel()*(1+self.Return())
    
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = (self.IndexRebalLevel()*self.Exposure())/self.UnderlyingRebalLevel()
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.IndexRebalLevel()-(self.IndexRebalLevel()*self.Exposure())
        return t_NewCloseComposition
    
    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Flip"] = self.Flip()
        OpenIndexSpecificData["RebalanceFlag"] = self.Rebal()
        OpenIndexSpecificData["UnderlyingRebalLevel"] = self.UnderlyingRebalLevel()
        OpenIndexSpecificData["Exposure"] = self.Exposure()
        OpenIndexSpecificData["IndexLevelRebal"] = self.IndexRebalLevel()
        return OpenIndexSpecificData
    
    
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewCloseComposition(),self.OpenIndexSpecificData()]

class JPFCVA01Calculation:
    def __init__(self,cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]   
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.RunDate = cl_FileProcessor.RunDate
#        print("RunDate",self.RunDate)
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        print("LastRunDate",self.LastRunDate)
        self.DailyPrices.loc[self.DailyPrices.shape[0]+1,"GenericTicker"]="USD WMCO Curncy"
        self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="USD WMCO Curncy","Price"]=1
        self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="USD WMCO Curncy","PriceDate"]=pd.to_datetime(self.RunDate)
        self.DailyPrices.loc[self.DailyPrices.shape[0]+1,"GenericTicker"]="JPFCTUSD"
        self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="JPFCTUSD","Price"]=1
        self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="JPFCTUSD"),"PriceDate"]=pd.to_datetime(self.RunDate)
        self.DailyPrices.loc[self.DailyPrices.shape[0]+1,"GenericTicker"]="JPFCTUSD"
        self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="JPFCTUSD")&(self.DailyPrices["PriceDate"]!=pd.to_datetime(self.RunDate)),"Price"]=1
        self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]=="JPFCTUSD")&(self.DailyPrices["PriceDate"]!=pd.to_datetime(self.RunDate)),"PriceDate"]=pd.to_datetime(self.LastRunDate)
        self.DailyPrices.loc[self.DailyPrices.shape[0]+1,"GenericTicker"]="PPP US Index"
        self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="PPP US Index","Price"]=1
        self.DailyPrices.loc[self.DailyPrices["GenericTicker"]=="PPP US Index","PriceDate"]=pd.to_datetime(self.RunDate)
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]

        
    def Price(self,STPriceTicker):
        return self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==STPriceTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
    def Price_1(self,PTPriceTick):
        return round(self.DailyPrices.loc[self.DailyPrices["GenericTicker"].str.contains(PTPriceTick),"Price"].values[0],3)
    def Signal(self,STPriceTick,PTPriceTick):
        dic={"AUD WMCO Curncy":-1,"CAD WMCO Curncy":1,"EUR WMCO Curncy":-1,"GBP WMCO Curncy":-1,"JPY WMCO Curncy":1,"NOK WMCO Curncy":1,"NZD WMCO Curncy":-1,"SEK WMCO Curncy":1,"USD WMCO Curncy":1}
        #dic={"AUD WMCO Curncy":-1,"CAD WMCO Curncy":-1,"EUR WMCO Curncy":-1,"GBP WMCO Curncy":-1,"JPY WMCO Curncy":-1,"NOK WMCO Curncy":-1,"NZD WMCO Curncy":-1,"SEK WMCO Curncy":-1,"USD WMCO Curncy":-1}
#        dic[STPriceTick]
        return np.log(self.Price_1(PTPriceTick)/(self.Price(STPriceTick)**(dic[STPriceTick])))

    def Weight(self):
        dic_temp={}
        dic_1={}
        STicker=sorted(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"].str.contains('WMCO')),"GenericTicker"])
        PTicker=["PPP AS Index","PPP CA Index","PPP EUAR Index","PPP UK Index","PPP JN Index","PPP NO Index","PPP NZ Index","PPP SW Index","PPP US Index"]
        final=list(zip(STicker,PTicker))
        for ST,PT in final:
            dic_temp.update({ST:self.Signal(ST,PT)})
#        print("Signal_dict",dic_temp)
        Average=np.average(list(dic_temp.values()))
#        print("Average",Average)
        for ST in STicker:
            dic_1.update({ST:-dic_temp[ST] + Average})
#        print("Inside Weight",dic_1)
        return dic_1
        
    def Weight_abs2(self):
        dic_2={}
        STicker=STicker=sorted(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"].str.contains('WMCO')),"GenericTicker"])
        abs_lis=[abs(i) for i in list(self.Weight().values())]
        for i in STicker:
            dic_2.update({i:(4/max(4,sum(abs_lis)))*self.Weight()[i]})
            if dic_2[i]<0:
                dic_2[i]=max(-1,dic_2[i])
            else:
                dic_2[i]=min(1,dic_2[i])
#        print("Weight_abs2",dic_2)
        return dic_2
    def Tracker_ret(self,GenTicker):
        P_t=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.RunDate),"Price"].values[0]
        P_t_1=self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]==GenTicker)&(self.DailyPrices["PriceDate"]==self.LastRunDate),"Price"].values[0]
        return (P_t/P_t_1-1)
    def Tracker_Return(self):
        dic_3={}
        STicker=sorted(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"].str.contains('WMCO')),"GenericTicker"])
        Tracker_Ticker=sorted(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.LastRunDate)&(self.DailyPrices["GenericTicker"].str.contains("JPFCT")),"GenericTicker"])     
        final_2=list(zip(STicker,Tracker_Ticker))
        for i,j in final_2:
            dic_3.update({i:self.Tracker_ret(j)})
        #print("Tracker_Return",dic_3)
        return dic_3
        
    def IndexLevel(self):
        IL_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
#        print("Inside IndexLevel t-1",IL_1)
        
        IL_2=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]!=self.LastRunDate,"IndexLevel"].values[0]
#        print("Inside IndexLevel t-2",IL_2)        
        STicker=sorted(self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"].str.contains('WMCO')),"GenericTicker"])
#        print("STicker",STicker)
        df=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]!=self.LastRunDate,["GenericTicker","Weights"]]
#        print(df)
        dic_4=df.set_index('GenericTicker').T.to_dict('list')
        for i in list(dic_4.keys()):
            dic_4[i[5:8]+" WMCO Curncy"]=dic_4[i][0]
            del dic_4[i]
        dic_4.update({"USD WMCO Curncy":1})
#        print("Inside IndexLevel",dic_4)
#        print("IndexLevel",sum(dic_4[x]*self.Tracker_Return()[x] for x in STicker)*IL_2+IL_1)
        return sum(dic_4[x]*self.Tracker_Return()[x] for x in STicker)*IL_2+IL_1

        
    def CashUnits(self):
        IL_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
        cash_dic=list(self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate]["Weights"])
        return self.IndexLevel()-sum(cash_dic)*IL_1

     
    def Weights_1(self,ticker):
        return self.IndexSpecificData.loc[(self.IndexSpecificData["GenericTicker"]==ticker)&(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"Weights"].values[0]
        
            
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        return t_NewCloseComposition

    def NewOpenComposition(self):
        IL_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
        t_NewOpenComposition = self.OpenComp.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = IL_1*t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'SpecificTicker'].apply((lambda x:self.Weights_1(x)/t_NewOpenComposition.loc[t_NewOpenComposition['GenericTicker'].str.contains(x),'Price'].values[0]))
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units']=self.CashUnits()                                                                                                    
        return t_NewOpenComposition

    def OpenIndexSpecificData(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["IndexLevel"] = self.IndexLevel()
        temp_dic=self.Weight()
        for i in list(self.Weight().keys()):
            temp_dic["JPFCT"+i[0:3]]=temp_dic[i]
            del temp_dic[i]
        del temp_dic["JPFCTUSD"]
        t_indexspecificdata["Weights"]=t_indexspecificdata["GenericTicker"].map(temp_dic)
        return t_indexspecificdata   
              
        
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(),self.OpenIndexSpecificData()]

    
class JPMSEB3B_IndexCal:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
#        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])

    def Price(self,ticker, date):
        return self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)&(self.DailyPrices["GenericTicker"]==ticker),"Price"].values[0]
    
    def HolidayList(self, exchange):
        try:
            self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
            if exchange == "all":
                return list(self.HolidayCalendar["Date"])
            else:
                return list(pd.to_datetime(self.HolidayCalendar.loc[self.HolidayCalendar["Center_Name"]==exchange,"Date"]))
        
        except:
            return list()

    def RebalFlag(self):
        runDate = hp.workday(hp.workday(self.RunDate,-1,self.HolidayList("all")),1,self.HolidayList("all"))
        if runDate.month!=runDate.month:
            return True
        else:
            return False

        
    def is_third_wed(self,date):
        c = calendar.Calendar(firstweekday=calendar.SUNDAY)
        year = date.year; month = date.month        
        monthcal = c.monthdatescalendar(year,month)
        third_wed = pd.to_datetime([day for week in monthcal for day in week if \
                        day.weekday() == calendar.WEDNESDAY and \
                        day.month == month][2])
        third_Wed = hp.workday(hp.workday(third_wed,-1,self.HolidayList("London")),1,self.HolidayList("London"))
        third_Wed_2 = hp.workday(third_wed,-2,self.HolidayList("London"))
#        return third_wed
        return [third_Wed_2, third_Wed]
    
    def Returns(self):
        ReturnData = self.OpenComposition.loc[~self.OpenComposition['InstrumentVTId'].str.contains('Ca:'),:][["GenericTicker"]].copy()
        ReturnData.loc[ReturnData["GenericTicker"]=="JPMSE31P","FXReturn"] =self.Price("EUR WMCO Curncy",self.RunDate)/self.Price("EUR WMCO Curncy",self.LastRunDate)
        ReturnData.loc[ReturnData["GenericTicker"]=="JPMSG31P","FXReturn"] =self.Price("GBP WMCO Curncy",self.RunDate)/self.Price("GBP WMCO Curncy",self.LastRunDate)
        ReturnData.loc[ReturnData["GenericTicker"]=="JPMSU31P","FXReturn"] = 1
        ReturnData["Return"] = 2*ReturnData["FXReturn"]*(ReturnData["GenericTicker"].apply((lambda x: self.Price(x,self.RunDate)))/ReturnData["GenericTicker"].apply((lambda x: self.Price(x,self.LastRunDate)))-1)
        Roll1 = hp.workday(self.is_third_wed(self.RunDate)[0],-2)
        Roll2 = hp.workday(self.is_third_wed(self.RunDate)[1],-2)
        ReturnData["RollAdj"] = 0
        if self.RunDate.month in [3,6,9,12]:
            if self.RunDate == Roll1:
                ReturnData.loc[ReturnData["GenericTicker"]=="JPMSE31P","RollAdj"] = 0.0002
                ReturnData.loc[ReturnData["GenericTicker"]=="JPMSU31P","RollAdj"] = 0.0002
            elif self.RunDate == Roll2:
                ReturnData.loc[ReturnData["GenericTicker"]=="JPMSG31P","RollAdj"] = 0.0004
        return ReturnData

    def Notional(self):
        RebalFlag = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"RebalanceFlag"].values[0]
        Notional_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexNotional"].values[0]
        Date_3 = hp.workday(self.RunDate,-3)
        USDBasket_3 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==Date_3,"FinalExposure"].values[0]
        if RebalFlag == True:
            return USDBasket_3
        else:
            return Notional_1            
    
    def USDBasket(self):
        USDBasket_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"FinalExposure"].values[0]
        Notional = self.Notional()
        Return = self.Returns()
        return USDBasket_1 + Notional*(np.sum(Return["Return"])-np.sum(Return["RollAdj"]))
    
    def IndexLevel(self):
        IL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
        USDBasket_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"FinalExposure"].values[0]
        USDBasket = self.USDBasket()
        FX = self.Price("EUR WMCO Curncy",self.RunDate)
        FX_1 = self.Price("EUR WMCO Curncy",self.LastRunDate)
        return IL_1*(1+((USDBasket/USDBasket_1)-1)*(FX_1/FX))

    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()
            
    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComposition.copy()
        t_NewCloseComposition["Date"] = self.RunDate
#        EFX = self.Price("EUR WMCO Curncy",self.RunDate)
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition

    def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComposition.copy()
        t_NewOpenComposition["Date"] = self.RunDate
#        EFX_1 = self.Price("EUR WMCO Curncy",self.LastRunDate)
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = 2*self.Notional()/\
                                  (t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'FxRate']*\
                                t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Price']*self.USDBasket())
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewOpenComposition

    def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["FinalExposure"] = self.USDBasket()
        t_indexspecificdata["IndexNotional"] = self.Notional()
        t_indexspecificdata["IndexLevel"] = self.IndexLevel()
        t_indexspecificdata["RebalanceFlag"] = self.RebalFlag()
        return t_indexspecificdata
        
    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    def fn_VTOpenComp(self):
        return [self.NewOpenComposition(), self.OpenIndexSpecific()]

class JPEDIVSGCalculation:
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.OpenComp = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.CloseComp = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.DailyPrices["SettlementDate"] = pd.to_datetime(self.DailyPrices["SettlementDate"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"]) 
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])
         
    def RebalanceFlag(self):
        SettleDate = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.RunDate),"SettlementDate"].values[0]) 

        if SettleDate <= self.RunDate:
            return 1
        else:
            return 0
    
    def Weight(self):
        
        SettleDate1 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.RunDate),"SettlementDate"].values[0])
        SettleDate2 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.RunDate),"SettlementDate"].values[0])
        SettleDate3 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED3") & (self.DailyPrices["PriceDate"]== self.RunDate),"SettlementDate"].values[0])
        
        dp1 = (SettleDate1 - self.LastRunDate).days
        dp2 = (SettleDate2 - self.LastRunDate).days
        dp3 = (SettleDate3 - self.LastRunDate).days

        if dp2 <365:
            return (dp3-365)/(dp3-dp2)
        else:
            if dp1 < 365:
                if dp1 <= 1:
                    return 0
                else:
                    return (dp2-365)/(dp2-dp1)
            else:
                return 1
    
    
    def Return(self):
        
        if self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'RebalanceFlag'].values[0] == False:
            DEDONE = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] /self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0] -1
            DEDTWO = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] /self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0] -1
        else:
            DEDONE = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] /self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0] -1
            DEDTWO = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0] /self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED3") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0] -1

        return list([DEDONE,DEDTWO])
    
    def SDIndex(self):
        SD_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"UnderlyingRebalLevel"].values[0]
        
        return (((self.Return()[0]*self.Weight())+(self.Return()[1]*(1-self.Weight())))+1) * SD_1

    
    def RT(self):
        SD_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"UnderlyingRebalLevel"].values[0]
        FT_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        FT = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]

        RTSD = math.log(self.SDIndex()/SD_1)
        FTSD = math.log(FT/FT_1)
        
        return list([RTSD,FTSD])
        
        
    def BetaSD(self):
        IndexSpec = (self.IndexSpecificData.sort_values(by = ["PriceDate"], ascending = True)).tail(20)[["PriceDate","Return","FinalExposure"]].reset_index().drop("index",axis = 1)
        
        IndexSpec = pd.DataFrame(np.insert(IndexSpec.values, 20, values=[self.RunDate,self.RT()[0],self.RT()[1]], axis=0),columns = IndexSpec.columns)
        
        ArrFT = IndexSpec["FinalExposure"]
        ArrSD = IndexSpec["Return"]


        Arr1 = ArrFT-sum(ArrFT)/21
        Arr2 = ArrSD-sum(ArrSD)/21
               
        return sum(Arr1*Arr2)/sum(Arr1**2)
    
    def Leverage(self):
        BSD = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Signal"].values[0]     
        return max(-1,min(1,-BSD))
    
    def GrossReturn(self):
        SD_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"UnderlyingRebalLevel"].values[0]
        FT_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        FT = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        Lev_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        return (self.SDIndex()/SD_1-1)+Lev_1*(FT/FT_1-1)
    
    def RebalEQ(self):
        Lev_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        FT_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        FT = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        
        return (0.0003*abs(self.Leverage()-(Lev_1*FT/((1+self.GrossReturn())*FT_1))))
    
    def Wt(self):
        Date_2 = pd.to_datetime(((self.IndexSpecificData.sort_values(by = ["PriceDate"], ascending = True)).tail(2)).head(1)["PriceDate"].values[0])

        
        SettleDate1 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"SettlementDate"].values[0])
        SettleDate2 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"SettlementDate"].values[0])
        SettleDate3 = pd.to_datetime(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED3") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"SettlementDate"].values[0])

        dp1 = (SettleDate1 - Date_2).days
        dp2 = (SettleDate2 - Date_2).days
        dp3 = (SettleDate3 - Date_2).days
        
        if dp2 <365:
            return (dp3-365)/(dp3-dp2)
        else:
            if dp1 < 365:
                if dp1 <= 1:
                    return 0
                else:
                    return (dp2-365)/(dp2-dp1)
            else:
                return 1
    
    def RebalDivAgg(self):
        DED1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        DED2 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.RunDate),"Price"].values[0]
        DED1_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        DED2_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        DED3_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED3") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        W3 = 0
        
     
        if self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'RebalanceFlag'].values[0] == True:
            RD1 = abs(self.Weight()/DED1-(1-self.Wt())/((1+self.GrossReturn())*DED2_1))*max(0.002*DED1,0.05)
        else:
            RD1 = abs(self.Weight()/DED1-self.Wt()/((1+self.GrossReturn())*DED1_1))*max(0.002*DED1,0.05)
        
        if self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]== self.LastRunDate),'RebalanceFlag'].values[0] == True:
            RD2 = abs((1-self.Weight())/DED2-W3/((1+self.GrossReturn())*DED3_1))*max(0.002*DED2,0.05)
        else:
            RD2 = abs((1-self.Weight())/DED2-(1-self.Wt())/((1+self.GrossReturn())*DED2_1))*max(0.002*DED2,0.05)
           
        return (RD1+RD2)
    
        
    def Return2(self):
        return (self.GrossReturn()-self.RebalDivAgg()-self.RebalEQ())
    
    def IndexLevel(self):
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0]
        return (IL_1*(1+self.Return2()))
    

        
    
    def NewCloseComposition(self):
        P1_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED1") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        P2_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "DED2") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        P3_1 = self.DailyPrices.loc[(self.DailyPrices["GenericTicker"]== "FTJGEUEE") & (self.DailyPrices["PriceDate"]== self.LastRunDate),"Price"].values[0]
        SD_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"UnderlyingRebalLevel"].values[0]
        Lev_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"Leverage"].values[0]
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"] == self.LastRunDate),"IndexLevel"].values[0]
        t_NewCloseComposition = self.OpenComp.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        t_NewCloseComposition.loc[t_NewCloseComposition['GenericTicker']== 'DED1','Units'] = self.Weight()*SD_1/P1_1
        t_NewCloseComposition.loc[t_NewCloseComposition['GenericTicker']== 'DED2','Units'] = (1-self.Weight())*SD_1/P2_1
        t_NewCloseComposition.loc[t_NewCloseComposition['GenericTicker']== 'FTJGEUEE','Units'] = (Lev_1*IL_1)/P3_1
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(df)
        return t_NewCloseComposition
    
    def CashUnits(self,df):
        return self.IndexLevel()-df[['Units','Price']].product(axis=1).sum()
    
   

    
    def OpenIndexSpecificData(self):
        OpenIndexSpecificData = self.IndexSpecificData.copy()
        OpenIndexSpecificData.drop(OpenIndexSpecificData.index[1:], inplace=True)
        OpenIndexSpecificData["PriceDate"] = self.RunDate
        OpenIndexSpecificData["IndexLevel"] = self.IndexLevel()
        OpenIndexSpecificData["Leverage"] = self.Leverage()
        OpenIndexSpecificData["UnderlyingRebalLevel"] = self.SDIndex()
        OpenIndexSpecificData["Return"] = self.RT()[0]
        OpenIndexSpecificData["FinalExposure"] = self.RT()[1]
        OpenIndexSpecificData["Signal"] = self.BetaSD()
        OpenIndexSpecificData["RebalanceFlag"] = self.RebalanceFlag()
        return OpenIndexSpecificData
     
    @hp.output_file(["VTCloseComposition"])
    
    def fn_VTCloseComp(self):
        return [self.NewCloseComposition()]
    
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
    
    def fn_VTOpenComp(self):
        return [self.NewCloseComposition(),self.OpenIndexSpecificData()]

class SGIXTFFX_IndexCal:
    
    def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
        self.ReturnData = cl_FileProcessor.dict_FileData["VTReturnData"]
#        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.ReturnData["Date"] = pd.to_datetime(self.ReturnData["Date"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])

    def RebalFlag(self):
        RebalDate = hp.workday(self.RunDate+BMonthEnd(-1),2,list(pd.to_datetime(self.HolidayCalendar["Date"])))
        if self.RunDate == RebalDate:
            return True
        else:
            return False
        
    def Price(self,date,ticker):
        return self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)&(self.DailyPrices["GenericTicker"]==ticker),"Price"].values[0]

    def FxRate(self,date,ticker):
        return self.DailyPrices.loc[(self.DailyPrices["PriceDate"]==date)&(self.DailyPrices["GenericTicker"]==ticker),"FxRate"].values[0]
        
    def Return(self,ticker):
        return self.Price(self.RunDate,ticker)/self.Price(self.LastRunDate,ticker)-1
    
    def Sigma(self,ticker):
        Sigma_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Return"].values[0]
        return math.sqrt(0.96*(Sigma_1**2)+0.04*(self.Return(ticker)**2))
    
    def RetBySigma(self,ticker):
        return self.Return(ticker)/self.Sigma(ticker)
    
    def Indicator(self,ticker,days):
        Returns = self.ReturnData.loc[self.ReturnData["Ticker"]==ticker,:].sort_values(by = "Date",ascending = True).tail(days-1)
        CurrRet = self.RetBySigma(ticker)
        if sum(Returns["ReturnValue"])+CurrRet > 0:
            return 1
        elif sum(Returns["ReturnValue"])+CurrRet < 0:
            return -1
        else:
            return 0
        
    def AggIndicator(self,ticker):
        return (self.Indicator(ticker,22)+self.Indicator(ticker,66)+self.Indicator(ticker,125)+self.Indicator(ticker,250))/4
    
    def TargetWeight(self,ticker):
        const_weight = self.IndexSpecificData.loc[(self.IndexSpecificData["GenericTicker"]==ticker)&(self.IndexSpecificData["PriceDate"]==self.LastRunDate),"DailyUnitChange"].values[0]
#        print(self.Sigma(ticker))
        return (0.1*self.AggIndicator(ticker)*const_weight)/(math.sqrt(250)*self.Sigma(ticker))
    
    def NBCERL(self,ticker):
        NBCERL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexNotional"].values[0]
        return NBCERL_1*(1+(self.Price(self.RunDate,ticker)/self.Price(self.LastRunDate,ticker)-1)*(self.FxRate(self.RunDate,ticker)/self.FxRate(self.LastRunDate,ticker)))
    
    def NGIL_Interim(self,ticker):
        SpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]==ticker,:].sort_values(by="PriceDate",ascending = False).reset_index()
        date_3 = SpecificData.loc[2,"PriceDate"]
        date_2 = SpecificData.loc[1,"PriceDate"]
        TW_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"CutOffWeight"].values[0]
        NGIL_3 = self.ReturnData.loc[(self.ReturnData["Date"]==date_3)&(self.ReturnData["IsLongReturn"]==0),"ReturnValue"].values[0]
        NBCERL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexNotional"].values[0]
        NBCERL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexNotional"].values[0]
        return TW_2*NGIL_3*(self.NBCERL(ticker)-NBCERL_1)/NBCERL_3
    
    def NGIL(self):
        NGIL_1 = self.ReturnData.loc[(self.ReturnData["Date"]==self.LastRunDate)&(self.ReturnData["IsLongReturn"]==0),"ReturnValue"].values[0]
        return NGIL_1 + sum([self.NGIL_Interim(ticker) for ticker in list(set(self.IndexSpecificData["GenericTicker"]))])
    
    def HV(self):
        df_NGIL = self.ReturnData.loc[self.ReturnData["IsLongReturn"]==0,:].sort_values(by="Date",ascending=True)
#        print(NGIL_1)
        df_NGIL["SqReturn"] = np.log(df_NGIL["ReturnValue"]/df_NGIL["ReturnValue"].shift(1))**2
        NGIL_1 = self.ReturnData.loc[(self.ReturnData["Date"]==self.LastRunDate)&(self.ReturnData["IsLongReturn"]==0),"ReturnValue"].values[0]
#        print(sum(df_NGIL["SqReturn"].tail(749)))
        return math.sqrt((sum(df_NGIL["SqReturn"].tail(749)) + math.log(self.NGIL()/NGIL_1)**2)/3)
    
    def ATW(self,ticker):
        HV_Rebal = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"UnderlyingRebalLevel"].values[0]
        print(self.TargetWeight(ticker),HV_Rebal)
        return (0.1*self.TargetWeight(ticker))/HV_Rebal
    
    def Weight(self,ticker):
#        print(self.IndexSpecificData["PriceDate"], self.LastRunDate)
        Weight_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]
        AC = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"FundingRate"].values[0]
        DC = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Signal"].values[0]
        print(self.ATW(ticker))
        return min(AC,max(min(DC,max(-1*DC,self.ATW(ticker)-Weight_1))+Weight_1,-1*AC))
    
    def BCERL(self,ticker):
        SpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]==ticker,:].sort_values(by="PriceDate",ascending = False).reset_index()
        date_2 = SpecificData.loc[1,"PriceDate"]
        RC = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"RebalancingTransactionCost"].values[0]
        Weight_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]
        BCERL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        ACT = (self.RunDate-self.LastRunDate).days/360
        if Weight_2 > 0:
            sign = 1
        elif Weight_2 < 0:
            sign = -1
        else:
            sign = 0
        return BCERL_1*(1+((self.Price(self.RunDate,ticker)/self.Price(self.LastRunDate,ticker))-1-sign*RC*ACT)*self.FxRate(self.RunDate,ticker)/self.FxRate(self.LastRunDate,ticker))
    
    def TC_Interim(self,ticker):
        SpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]==ticker,:].sort_values(by="PriceDate",ascending = False).reset_index()
        date_3 = SpecificData.loc[2,"PriceDate"]
        date_2 = SpecificData.loc[1,"PriceDate"]
        TC = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Fee"].values[0]
        Weight_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]
        Weight_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]
        IL_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexLevel"].values[0]
        IL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexLevel"].values[0]
        BCERL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        BCERL_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        BCERL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        return TC*abs(Weight_1*IL_2*(self.BCERL(ticker)/BCERL_2)-Weight_2*IL_3*(BCERL_1/BCERL_3)*(self.Price(self.RunDate,ticker)/self.Price(self.LastRunDate,ticker))*(self.FxRate(self.RunDate,ticker)/self.FxRate(self.LastRunDate,ticker)))        

    def GIL_Interim(self,ticker):
        SpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]==ticker,:].sort_values(by="PriceDate",ascending = False).reset_index()
        date_3 = SpecificData.loc[2,"PriceDate"]
        date_2 = SpecificData.loc[1,"PriceDate"]
        Weight_2 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_2)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]
        BCERL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        BCERL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
        GIL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"Leverage"].values[0]                
#        print(GIL_3)
        return Weight_2*(GIL_3/BCERL_3)*(self.BCERL(ticker)-BCERL_1)
    
    def TC(self):
        return sum([self.TC_Interim(ticker) for ticker in list(set(self.IndexSpecificData["GenericTicker"]))])

    def GIL(self):
        GIL_1 = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"Leverage"].tail(1).values[0]                
#        print([self.GIL_Interim(ticker) for ticker in list(set(self.IndexSpecificData["GenericTicker"]))])
        return GIL_1 + sum([self.GIL_Interim(ticker) for ticker in list(set(self.IndexSpecificData["GenericTicker"]))])
    
    def IndexLevel(self):
        ticker = list(set(self.IndexSpecificData["GenericTicker"]))[0]
        SpecificData = self.IndexSpecificData.loc[self.IndexSpecificData["GenericTicker"]==ticker,:].sort_values(by="PriceDate",ascending = False).reset_index()
        date_3 = SpecificData.loc[2,"PriceDate"]
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexLevel"].values[0]
        IL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexLevel"].values[0]
        GIL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Leverage"].values[0]                
        GIL_3 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==date_3)&(self.IndexSpecificData["GenericTicker"]==ticker),"Leverage"].values[0]                
        ACT = (self.RunDate-self.LastRunDate).days/360
#        print(IL_1,IL_3,self.GIL(),GIL_1,self.TC())
        return IL_1*(1-0.005*ACT)+IL_3*((self.GIL()-GIL_1)/GIL_3)-self.TC()
    
    def CashUnits(self,IL,df):
        return IL-df[['Units','Price','FxRate']].product(axis=1).sum()

    def NewCloseComposition(self):
        t_NewCloseComposition = self.OpenComposition.copy()
        t_NewCloseComposition["Date"] = self.RunDate
        df = t_NewCloseComposition.loc[~t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewCloseComposition.loc[t_NewCloseComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
        return t_NewCloseComposition
    
    def Weight_1(self,ticker):
        return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"Weights"].values[0]

    def BCERL_1(self,ticker):
        return self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"FinalExposure"].values[0]
    
    def NewOpenComposition(self):
        ticker = list(set(self.IndexSpecificData["GenericTicker"]))[0]
        IL_1 = self.IndexSpecificData.loc[(self.IndexSpecificData["PriceDate"]==self.LastRunDate)&(self.IndexSpecificData["GenericTicker"]==ticker),"IndexLevel"].values[0]
        t_NewOpenComposition = self.OpenComposition.copy()
        t_NewOpenComposition["Date"] = self.RunDate
#        print(self.OpenComposition["GenericTicker"])
#        print(t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),:][['GenericTicker']])
        t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'GenericTicker'].apply((lambda x: self.BCERL(x)))*\
                                 t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'GenericTicker'].apply((lambda x: self.Weight_1(x)))*IL_1/\
                                 (t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'GenericTicker'].apply((lambda x: self.BCERL_1(x)))*t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Price']*t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'FxRate'])
        df = t_NewOpenComposition.loc[~t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),['Units','Price','FxRate']]
        t_NewOpenComposition.loc[t_NewOpenComposition['InstrumentVTId'].str.contains('Ca:'),'Units'] = self.CashUnits(self.IndexLevel(),df)
#        print(t_NewOpenComposition[["GenericTicker","Price","Units"]])
        return t_NewOpenComposition

    def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["Leverage"] = self.GIL()
        t_indexspecificdata["IndexLevel"] = self.IndexLevel()
        for ticker in list(set(self.IndexSpecificData["GenericTicker"])):
            t_indexspecificdata.loc[t_indexspecificdata["GenericTicker"]==ticker,"CutOffWeight"] = self.TargetWeight(ticker)
            t_indexspecificdata.loc[t_indexspecificdata["GenericTicker"]==ticker,"FinalExposure"] = self.BCERL(ticker)
            t_indexspecificdata.loc[t_indexspecificdata["GenericTicker"]==ticker,"IndexNotional"] = self.NBCERL(ticker)
            t_indexspecificdata.loc[t_indexspecificdata["GenericTicker"]==ticker,"Return"] =  self.Sigma(ticker)
            t_indexspecificdata.loc[t_indexspecificdata["GenericTicker"]==ticker,"Weights"] = self.Weight(ticker)
        if self.RebalFlag():
            t_indexspecificdata["RebalanceFlag"] = self.RebalFlag()
            t_indexspecificdata["RebalanceDate"] = self.RunDate
            t_indexspecificdata["UnderlyingRebalLevel"] = self.HV()
        return t_indexspecificdata            


    def OpenReturnData(self):
        NGIL_Return = self.ReturnData.loc[self.ReturnData["Date"]==self.LastRunDate,:].copy()
        NGIL_Return["Date"]=self.RunDate
        NGIL_Return.loc[NGIL_Return["IsLongReturn"]==0,"ReturnValue"]=self.NGIL()
        for ticker in list(set(self.IndexSpecificData["GenericTicker"])):        
            NGIL_Return.loc[(NGIL_Return["IsLongReturn"]==1)&(NGIL_Return["Ticker"]==ticker),"ReturnValue"]=self.RetBySigma(ticker)
        return NGIL_Return

    @hp.output_file(["VTCloseComposition"])
    def fn_VTCloseComp(self):
#        print(self.FxRate(self.RunDate,"SGIXGILT"),self.FxRate(self.LastRunDate,"SGIXIK"))
#        print(self.NewCloseComposition()[["GenericTicker","Units","Price","FxRate"]])
        return [self.NewCloseComposition()]
        
    @hp.output_file(["VTOpenComposition", "VTIndexSpecificData", "VTReturnData"])
    def fn_VTOpenComp(self):
#        pass
        return [self.NewOpenComposition(), self.OpenIndexSpecific(), self.OpenReturnData()]

class JPVOFXB2_IndexCal:
    
     def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.HolidayCalendar = cl_FileProcessor.dict_FileData["VTHolidayCalendar"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
#        self.HolidayCalendar["Date"] = pd.to_datetime(self.HolidayCalendar["Date"])
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
#        self.OpenDate = pd.to_datetime(self.DailyPrices.loc[self.DailyPrices["PriceDate"]==self.RunDate,"OpenDate"].values[0])

     def rebal_flag(self):
         
         if self.RunDate.month != self.LastRunDate.month:
             return True
         else:
             return False
         
     def Index_Level(self) :
         data = self.DailyPrices[self.DailyPrices["PriceDate"] == self.RunDate]
         data = data[["GenericTicker","Price"]]
         to_add =["Constant",1]
         data.loc[len(data)] = to_add
         data = data.sort_values(["GenericTicker"],ascending = True)
      
         composition = self.OpenComposition.sort_values(["GenericTicker"],ascending = True)
         data = data.reset_index(drop = True)
         composition = composition.reset_index(drop = True)
         data["Units"] = composition["Units"]
         IL = data[["Price","Units"]].product(axis = 1).sum()

         return IL
                    
     def NewCloseComposition(self):
       t_NewCloseComposition = self.OpenComposition.copy()
       t_NewCloseComposition["Date"] = self.RunDate
       return t_NewCloseComposition

     def OpenUnitswoCash(self):
        Date_2 = (((self.IndexSpecificData).sort_values(["PriceDate"],ascending = True)).tail(2)).head(1)["PriceDate"].values[0]
        data = self.DailyPrices.loc[self.DailyPrices["PriceDate"] == Date_2,:]
        data = data[["GenericInstrumentVTId","Price"]]
        
        t_NewOpenComposition = self.OpenComposition.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        IL_2 = ((self.IndexSpecificData).sort_values(["PriceDate"],ascending = True).tail(2)).head(1)["IndexLevel"].values[0]
      
        if self.rebal_flag() == True:
            data["Units"] = (IL_2*0.2)/data["Price"]
        else:
            data["Units"] = t_NewOpenComposition.loc[~t_NewOpenComposition["InstrumentVTId"].str.contains("Ca:"),"Units"]
        return data[["GenericInstrumentVTId","Units"]]

     def NewOpenComposition(self):
        t_NewOpenComposition = self.OpenComposition.copy()
        t_NewOpenComposition["Date"] = self.RunDate
        
        if self.rebal_flag() == True:
            OpenUnits = self.OpenUnitswoCash()
            for ticker in list(OpenUnits["GenericInstrumentVTId"]):
#                print(t_NewOpenComposition.loc[t_NewOpenComposition["SpecificTicker"]==ticker,:])
                t_NewOpenComposition.loc[t_NewOpenComposition["InstrumentVTId"]==ticker,"Units"] = OpenUnits.loc[OpenUnits["GenericInstrumentVTId"]==ticker,"Units"].values[0]
            t_NewOpenComposition.loc[t_NewOpenComposition["InstrumentVTId"].str.contains("Ca:"),"Units"] = self.Index_Level() - t_NewOpenComposition.loc[~t_NewOpenComposition["InstrumentVTId"].str.contains("Ca:"),["Units","Price"]].product(axis=1).sum()
        return t_NewOpenComposition
    
     def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        t_indexspecificdata["IndexLevel"] = self.Index_Level()
        t_indexspecificdata["RebalanceFlag"] = self.rebal_flag()
        return t_indexspecificdata
        
     @hp.output_file(["VTCloseComposition"])
     def fn_VTCloseComp(self):
#        print(self.NewOpenComposition()) 
        return [self.NewCloseComposition()]
        
     @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
     def fn_VTOpenComp(self):
#         pass
        return [self.NewOpenComposition(), self.OpenIndexSpecific()]   

class CSEAFMRS_IndexCal:
    
     def __init__(self, cl_FileProcessor):
        self.directory = cl_FileProcessor.directory
        self.IndexVTId = cl_FileProcessor.IndexVTId
        self.log = cl_FileProcessor.log
        self.DailyPrices = cl_FileProcessor.dict_FileData["VTDailyPrices"]
        self.IndexSpecificData = cl_FileProcessor.dict_FileData["VTIndexSpecificData"]
        self.CloseComposition = cl_FileProcessor.dict_FileData["VTCloseComposition"]
#        self.CloseComposition.to_csv("CSEAFMRS_CloseComposition.csv")
        self.OpenComposition = cl_FileProcessor.dict_FileData["VTOpenComposition"]
#        self.OpenComposition.to_csv("CSEAFMRS_OpenComposition.csv")
        self.DailyPrices["PriceDate"] = pd.to_datetime(self.DailyPrices["PriceDate"])
        self.DailyPrices["SettlementDate"]=pd.to_datetime(self.DailyPrices["SettlementDate"])
#        self.DailyPrices.to_csv("CSEAFMRS_dailyprices.csv")
        self.IndexSpecificData["PriceDate"] = pd.to_datetime(self.IndexSpecificData["PriceDate"])
#        self.IndexSpecificData.to_csv("CSEAFMRS_IndexSpecificData.csv")
        self.RunDate = cl_FileProcessor.RunDate
        self.LastRunDate = cl_FileProcessor.LastRunDate
    
        
     def Lg_Returns(self,Price_t,Price_t_1):
         return np.log(Price_t/Price_t_1)
         
     def delta(self):
         Lev=25
         IL_1=self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,"IndexLevel"].values[0]
         Run_date_price=self.IndexSpecificData.loc[(self.DailyPrices["PriceDate"]==self.RunDate)&(self.DailyPrices["GenericTicker"]=="SPTR"),"Price"].values[0]
         Price_array=list(self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]=="SPTR")&(self.DailyPrices["PriceDate"]!=self.RunDate),["PriceDate","Price"]].sort_values(by="PriceDate").tail(4)["Price"])
         return 2*IL_1*max(-1,Lev*0.2*sum(np.log(Run_date_price)**4)/np.prod(Price_array))
    
     def t_Es(self,ticker):
        settlementDate=self.DailyPrices.loc[(self.DailyPrices["SpecificTicker"]==ticker)&(self.DailyPrices["PriceDate"]==self.RunDate),"SettlementDate"]
        return (settlementDate-self.RunDate)/360

     def Far_expiry(self):
        settlementDates=list(self.DailyPrices.loc[(self.DailyPrices["GenericTicker"].str.contain("Z=R"))&(self.DailyPrices["PriceDate"]==self.RunDate),"SettlementDate"])
        leftnearestdate=bisect.bisect_left(settlementDates,self.Rundate)
        rightnearsetdate=bisect.bisect_right(settlementDates,self.Rundate)
        
        

        
        
        
        
     def OpenIndexSpecific(self):
        t_indexspecificdata = self.IndexSpecificData.loc[self.IndexSpecificData["PriceDate"]==self.LastRunDate,:].copy()
        t_indexspecificdata["PriceDate"] = self.RunDate
        return t_indexspecificdata
        
     @hp.output_file(["VTCloseComposition"])
     def fn_VTCloseComp(self):
#        print(self.NewOpenComposition()) 
        return [self.NewCloseComposition()]
        
     @hp.output_file(["VTOpenComposition", "VTIndexSpecificData"])
     def fn_VTOpenComp(self):
#         pass
        return [self.NewOpenComposition(), self.OpenIndexSpecific()] 
     
class cl_VTCalculation:
    
    VtClassMapper = {"EDEIndexCalculation": EDEIndexCalculation
                     ,"EDUIndexCalculation": EDUIndexCalculation
                     ,"SDIVIndexCalculation": SDIVIndexCalculation
                     ,"MRETIndexCalculation": MRETIndexCalculation
                     ,"RVOLIndexCalculation": RVOLIndexCalculation
                     ,"CUBESDIndexCalculation": CUBESDIndexCalculation
                     ,"CUBESDTRIndexCalculation": CUBESDTRIndexCalculation
                     ,"EquityIndexCalculation": EquityIndexCalculation
                     ,"CitiMomentumIndexCalculation": CitiMomentumIndexCalculation
                     ,"CitiBCOMIndexCalculation": CitiBCOMIndexCalculation
                     ,"NomuraMomentumValueIndexCalculation": NomuraMomentumValueIndexCalculation
                     ,"NomuraMomentumIndexCalculation": NomuraMomentumIndexCalculation
                     ,"MRUEIndexCalculation": MRUEIndexCalculation
                     ,"DSVIXTRIndexCalculation": DSVIXTRIndexCalculation
                     ,"SILIndexCalculation": SILIndexCalculation
                     ,"IXERIndexCalculation": IXERIndexCalculation
                     ,"IXMGCalculation": IXMGCalculation
                     ,"SGUSUICalculation" : SGUSUICalculation
                     ,"SGMOUSCalculation" : SGMOUSCalculation
                     ,"SGE2UICalculation" : SGE2UICalculation
                     ,"SGMOE2Calculation":SGMOE2Calculation
                     ,"NomuraSelectIndexCalculation_Cmdty" : NomuraSelectIndexCalculation_Cmdty
                     ,"MEUUCalculation":MEUUCalculation
                     ,"SGIF4FXCalculation":SGIF4FXCalculation
                     ,"JGCTRCCECalculations":JGCTRCCECalculations
                     ,"CIIRMAGMCalculation":CIIRMAGMCalculation
                     ,"CIEQVUIDCalculation":CIEQVUIDCalculation
                     ,"JTRDX2BUCalculation":JTRDX2BUCalculation
                     ,"SGIXUSGC_IndexCal":SGIXUSGC_IndexCal
                     ,"JPZMFTUSCalculation":JPZMFTUSCalculation
                     ,"JPMSEB3B_IndexCal":JPMSEB3B_IndexCal
                     ,"JPFCVA01Calculation":JPFCVA01Calculation
                     ,"JPEDIVSGCalculation":JPEDIVSGCalculation
                     ,"SGIXTFFX_IndexCal":SGIXTFFX_IndexCal
                     ,"JPVOFXB2_IndexCal":JPVOFXB2_IndexCal
                     ,"CSEAFMRS_IndexCal":CSEAFMRS_IndexCal}
    global VTclassobject
    
    @staticmethod            
    def fn_VTOpenCloseComp(cl_FileProcessor):
        VTclassobject = cl_VTCalculation.VtClassMapper[cl_FileProcessor.VTClassName](cl_FileProcessor)
        VTclassobject.fn_VTCloseComp()
        VTclassobject.fn_VTOpenComp() 
            
class cl_OptionMetricsCalculations:
    
#    print('yahaa2')
    PrClassMapper = {"OptionMetrics": OM.cl_OptionMetrics
                     ,'Ei:US:XX:XX:X:XX:DWSP:XXXX:ML': OM.cl_OptionMetrics
                    }
    
    global OMclassobject
    
    @staticmethod
    def fn_VolSurfaceCalc(cl_FileProcessor):
        OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)
        OMclassobject.fn_LnItpVolSurf()
        
    @staticmethod
    def fn_FwdCurveCalc(cl_FileProcessor):
        OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)
        OMclassobject.fn_FwdCurve()
        
    @staticmethod
    def fn_OptionPricingCalc(cl_FileProcessor):
        print("testcoreProcess")
        OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)
        OMclassobject.fn_OptionPricing()
            
        
    @staticmethod
    def fn_EquityOptionBankOpen(cl_FileProcessor):
        try:
            
            BankOCRawData = cl_FileProcessor.dict_FileData["BankOCRawData"][(cl_FileProcessor.dict_FileData["BankOCRawData"]["Date"] == cl_FileProcessor.RunDate) ].copy()
            BankOCRawData["MaturityDate"] = pd.to_datetime(BankOCRawData["MaturityDate"])
            OptionsOC = BankOCRawData.loc[BankOCRawData["InstrumentVTId"].str.contains("Eo:"),:]
            OptionsOC = OptionsOC.loc[OptionsOC["BankUnit"]!=0,:]                                          
            OptionsOC.reset_index(inplace = True)
            df_Option = pd.DataFrame()
            for options in OptionsOC["InstrumentVTId"]:
                if options == OptionsOC["InstrumentVTId"][0]:
                    InputForOptionPremiumColumns = ["PricingDate","OptionType","StrikePrice","SpotPrice","MaturityDate","RfRate","Volatility","Premium","DividendYield","DividendPV","ImpliedVolCalc","PremiumCalc","PricingModel","UnderlyingVTId","UnderlyingName"]
                    if OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"OptionType"].values[0] == "C":
                        OptionType = 'Call'
                    elif OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"OptionType"].values[0] == "P":
                        OptionType = 'Put'
                        
                    input_dict = {"PricingDate": cl_FileProcessor.RunDate,
                                  "OptionType": OptionType,
                                  "StrikePrice": OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"Strike"].values[0]/1000,
                                  "MaturityDate": OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"MaturityDate"].values[0],
                                  "ImpliedVolCalc": False,
                                  "PremiumCalc": True,
                                  "PricingModel": "BSM"
                                  }
                    cl_FileProcessor.dict_FileData["InputForOptionPremium"] = pd.DataFrame(data = input_dict, columns = InputForOptionPremiumColumns, index = [1])
                    OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)                
                    OMclassobject.fn_InterimOptionPricing()
                    Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
                    temp_dict = {**Option_details, **option_parameters}
                    temp_df = pd.DataFrame(data = temp_dict, index = [1])
                    temp_df["InstrumentVTId"] = options
                    df_Option = df_Option.append(temp_df)
                                    
                else:
                    InputForOptionPremiumColumns = ["PricingDate","OptionType","StrikePrice","SpotPrice","MaturityDate","RfRate","Volatility","Premium","DividendYield","DividendPV","ImpliedVolCalc","PremiumCalc","PricingModel","UnderlyingVTId","UnderlyingName"]
                    if OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"OptionType"].values[0] == "C":
                        OptionType = 'Call'
                    elif OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"OptionType"].values[0] == "P":
                        OptionType = 'Put'
                    
                    OMclassobject.WEB_OP_Input['MaturityDate'] = OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"MaturityDate"].values[0]
                    OMclassobject.WEB_OP_Input['OptionType'] = OptionType
                    OMclassobject.WEB_OP_Input['StrikePrice'] = OptionsOC.loc[OptionsOC["InstrumentVTId"]==options,"Strike"].values[0]/1000
                    Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
                    temp_dict = {**Option_details, **option_parameters}
                    temp_df = pd.DataFrame(data = temp_dict, index = [1])
                    temp_df["InstrumentVTId"] = options                
                    df_Option = df_Option.append(temp_df)
                    
            df_Option.set_index("InstrumentVTId", inplace = True)
            df_Option.rename(columns = {'option_type': 'OptionType', 'pv_div': 'PVDividend', 'spot0': 'UnderlyingPrice',  'rf_rate': "RfRate", 'yield_div':"DividendYield", 'vol':"Volatility"}, inplace = True)
            df_Option.drop(['div_type','expiry_type', 'maturity'], axis = 1, inplace = True)
            OptionsOC.set_index("InstrumentVTId", inplace = True)
            for columns in df_Option.columns:
                OptionsOC.loc[:,columns] = df_Option.loc[:,columns]
            OptionsOC.rename(columns = {"BankUnit": "Units"}, inplace = True)
            BankOCRawData.rename(columns = {"BankUnit": "Units"}, inplace = True)
            NewOpenComposition = pd.DataFrame(columns = cl_FileProcessor.dict_FileData["OpenComposition"].columns)
            OptionsOC.reset_index(inplace = True)
            NewOpenComposition = NewOpenComposition.append(BankOCRawData.loc[~(BankOCRawData["InstrumentVTId"].str.contains("Eo:")),NewOpenComposition.columns])
            NewOpenComposition = NewOpenComposition.append(OptionsOC.loc[:,NewOpenComposition.columns])
            NewOpenComposition["Date"] = cl_FileProcessor.RunDate
#            print("Here")
#            print(NewOpenComposition)
            NewOpenComposition = NewOpenComposition.round(18)
            df_JsonString = NewOpenComposition.to_json(orient = 'records', date_format = 'iso')
            out_path = os.path.join(cl_FileProcessor.directory, "P_OpenComposition_"+cl_FileProcessor.IndexVTId.replace(":","")+"_"+cl_FileProcessor.RunDate.to_pydatetime().strftime('%Y%m%d')+".txt")
            with open(out_path, 'w') as out:
                out.write("Run Status: \n" + df_JsonString)
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")

            
    @staticmethod        
    def fn_PnLCalculator(InputDict, cl_FileProcessor, instrument):

        PnLDict = {}
        InputForPnL = pd.DataFrame(data = InputDict, index = [1])
        InputForPnL.drop(["LastRunPrice", "LastRunDate", "Units", "Price"], axis = 1, inplace = True)
        if np.all(InputForPnL["OptionType"]=="C"):
            InputForPnL["OptionType"] = "Call"
        elif np.all(InputForPnL["OptionType"]=="P"):
            InputForPnL["OptionType"] = "Put"
            
        ["PricingDate","OptionType","StrikePrice","SpotPrice","MaturityDate","RfRate","Volatility","Premium","DividendYield","DividendPV","ImpliedVolCalc","PremiumCalc","PricingModel","UnderlyingVTId","UnderlyingName","LastRunPrice", "LastRunDate", "Units", "Price"]
        """Price Impact Calculation"""            
        temp_input = InputForPnL.copy()
        temp_input["SpotPrice"] = np.nan
        temp_input["PricingDate"] = InputDict["LastRunDate"]
        cl_FileProcessor.dict_FileData["InputForOptionPremium"] = temp_input
        OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)                
#        OMclassobject.WEB_OP_Input.to_csv("PriceImpactInput.csv")        
        OMclassobject.fn_InterimOptionPricing()
        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
        temp_dict = {**Option_details, **option_parameters}
        temp_df = pd.DataFrame(data = temp_dict, index = [1])


        PnLDict["PriceImpact"] = float((temp_df["Price"] - InputDict["LastRunPrice"]) * InputDict["Units"])

        """Interest Rate Impact Calculation"""
        
        OMclassobject.temp_spot = InputDict["SpotPrice"]
        OMclassobject.temp_rfrate = None
        
        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
        temp_dict = {**Option_details, **option_parameters}
        temp_df = pd.DataFrame(data = temp_dict, index = [1])
        PnLDict["InterestRateImpact"] = float((temp_df["Price"] - InputDict["LastRunPrice"]) * InputDict["Units"])

        """Volatility Impact Calculation"""
        OMclassobject.temp_rfrate = InputDict["RfRate"]
        OMclassobject.temp_vol = None
        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
        temp_dict = {**Option_details, **option_parameters}
        temp_df = pd.DataFrame(data = temp_dict, index = [1])
        
        PnLDict["VolatilityImpact"] = float((temp_df["Price"] - InputDict["LastRunPrice"]) * InputDict["Units"])
        if (InputDict["PricingDate"] - InputDict["MaturityDate"]).days == 0:
            PnLDict["VolatilityImpact"] = 0

        """Time Impact Calculation"""
        OMclassobject.temp_vol = InputDict["Volatility"]
        OMclassobject.WEB_OP_Input['PricingDate'] = InputDict["PricingDate"]
        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
        temp_dict = {**Option_details, **option_parameters}
        temp_df = pd.DataFrame(data = temp_dict, index = [1])
        
        PnLDict["TimeImpact"] = float((temp_df["Price"] - InputDict["LastRunPrice"]) * InputDict["Units"])


        """Unexplained"""
        
        PnLDict["Unexplained"] = (InputDict["Price"] - InputDict["LastRunPrice"]) * InputDict["Units"] - sum(PnLDict.values())
        PnLDict["InstrumentVTId"] = instrument
        return PnLDict        
                    
    
    @classmethod
    def fn_EquityOptionBankClose(self,cl_FileProcessor):
        try:
            OpenComposition = cl_FileProcessor.dict_FileData["OpenComposition"].loc[~(cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca:")),:].copy()
            OpenComposition["MaturityDate"] = pd.to_datetime(OpenComposition["MaturityDate"])
            temp_Close = OpenComposition.copy()
            temp_Close = temp_Close.loc[temp_Close["Units"]!=0,:]
            NewOpenPath = os.path.join(cl_FileProcessor.directory, "P_OpenComposition_"+cl_FileProcessor.IndexVTId.replace(":","")+"_"+cl_FileProcessor.RunDate.to_pydatetime().strftime('%Y%m%d')+".txt")
            with open(NewOpenPath, 'r') as in_file:
                JsonString = in_file.readline() #To skip the first "run status" line            
                NewOpenComposition = pd.read_json(in_file.readline(), orient = 'records')
            i=0
            columns_list = ['Delta', 'Gamma', 'Price', 'Rho', 'Theta', 'Vega', 'PVDividend', 'RfRate','UnderlyingPrice', 'Volatility', 'DividendYield']
            temp_Close.set_index("InstrumentVTId", inplace = True)
            NewOpenComposition.set_index("InstrumentVTId", inplace = True)
            for instruments in temp_Close.index:
                if instruments in list(NewOpenComposition.index):
#                    print(NewOpenComposition.loc[NewOpenComposition["InstrumentVTId"]==instruments,columns_list])
                    temp_Close.loc[instruments,columns_list] = NewOpenComposition.loc[instruments,columns_list]
                else:
                    if i == 0:
                        InputForOptionPremiumColumns = ["PricingDate","OptionType","StrikePrice","SpotPrice","MaturityDate","RfRate","Volatility","Premium","DividendYield","DividendPV","ImpliedVolCalc","PremiumCalc","PricingModel","UnderlyingVTId","UnderlyingName"]
                        if temp_Close.loc[instruments,"OptionType"] == "C":
                            OptionType = 'Call'
                        elif temp_Close.loc[instruments,"OptionType"] == "P":
                            OptionType = 'Put'
                            
                        input_dict = {"PricingDate": cl_FileProcessor.RunDate,
                                      "OptionType": OptionType,
                                      "StrikePrice": temp_Close.loc[instruments,"Strike"]/1000,
                                      "MaturityDate": temp_Close.loc[instruments,"MaturityDate"],
                                      "ImpliedVolCalc": False,
                                      "PremiumCalc": True,
                                      "PricingModel": "BSM"
                                      }
                        cl_FileProcessor.dict_FileData["InputForOptionPremium"] = pd.DataFrame(data = input_dict, columns = InputForOptionPremiumColumns, index = [1])
                        OMclassobject = cl_OptionMetricsCalculations.PrClassMapper[cl_FileProcessor.IndexVTId](cl_FileProcessor)                
                        OMclassobject.fn_InterimOptionPricing()
                        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
                        temp_dict = {**Option_details, **option_parameters}
                        temp_df = pd.DataFrame(data = temp_dict, index = [1])
                        temp_df["InstrumentVTId"] = instruments
                        temp_df.rename(columns = {'option_type': 'OptionType', 'pv_div': 'PVDividend', 'spot0': 'UnderlyingPrice',  'rf_rate': "RfRate", 'yield_div':"DividendYield", 'vol':"Volatility"}, inplace = True)
                        temp_Close.loc[instruments,columns_list] = temp_df.set_index("InstrumentVTId").loc[instruments,columns_list]
                        i+=1
                    else:
                        InputForOptionPremiumColumns = ["PricingDate","OptionType","StrikePrice","SpotPrice","MaturityDate","RfRate","Volatility","Premium","DividendYield","DividendPV","ImpliedVolCalc","PremiumCalc","PricingModel","UnderlyingVTId","UnderlyingName"]
                        if temp_Close.loc[instruments,"OptionType"] == "C":
                            OptionType = 'Call'
                        elif temp_Close.loc[instruments,"OptionType"] == "P":
                            OptionType = 'Put'
                    
                        OMclassobject.WEB_OP_Input['MaturityDate'] = temp_Close.loc[instruments,"MaturityDate"]
                        OMclassobject.WEB_OP_Input['OptionType'] = OptionType
                        OMclassobject.WEB_OP_Input['StrikePrice'] = temp_Close.loc[instruments,"Strike"]/1000
                        Option_details, option_parameters = OMclassobject.fn_OptionPricingParameters()
                        temp_dict = {**Option_details, **option_parameters}
                        temp_df = pd.DataFrame(data = temp_dict, index = [1])
                        temp_df["InstrumentVTId"] = instruments                
                        temp_df.rename(columns = {'option_type': 'OptionType', 'pv_div': 'PVDividend', 'spot0': 'UnderlyingPrice',  'rf_rate': "RfRate", 'yield_div':"DividendYield", 'vol':"Volatility"}, inplace = True)
                        temp_Close.loc[instruments,columns_list] = temp_df.set_index("InstrumentVTId").loc[instruments,columns_list]
                        i+=1
            temp_Close.reset_index(inplace = True)
            NewOpenComposition.reset_index(inplace = True)
            df_Cash = cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"].str.contains("Ca:"),:].copy()
            CashUnits = sum(NewOpenComposition["Price"]*NewOpenComposition["Units"]*NewOpenComposition["FxRate"])-sum(temp_Close["Price"]*temp_Close["Units"]*temp_Close["FxRate"])
            df_Cash.loc[:,"Units"] = CashUnits
            NewCloseComposition = pd.DataFrame(columns = cl_FileProcessor.dict_FileData["OpenComposition"].columns)
            NewCloseComposition = NewCloseComposition.append(temp_Close)
            NewCloseComposition = NewCloseComposition.append(df_Cash)
            NewCloseComposition["Date"] = cl_FileProcessor.RunDate

            df_PnL = pd.DataFrame()
            NewCloseComposition.drop(["PriceImpact", "TimeImpact", "VolatilityImpact", "InterestRateImpact", "Unexplained"], axis = 1, inplace = True)
#            print(NewCloseComposition)            
            for instruments in NewCloseComposition["InstrumentVTId"]:
                if "Eo:" in instruments:
                    InputDict = {"PricingDate": cl_FileProcessor.RunDate,
                                 "OptionType": NewCloseComposition.loc[NewCloseComposition["InstrumentVTId"]==instruments,"OptionType"].values[0],
                                 "StrikePrice": NewCloseComposition.loc[NewCloseComposition["InstrumentVTId"]==instruments,"Strike"].values[0]/1000,
                                 "SpotPrice": cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"UnderlyingPrice"].values[0],
                                 "MaturityDate": NewCloseComposition.loc[NewCloseComposition["InstrumentVTId"]==instruments,"MaturityDate"].values[0],
                                 "RfRate": cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"RfRate"].values[0],
                                 "Volatility": cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"Volatility"].values[0],
                                 "DividendYield":  cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"DividendYield"].values[0],
                                 "DividendPV":  cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"PVDividend"].values[0],
                                 "ImpliedVolCalc": False,
                                 "PremiumCalc": True,
                                 "PricingModel": "BSM",
                                 "UnderlyingVTId": None,
                                 "UnderlyingName": None,
                                 "LastRunPrice": cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"Price"].values[0], 
                                 "LastRunDate": cl_FileProcessor.LastRunDate, 
                                 "Units": cl_FileProcessor.dict_FileData["OpenComposition"].loc[cl_FileProcessor.dict_FileData["OpenComposition"]["InstrumentVTId"]==instruments,"Units"].values[0], 
                                 "Price": NewCloseComposition.loc[NewCloseComposition["InstrumentVTId"]==instruments,"Price"].values[0]
                                 }
#                    pd.DataFrame(data = InputDict, index = [1]).to_csv("InputDict.csv")
                    PnLDict = self.fn_PnLCalculator(InputDict, cl_FileProcessor, instruments)
                    temp_PnL = pd.DataFrame(data = PnLDict, index = [1]).set_index("InstrumentVTId")
                    df_PnL = df_PnL.append(temp_PnL)
            NewCloseComposition = pd.concat([NewCloseComposition.set_index("InstrumentVTId"), df_PnL], axis = 1)
            NewCloseComposition.reset_index(inplace = True)
            NewCloseComposition.rename(columns = {"index": "InstrumentVTId"}, inplace = True)
            NewCloseComposition = NewCloseComposition.round(18)
            df_JsonString = NewCloseComposition.to_json(orient = 'records', date_format = 'iso')
            out_path = os.path.join(cl_FileProcessor.directory, "P_CloseComposition_"+cl_FileProcessor.IndexVTId.replace(":","")+"_"+cl_FileProcessor.RunDate.to_pydatetime().strftime('%Y%m%d')+".txt")
            with open(out_path, 'w') as out:
                out.write("Run Status: \n" + df_JsonString)
            cl_FileProcessor.log.process_success()
        except Exception as e:
            cl_FileProcessor.log.process_failure()
            cl_FileProcessor.log.error_reporting(e)
            raise Exception("Process ended with Failure")
            

            
                
                
class cl_ProcessMapper:
    
    processmapper = {PrNames.genBankComp.value: {SubPrNames.genBankComp_StdCloseComp.value: cl_BankOCClose.fn_Stdclose
                                                 ,SubPrNames.genBankComp_DBCloseComp.value: cl_BankOCClose.fn_DBClose
                                                 ,SubPrNames.genBankComp_StdOpenComp.value: cl_BankOCOpen.fn_StdOpen
                                                 ,SubPrNames.genBankComp_IoIOpenSameCurrency.value: cl_BankOCOpen.fn_IoIOpenSameCurrency
                                                 ,SubPrNames.genBankComp_MRUECloseComp.value: cl_BankOCClose.fn_MRUEClose
                                                 ,SubPrNames.genBankComp_IoIMRUEOpen.value: cl_BankOCOpen.fn_IoIMRUEOpen
                                                 ,SubPrNames.genBankComp_OpenBankOCwoCash.value: cl_BankOCOpen.fn_OpenBankOCwoCash
                                                 ,SubPrNames.genBankComp_CloseBankOCwoCash.value: cl_BankOCClose.fn_CloseBankOCwoCash
                                                 ,SubPrNames.genBankComp_IXERClose.value: cl_BankOCClose.fn_IXERClose
                                                 ,SubPrNames.genBankComp_BarclaysExceedOpen.value: cl_BankOCOpen.fn_BarclaysExceedOpen
                                                 ,SubPrNames.genBankComp_LNENClose.value: cl_BankOCClose.fn_LNENClose
                                                 ,SubPrNames.genBankComp_BarclaysExcessReturnOpen.value: cl_BankOCOpen.fn_BarclaysExcessReturnOpen
                                                 ,SubPrNames.genBankComp_RDUEClose.value: cl_BankOCClose.fn_RDUEClose
                                                 ,SubPrNames.genBankComp_RDUEOpen.value: cl_BankOCOpen.fn_RDUEOpen
                                                 ,SubPrNames.genBankComp_EquityOptionBankOpen.value: cl_OptionMetricsCalculations.fn_EquityOptionBankOpen
                                                 ,SubPrNames.genBankComp_EquityOptionBankClose.value: cl_OptionMetricsCalculations.fn_EquityOptionBankClose}
                    ,PrNames.genCompLevel.value: {SubPrNames.genComp2Level_StdComp2woFXLevel.value: cl_Comp2Level.fn_StdComp2LevelwoFX
                                                  ,SubPrNames.genComp2Level_StdComp2Level.value: cl_Comp2Level.fn_StdComp2Level
                                                  ,SubPrNames.genComp2Level_RoundStdComp2Level.value: cl_Comp2Level.fn_RoundStdComp2Level
                                                  ,SubPrNames.genComp2Level_EqComp2Level.value: cl_Comp2Level.fn_EqComp2Level
                                                  ,SubPrNames.genComp2Level_OptionComp2Level.value: cl_Comp2Level.fn_OptionComp2Level}
                    ,PrNames.genVTComp.value: {SubPrNames.genVTComp_OpenClose.value: cl_VTCalculation.fn_VTOpenCloseComp
                                                }
                    ,PrNames.genVolSurf.value: {SubPrNames.genVolSurf_LnIntVolSurf.value: cl_OptionMetricsCalculations.fn_VolSurfaceCalc 
                                                }
                    ,PrNames.genFwdCurve.value: {SubPrNames.genFwdCurve_FwdCurve.value: cl_OptionMetricsCalculations.fn_FwdCurveCalc
                                                 }
                    ,PrNames.genOptionPremium.value: {SubPrNames.genOptionPremium_OptionPremium.value: cl_OptionMetricsCalculations.fn_OptionPricingCalc
                                                    }
                                                
#                    ,PrNames.genVTCompLevel.value: {SubPrNames.genVTCompLevel.value: cl_Comp2Level.fn_StdComp2Level}
                     }