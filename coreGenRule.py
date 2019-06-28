# -*- coding: utf-8 -*-
"""
Created on Mon May 13 10:36:20 2019

@author: tanmay.sidhu
"""
import pylogger
logA=pylogger.loggy()
from core_var import conf

class rule_divisor(conf):
    """This is for Divisor based Methodlogy"""

    def __init__(self):
        "Initialising all the variables used in this Divisor class"
        try:
            super().__init__()
            print("Inside rule divisor init")
            logA.info("Divisor is INITIATED")
            logA.info("Divisor Initiation is SUCCESFULLY done")
        except:
            logA.error("Divisor Initialisation has FAILED")
#    def fn_master(self,df,col1,col1_event,col2,col2_event):
#        self.df.loc[(self.df[col1].isin([i for i in col1_event]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"PriceEx_1"]
        
    def fn_cash_ex_excep(self,Cash_Dividend,Eventvalue):
        """Just a func to have a clean code structure"""
        return self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventName"].isin([Cash_Dividend]),Eventvalue]
#    def fn_sql_cash_div_except(self,Cash_Dividend,Eventvalue):
#        return self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PayDatePrice_1"]

    def fn_s_cash_ex_excep(self,Cash_Dividend,Eventvalue):
         """Just a func to have a clean code structure"""
         return self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin([Cash_Dividend]),Eventvalue]
    def handling_duplicates(self):
        
        #cash Dividend
        #Stock Dividend
        #stock Split
        #rights issue
        #spin off
        df_ex=self.d_final_combinedEx.groupby('CombinedEx')
        for i in list(df_ex.groups):
            if not len(df_ex.get_group(i).loc[df_ex.get_group(i)["EventName"].isin(['Cash Dividend'])])==0:
                if df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"EventName"].count() > 0:
                    au_PriceEx_1=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"PriceEx_1"]
                    au_Fx_1=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"FxEx_1"]
                    au_Taxrate=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"Taxrate"]
                    Frank_Percent=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"Franking_percent"]
                    Income_dis_percent=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"Income_percent"]
                    Ef_Taxrate=au_Taxrate*(1 - Frank_Percent - Income_dis_percent)
                    au_EventAmount=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"EventAmount"].sum()
                    Ef_EventAmount=au_EventAmount(1-Ef_Taxrate)
                    df_ex.get_group(i).loc[df_ex.get_group(i)["CountryCode"].isin(["AU"])&(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"])),"GTR(PAF)"]=(au_PriceEx_1-(au_EventAmount*au_Fx_1))/au_PriceEx_1
                    df_ex.get_group(i).loc[df_ex.get_group(i)["CountryCode"].isin(["AU"])&(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"])),"NTR(PAF)"]=(au_PriceEx_1-(Ef_EventAmount*au_Fx_1))/au_PriceEx_1
                    aus_adj_price_NTR=df_ex.get_group(i).loc[df_ex.get_group(i)["CountryCode"].isin(["AU"])&(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"])),"NTR(PAF)"].values[0]*au_PriceEx_1
                    df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"PriceEx_1_NTR"]=aus_price_NTR
                    aus_adj_price_GTR=df_ex.get_group(i).loc[df_ex.get_group(i)["CountryCode"].isin(["AU"])&(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"])),"GTR(PAF)"].values[0]*au_PriceEx_1
                    df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"PriceEx_1_GTR"]=aus_adj_price_GTR
                    df_ex.get_group(i).loc[df_ex.get_group(i)["CountryCode"].isin(["AU"])&(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"])),"PR(PAF)"]=1
                    df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(df_ex.get_group(i)["CountryCode"].isin(["AU"])),"PriceEx_1_PR"]=au_PriceEx_1
                if len(df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin('Cash Dividend'))&(~df_ex.get_group(i)['CountryCode'].isin(['AU']))]) > 0:
                    PriceEx_1=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(~df_ex.get_group(i)["CountryCode"].isin(["AU"])),"PriceEx_1"]
                    EventAmount=df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin('Cash Dividend'))&(~df_ex.get_group(i)["CountryCode"].isin(['AU'])),"EventAmount"].sum()
                    FxEx_1=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(~df_ex.get_group(i)["CountryCode"].isin(["AU"])),"FxEx_1"]
                    Taxrate=df_ex.get_group(i).loc[(df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]))&(~df_ex.get_group(i)["CountryCode"].isin(["AU"])),"Taxrate"]
                    df_ex.get_group(i).loc[df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"]=(PriceEx_1 - ( EventAmount * FxEx_1 * (Taxrate/100) )) / PriceEx_1
                    adj_price_NTR=df_ex.get_group(i).loc[df_ex.get_group(i)["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"].values[0]*PriceEx_1
                    df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin(['Cash Dividend']))&(~df_ex.get_group(i)['CountryCode'].isin(['AU'])),"NTR_PriceEx_1"]=adj_price
                    df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin(['Cash Dividend']))&(~df_ex.get_group(i)['CountryCode'].isin(['AU'])),"GTR(PAF)"]=(PriceEx_1 - ( EventAmount * FxEx_1) / PriceEx_1)
                    adj_price_GTR=df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin(['Cash Dividend']))&(~df_ex.get_group(i)['CountryCode'].isin(['AU'])),"GTR(PAF)"].values[0]*PriceEx_1
                    df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin(['Cash Dividend']))&(~df_ex.get_group(i)['CountryCode'].isin(['AU'])),"GTR_PriceEx_1"]=adj_price_GTR
                    df_ex.get_group(i).loc[(df_ex.get_group(i)['EventName'].isin(['Cash Dividend']))&(~df_ex.get_group(i)['CountryCode'].isin(['AU'])),"PR(PAF)"]=1
        df=self.d_final_combinedEx.loc[self.d_final_combinedEx["EventName"].isin(["Cash Dividend"])]
        for i in  
        self.d_final_combinedEx.loc[self.d_final_combinedEx["EventName"].isin(["Cash Dividend"])]
        
        

#""" CA adjustment function"""
#
#
#def CA_adjust(current_date, next_opendate, prerebaldate):
#    global df_input_CAo_required1
#    global df_mapping
#    global df_rebal_details
#    global data
#    
#    
#    columns = "2595708"
#    for columns in df_rebal_details["SEDOL-CHK"][df_rebal_details.index == prerebaldate]:
#        
#                if (df_mapping.loc[columns,"RIC"] in np.array(df_input_CAo_required1["RIC"])):
#                    df_input_CAo_required = df_input_CAo_required1[df_input_CAo_required1["RIC"]==df_mapping.loc[columns,"RIC"]]
#                    
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Delisting"]) == 0:
#                        data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = 0
#                        
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Cash Dividend"]) == 0:
#                        #Get Currency of CA
#                        curncy = df_input_CAo_required[df_input_CAo_required["Action Type"]=="Cash Dividend"]["Currency"]
#                        mthd = data["Conversion_methodology"].loc[curncy,:]
#                        if mthd.iloc[0,1] == "inverse":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] - (df_input_CAo_required[df_input_CAo_required["Action Type"]=="Cash Dividend"]["Gross Amount"] / fx_rate.loc[mthd["Ticker"],"Spot Rate"].values/mthd.iloc[0,2]).values))
#                        elif mthd.iloc[0,1] == "multiply":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] - (df_input_CAo_required[df_input_CAo_required["Action Type"]=="Cash Dividend"]["Gross Amount"] * fx_rate.loc[mthd["Ticker"],"Spot Rate"].values/mthd.iloc[0,2]).values))
#
#                    
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Dividend"]) == 0:
#                        data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]/np.array(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Dividend"]["Gross Amount"])))
#                        data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]*np.array(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Dividend"]["Gross Amount"])))
#                    
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Split"]) == 0:
#                        data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]/np.array(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Split"]["Gross Amount"])))
#                        data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float((data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]*np.array(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Stock Split"]["Gross Amount"])))
#                    
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]) == 0:
#                        curncy = df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Currency"]
#                        mthd = data["Conversion_methodology"].loc[curncy,:]                                                  
#                        if mthd.iloc[0,1] == "inverse":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float(np.array((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]+(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Subscription Price"]/mthd.iloc[0,2]/fx_rate.loc[mthd["Ticker"],"Spot Rate"].values*df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Gross Amount"]))/(1+df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Gross Amount"])))
#                        elif mthd.iloc[0,1] == "multiply":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float(np.array((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]+(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Subscription Price"]/mthd.iloc[0,2]*fx_rate.loc[mthd["Ticker"],"Spot Rate"].values*df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Gross Amount"]))/(1+df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Gross Amount"])))
#                        
#                        data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float(np.array(data["Adj_units"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]*(1+df_input_CAo_required[df_input_CAo_required["Action Type"]=="Rights Offerings"]["Gross Amount"])))
#                    
#                    if not len(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]) == 0:
#                        curncy = df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]["Currency"]
#                        mthd = data["Conversion_methodology"].loc[curncy,:]                                                  
#                        if mthd.iloc[0,1] == "inverse":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float(np.array((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]-(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]["Subscription Price"]*df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]["Gross Amount"] / fx_rate.loc[mthd["Ticker"],"Spot Rate"].values /mthd.iloc[0,2]))))    
#                        elif mthd.iloc[0,1] == "multiply":
#                            data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]] = float(np.array((data["Adj_px"].loc[next_opendate,df_mapping.loc[columns,"RIC"]]-(df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]["Subscription Price"]*df_input_CAo_required[df_input_CAo_required["Action Type"]=="Spin-off"]["Gross Amount"] * fx_rate.loc[mthd["Ticker"],"Spot Rate"].values /mthd.iloc[0,2]))))
#"""Function end"""
#    
        
    def cash_dividend(self):
        """Calculation of PAF for Cash Dividend"""
        try:#CHANGE Country CODE to currency HERE"""
        #Handling of Australian stock security
            if self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"EventName"].count() > 0:
#                print("testing",self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"]))].count())
                logA.info("Cash Dividend for exceptional cases Calculation is INITIATED")
                """CHANGE Country CODE to currency HERE"""
                au_PriceEx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"PriceEx_1"]
                au_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"FxEx_1"]
                au_Taxrate=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"Taxrate"]
                Frank_Percent=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"Franking_percent"]
                Income_dis_percent=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"Income_percent"]
                Ef_Taxrate=au_Taxrate*(1 - Frank_Percent - Income_dis_percent)
                au_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"EventAmount"]
                Ef_EventAmount=au_EventAmount(1-Ef_Taxrate)
                self.sql_cash_div_except.loc[self.sql_cash_div["CountryCode"].isin(["AU"])&(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"])),"NTR(PAF)"]=(au_PriceEx_1-(Ef_EventAmount*au_Fx_1))/au_PriceEx_1
            elif self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP","KR"])),"EventName"].count()>0:
                logA.info("Cash Dividend for JP and KR is INITIATED")
                if self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"EventName"].count()>0:
                    if self.kr_user_choice =="pay" and self.jp_user_choice =="pay":
                        k_PriceEx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PayDatePrice_1"]
                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PayDateFx_1"]
                        k_Taxrate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"Taxrate"]
                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"EventAmount"]
                        #calculate PR,NTR,GTR
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PR(PAF)"]=1
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"NTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*(k_Taxrate/100)*k_Fx_1))/k_PriceEx_1                 
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"GTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*k_Fx_1))/k_PriceEx_1
                    elif self.kr_user_choice =="ex" and self.jp_user_choice=='ex':
                        k_PriceEx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PriceEx_1"]
                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"FxEx_1"]
                        k_Taxrate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"Taxrate"]
                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"EventAmount"]
                        #calculate PR,NTR,GTR
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"PR(PAF)"]=1
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"NTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*(k_Taxrate/100)*k_Fx_1))/k_PriceEx_1                 
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR","JP"])),"GTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*k_Fx_1))/k_PriceEx_1
                    elif self.kr_user_choice=="ex":
                        k_PriceEx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"PriceEx_1"]
                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"FxEx_1"]
                        k_Taxrate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"Taxrate"]
                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"EventAmount"]
                        #calculate PR,NTR,GTR
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"PR(PAF)"]=1
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"NTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*(k_Taxrate/100)*k_Fx_1))/k_PriceEx_1                 
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["KR"])),"GTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*k_Fx_1))/k_PriceEx_1
                    elif self.jp_user_choice=="pay":
                        k_PriceEx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"PayDatePrice_1"]
                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"PayDateFx_1"]
                        k_Taxrate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"Taxrate"]
                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"EventAmount"]
                        #calculate PR,NTR,GTR
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"PR(PAF)"]=1
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"NTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*(k_Taxrate/100)*k_Fx_1))/k_PriceEx_1                 
                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventName"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["CountryCode"].isin(["JP"])),"GTR(PAF)"]=(k_PriceEx_1 - (k_EventAmount*k_Fx_1))/k_PriceEx_1
                        
                                
                    
            else:
                logA.info("Cash Dividend for exceptional cases Calculation: NO RECORDS FOUND")
        except Exception as e:
            logA.error(e)
            logA.error("Cash Dividend for exceptional cases Calculation has FAILED")
        try:
            logA.info("Cash Dividend for non exceptional cases Calculation is INITIATED")
            self.sql_cash_div_ex_excep["GTR(PAF)"]=(self.sql_cash_div_ex_excep["PriceEx_1"] - ( self.sql_cash_div_ex_excep["EventAmount"] * self.sql_cash_div_ex_excep["FxEx_1"]))/self.sql_cash_div_ex_excep["PriceEx_1"]
            EventAmount=self.fn_cash_ex_excep("Cash Dividend","EventAmount")
            logA.debug("NTR_EventAmount_{0}".format(EventAmount))
            PriceEx_1=self.fn_cash_ex_excep("Cash Dividend","PriceEx_1")
            logA.debug("NTR_PriceEx_1_{0}".format(PriceEx_1))
            FxEx_1=self.fn_cash_ex_excep("Cash Dividend","FxEx_1")
            logA.debug("NTR_FxEx_1_{0}".format(FxEx_1))
            Taxrate=self.fn_cash_ex_excep("Cash Dividend","Taxrate")
            logA.debug("NTR_Taxrate_{0}".format(Taxrate))
            self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"]=(PriceEx_1 - ( EventAmount * FxEx_1 * (Taxrate/100) )) / PriceEx_1
            logA.info("NTR Calculation is SUCCESFULLY done")
            self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventName"]=="Cash Dividend","PR"]=1
            logA.info("Cash Dividend Calculation is SUCCESFULLY done")                          
        except Exception as e:
            logA.error(e)
            logA.error("Cash Dividend for non exceptional cases Calculation has FAILED")
            
    def special_cash_dividend(self):
#        print("hey",self.sql_cash_div_ex_excep.loc[~(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"])),"EventName"].count())
        
        """Calculation for special cash Dividend and similar kind of treatment"""
        try:
            #Exceptional cases will be handled the same as below
            logA.info("Special Cash Dividend for non exceptional cases Calculation is INITIATED")
            if self.sql_cash_div_ex_excep.loc[~(self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"])),"EventName"].count()>0:
                
                s_EventAmount=self.fn_s_cash_ex_excep("Cash Dividend","EventAmount")
                s_PriceEx_1=self.fn_s_cash_ex_excep("Cash Dividend","PriceEx_1")
                s_FxEx_1=self.fn_s_cash_ex_excep("Cash Dividend","FxEx_1")
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"]=(s_PriceEx_1 - ( s_EventAmount *s_FxEx_1 )) / s_PriceEx_1                            
                PriceEx_1=self.fn_s_cash_ex_excep("Cash Dividend","PriceEx_1")
                logA.debug("Total No of special cash dividend(non exceptional cases): {0}".format(PriceEx_1.count()))
                logA.debug("PriceEx_1_{0}".format(PriceEx_1))
                FxEx_1=self.fn_s_cash_ex_excep("Cash Dividend","FxEx_1")
                logA.debug("FxEx_1_{0}".format(FxEx_1))
                DividendAmount=self.fn_s_cash_ex_excep("Cash Dividend","EventAmount")
                logA.debug("DividendAmount_{0}".format(DividendAmount))
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"PR"]=(PriceEx_1 -( DividendAmount * FxEx_1))/PriceEx_1
                self.sql_cash_div_ex_excep.to_csv("testt.csv")
                logA.info("Special Cash Dividend for non exceptional cases Calculation is SUCCESFULLY done")
            else:
                logA.info("Special Cash Dividend for non exceptional cases : NO RECORDS FOUND")
                pass
        except Exception as e:
            logA.error(e)
            logA.error("Special Cash Dividend Calculation has FAILED")
            pass
    def stock_dividend(self):
        try:
            logA.info("Stock Dividend Calculation is INITIATED")
#            self.sql_others
            
        except Exception as e:
            logA.error(e)
            logA.error("Stock Dividend Calculation has FAILED")
            pass
    def stock_split(self):
        try:
            logA.info("Stock Dividend Calculation is INITIATED")
            pass
        except Exception as e:
            logA.error(e)
            logA.error("Special Cash Dividend Calculation has FAILED")
            pass
    def rights_issue(self):
        try:
            logA.info("Rights Issue Calculation is INITIATED")
            pass
        except Exception as e:
            logA.error(e)
            logA.error("Rights Issue Calculation has FAILED")
            pass

check=rule_divisor()

check.cash_dividend()
#check.special_cash_dividend()
