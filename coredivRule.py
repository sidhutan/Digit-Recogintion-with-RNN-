# -*- coding: utf-8 -*-
"""
Created on Tue Jul 16 11:33:08 2019

@author: tanmay.sidhu
"""

# -*- coding: utf-8 -*-
"""
Created on Mon May 13 10:36:20 2019

@author: tanmay.sidhu
"""
import pylogger
logA=pylogger.loggy()
from core_var import conf
import pandas as pd
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
#        self.df.loc[(self.df[col1].isin([i for i in col1_event]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"PriceExT1"]
        
    def fn_cash_ex_excep(self,event_type,eventvalue):
        """Just a func to have a clean code Structure"""
        return self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventType"].isin([event_type]),eventvalue]

    def fn_s_cash_ex_excep(self,event_type,eventvalue):
         """Just a func to have a clean code Structure"""
         return self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin([event_type]),eventvalue]
    def fn_s_cash_excep(self,event_type,eventvalue):
         return self.sql_cash_div_except.loc[self.sql_cash_div_except["EventType"].isin([event_type]),eventvalue]
    def fn_handling_duplicates(self,df1):
        
        """Handling sequence """
        #cash Dividend
        #Stock Dividend
        #stock Split
        #rights issue
        #spin off
        if df1==self.d_final_combinedPay:
            counter=0
            """Only Cash and  Dividend"""
            df_pay=self.d_final_combinedPay.groupby("CombinedPay")
            for i in list(df_pay.groups):
                grp_name=i
                i=df_pay.get_group(i)
                if len(i.loc[i["EventType"].isin(["Cash Dividend"])])>0:
                    counter=counter+1
                    if i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"]).isin(["JP","KR"]),"EventType"].count() > 0:
                        jp_kr_PriceExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PriceExT1"]
                        jp_kr_FxExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"FxExT1"]
                        jp_kr_TaxRate=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"TaxRate"]
                        jp_kr_event_amount=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"EventAmount"].sum()
#                        print("dem",(jp_kr_PriceExT1-(jp_kr_event_amount)*(jp_kr_FxExT1))/jp_kr_PriceExT1)
                        GTR_PAF= (jp_kr_PriceExT1-(jp_kr_event_amount)*(jp_kr_FxExT1))/jp_kr_PriceExT1                       
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"GTR(PAF)"]=GTR_PAF
                        NTR_PAF=(jp_kr_PriceExT1-(jp_kr_event_amount)*(jp_kr_FxExT1)*(jp_kr_TaxRate/100))/jp_kr_PriceExT1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NTR(PAF)"]=NTR_PAF
                        PR_PAF=1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PR(PAF)"]=PR_PAF
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NSAF"]=1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"GTR_PriceExT1"]=jp_kr_PriceExT1*GTR_PAF
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NTR_PriceExT1"]=jp_kr_PriceExT1*NTR_PAF
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PR_PriceExT1"]=jp_kr_PriceExT1*PR_PAF
                    if i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"]).isin(["JP","KR"]),"EventType"].count() > 0:
                        if counter>0:
                            jp_kr_PriceExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PriceExT1"]
                        else:
                            jp_kr_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PriceExT1"]
                        
                        jp_kr_FxExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"FxExT1"]
                        jp_kr_TaxRate=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"TaxRate"]
                        jp_kr_event_amount=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"EventAmount"].sum()
                        GTR_PAF=(jp_kr_PriceExT1-(jp_kr_event_amount)*(jp_kr_FxExT1))/jp_kr_PriceExT1
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"GTR(PAF)"]=GTR_PAF
                        NTR_PAF=(jp_kr_PriceExT1-(jp_kr_event_amount)*(jp_kr_FxExT1))/jp_kr_PriceExT1
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NTR(PAF)"]=NTR_PAF
                        PR_PAF=1
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PR(PAF)"]=PR_PAF
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NSAF"]=1
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"GTR_PriceExT1"]=jp_kr_PriceExT1*GTR_PAF
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"NTR_PriceExT1"]=jp_kr_PriceExT1*NTR_PAF
                        i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["JP","KR"])),"PR_PriceExT1"]=jp_kr_PriceExT1*PR_PAF
                        
        if df1==self.d_final_combinedEx:
            counter=0
            df_ex=self.d_final_combinedEx.groupby('CombinedEx')
            for i in list(df_ex.groups):
                grp_name=i
                i=df_ex.get_group(i)
                
                if len(i.loc[i["EventType"].isin(['Cash Dividend'])]) > 0:
                    print("Inside duplicates: Cash Dividend")
                    counter=counter+1
                    i.loc[i["EventType"].isin(['Cash Dividend']),"Counter"]=counter
                    if i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventType"].count() > 0:
                        print("Inside duplicates: Cash Dividend_AU")
                        au_PriceExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"PriceExT1"]
                        au_Fx_1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"FxExT1"]
                        au_TaxRate=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"TaxRate"]
                        Frank_Percent=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"FrankingPercent"]
                        Income_dis_percent=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"IncomePercent"]
                        Ef_TaxRate=au_TaxRate*(1 - Frank_Percent - Income_dis_percent)
                        au_EventAmount=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventAmount"].sum()
                        Ef_EventAmount=au_EventAmount(1-Ef_TaxRate)
                        i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Cash Dividend"])),"GTR(PAF)"]=(au_PriceExT1-(au_EventAmount*au_Fx_1))/au_PriceExT1
                        i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Cash Dividend"])),"NTR(PAF)"]=(au_PriceExT1-(Ef_EventAmount*au_Fx_1))/au_PriceExT1
                        aus_adj_price_NTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Cash Dividend"])),"NTR(PAF)"].values[0]*au_PriceExT1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NTR_PriceExT1"]=aus_adj_price_NTR
                        aus_adj_price_GTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Cash Dividend"])),"GTR(PAF)"].values[0]*au_PriceExT1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"GTR_PriceExT1"]=aus_adj_price_GTR
                        i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Cash Dividend"])),"PR(PAF)"]=1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"PR_PriceExT1"]=au_PriceExT1*1
                        i.loc[(i["EventType"].isin(["Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NSAF"]=1
                    if len(i.loc[(i["EventType"].isin('Cash Dividend'))&(~i['LocalCurrency'].isin(['AU']))]) > 0:
                        print("Inside duplicates: cash dividend other than AU")
                        PriceExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"PriceExT1"]
                        EventAmount=i.loc[(i['EventType'].isin('Cash Dividend'))&(~i["LocalCurrency"].isin(['AU'])),"EventAmount"].sum()
                        FxExT1=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"FxExT1"]
                        TaxRate=i.loc[(i["EventType"].isin(["Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"TaxRate"]
                        i.loc[i["EventType"].isin(["Cash Dividend"]),"NTR(PAF)"]=(PriceExT1 - ( EventAmount * FxExT1 * (TaxRate/100) )) / PriceExT1
                        adj_price_NTR=i.loc[i["EventType"].isin(["Cash Dividend"]),"NTR(PAF)"].values[0]*PriceExT1
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"NTR_PriceExT1"]=adj_price_NTR
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"GTR(PAF)"]=(PriceExT1 - ( EventAmount * FxExT1) / PriceExT1)
                        adj_price_GTR=i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"GTR(PAF)"].values[0]*PriceExT1
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"GTR_PriceExT1"]=adj_price_GTR
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"PR(PAF)"]=1
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"PR_PriceExT1"]=PriceExT1*1
                        i.loc[(i['EventType'].isin(['Cash Dividend']))&(~i['LocalCurrency'].isin(['AU'])),"NSAF"]=1
                        
                    if len(i.loc[(i['EventType'].isin(['Special Cash Dividend']))]) > 0:
                        print("Inside duplicates: special cash")
                        counter=counter+1
                        i.loc[(i['EventType'].isin(['Special Cash Dividend'])),"Counter"]=counter
                        if i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventType"].count() > 0:
                            print("Inside duplicates: special cash_AU")
                            GTR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"GTR_PriceExT1"].values[0]
                            NTR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NTR_PriceExT1"].values[0]
                            PR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"PR_PriceExT1"].values[0]    
    #                        eventamount=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventAmount"].values[0]
                            au_Fx_1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"FxExT1"]
                            au_EventAmount=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventAmount"].sum()
                            i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"GTR(PAF)"]=(GTR_PriceExT1-(au_EventAmount*au_Fx_1))/GTR_PriceExT1
                            i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"NTR(PAF)"]=(NTR_PriceExT1-(au_EventAmount*au_Fx_1))/NTR_PriceExT1
                            aus_adj_price_NTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"NTR(PAF)"].values[0]*NTR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NTR_PriceExT1"]=aus_adj_price_NTR
                            aus_adj_price_GTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"GTR(PAF)"].values[0]*GTR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"GTR_PriceExT1"]=aus_adj_price_GTR
                            i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"PR(PAF)"]=(au_PriceExT1-(au_EventAmount*au_Fx_1))/GTR_PriceExT1
                            aus_adj_price_PR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"PR(PAF)"].values[0]*PR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"PR_PriceExT1"]=aus_adj_price_PR
                        if len(i.loc[(i["EventType"].isin('Special Cash Dividend'))&(~i['LocalCurrency'].isin(['AU']))]) > 0:
                            GTR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"GTR_PriceExT1"].values[0]    
                            NTR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"NTR_PriceExT1"].values[0]
                            PR_PriceExT1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"PR_PriceExT1"].values[0]    
    #                        eventamount=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"EventAmount"].values[0]
                            au_Fx_1=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"FxExT1"]
                            au_EventAmount=i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(~i["LocalCurrency"].isin(["AU"])),"EventAmount"].sum()
                            i.loc[(~i["LocalCurrency"].isin(["AU"]))&(i["EventType"].isin(["Special Cash Dividend"])),"GTR(PAF)"]=(GTR_PriceExT1-(au_EventAmount*au_Fx_1))/GTR_PriceExT1
                            i.loc[(~i["LocalCurrency"].isin(["AU"]))&(i["EventType"].isin(["Special Cash Dividend"])),"NTR(PAF)"]=(NTR_PriceExT1-(au_EventAmount*au_Fx_1))/NTR_PriceExT1
                            aus_adj_price_NTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"NTR(PAF)"].values[0]*NTR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NTR_PriceExT1"]=aus_adj_price_NTR
                            aus_adj_price_GTR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"GTR(PAF)"].values[0]*GTR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"GTR_PriceExT1"]=aus_adj_price_GTR
                            i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"PR(PAF)"]=(au_PriceExT1-(au_EventAmount*au_Fx_1))/GTR_PriceExT1
                            aus_adj_price_PR=i.loc[i["LocalCurrency"].isin(["AU"])&(i["EventType"].isin(["Special Cash Dividend"])),"PR(PAF)"].values[0]*PR_PriceExT1
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"PR_PriceExT1"]=aus_adj_price_PR
                            i.loc[(i["EventType"].isin(["Special Cash Dividend"]))&(i["LocalCurrency"].isin(["AU"])),"NSAF"]=1
                    if len(i.loc[i["EventType"].isin(["Stock Dividend"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventType"].isin(["Stock Dividend"]),"Counter"]=counter
                        i.loc[i["EventType"].isin(['Cash Dividend'])]
                        TermNewShares=i.loc[(i["EventType"].isin(["Stock Dividend"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventType"].isin(["Stock Dividend"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        PAF=1/1+m
                        GTR_PriceExT1=i["GTR_PriceExT1"]*PAF
                        NTR_PriceExT1=i["NTR_PriceExT1"]*PAF
                        PR_PriceExT1=i["PR_PriceExT1"]*PAF
                        i.loc[i["EventType"].isin(["Stock Dividend"]),"GTR_PriceExT1"]=GTR_PriceExT1
                        i.loc[i["EventType"].isin(["Stock Dividend"]),"NTR_PriceExT1"]=NTR_PriceExT1
                        i.loc[i["EventType"].isin(["Stock Dividend"]),"PR_PriceExT1"]=PR_PriceExT1
                        i.loc[i["EventType"].isin(["Stock Dividend"]),"NSAF"]=1-m
                    if len(i.loc[i["EventType"].isin(["Stock Split"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventType"].isin(["Stock Split"]),"Counter"]=counter
                        TermNewShares=i.loc[(i["EventType"].isin(["Stock Split"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventType"].isin(["Stock Split"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        i.loc[i["EventType"].isin(["Stock Split"]),"NSAF"]=m
                        PAF=1/m
                        GTR_PriceExT1=i["GTR_PriceExT1"]*PAF
                        NTR_PriceExT1=i["NTR_PriceExT1"]*PAF
                        PR_PriceExT1=i["PR_PriceExT1"]*PAF
                    if len(i.loc[i["EventType"].isin(["Rights Issue"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventType"].isin(["Rights Issue"]),"Counter"]=counter
                        GTR_PriceExT1=i["GTR_PriceExT1"]
                        NTR_PriceExT1=i["NTR_PriceExT1"]
                        PR_PriceExT1=i["PR_PriceExT1"]
                        TermNewShares=i.loc[(i["EventType"].isin(["Rights Issue"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventType"].isin(["Rights Issue"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        offerprice=i.loc[(i["EventType"].isin(["Rights Issue"])),"OfferPrice"].values[0]
                        PAF_G=(GTR_PriceExT1+(m*offerprice))/GTR_PriceExT1*(1-m)
                        PAF_N=(NTR_PriceExT1+(m*offerprice))/NTR_PriceExT1*(1-m)
                        PAF_P=(PR_PriceExT1+(m*offerprice))/PR_PriceExT1*(1-m)
                        GTR_PriceExT1=i["GTR_PriceExT1"]*PAF_G
                        NTR_PriceExT1=i["NTR_PriceExT1"]*PAF_N
                        PR_PriceExT1=i["PR_PriceExT1"]*PAF_P
                        i.loc[i["EventType"].isin(["Rights Issue"]),"NSAF"]=1+m
                    """Need to include the NSAF"""
                            #no spin off cases
    #                if len(i["EventType"].isin(["Spin off"])) >0:
    #                    GTR_PriceExT1=i["GTR_PriceExT1"]
    #                    NTR_PriceExT1=i["NTR_PriceExT1"]
    #                    PR_PriceExT1=i["PR_PriceExT1"]
    #                    TermsNewShares=i.loc[(i["EventType"].isin(["Spin off"],"TermNewShares"))].values[0]
    #                    TermsOldShares=i.loc[(i["EventType"].isin(["Spin off"],"TermOldShares"))].values[0]
                closeprice=i.loc[i["Counter"]==max(i["Counter"]),"PriceExT1"].values[0]
                adj_GTR_price=i.loc[i["Counter"]==max(i["Counter"]),"GTR_PriceExT1"].values[0]
                adj_NTR_price=i.loc[i["Counter"]==max(i["Counter"]),"NTR_PriceExT1"].values[0]
                adj_PR_price=i.loc[i["Counter"]==max(i["Counter"]),"PR_PriceExT1"].values[0]
    #            types = {"TYPE":["PR","GTR","NTR"]}
                df = pd.DataFrame(columns=["PR_PAF","GTR_PAF","NTR_PAF","PR_SAF","GTR_SAF","NTR_SAF"])
                if closeprice==adj_GTR_price:
                    df.loc[grp_name,"GTR_PAF"] = 1
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
    #                df.loc[df[i]=="GTR","PAF"] = 1
                else:
                    df.loc[grp_name,"GTR_PAF"]=adj_GTR_price/closeprice
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
#                    df.loc[grp_name,"SAF"]=
    #                df.loc[df["TYPE"]=="GTR","PAF"] = adj_GTR_price/closeprice
                if closeprice== adj_NTR_price:
                    df.loc[grp_name,"NTR_PAF"]=1
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
    #                df.loc[df["TYPE"]=="NTR","PAF"] = 1
                else:
                    df.loc[grp_name,"NTR_PAF"]=adj_NTR_price/closeprice
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
    #                df.loc[df["TYPE"]=="NTR","PAF"] = adj_NTR_price/closeprice
                if closeprice== adj_NTR_price:
                    df.loc[grp_name,"PR_PAF"]=1
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
    #                df.loc[df["TYPE"]=="PR","PAF"] = 1
                else:
                    df.loc[grp_name,"PR_PAF"]=adj_PR_price/closeprice
                    df.loc[grp_name,"NSAF"]=i.loc["NSAF"].prod()
    #                df.loc[df["TYPE"]=="PR","PAF"] = adj_PR_price/closeprice
                df.to_csv("df.csv")                 
                                            
                                        
                            
                        
                    
                                   
                        
                    
                        
                        
                        
                                       
                        
                        
                        
    
        

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
    def duplicate_handler(self):
        if (self.d_final_combinedEx is not None) or (not self.d_final_combinedEx.empty):
            self.fn_handling_duplicates(self,self.d_final_combinedEx)
        if (self.d_final_combinedPay is not None) or (not self.d_final_combinedPay.empty):
            self.fn_handling_duplicates(self,self.d_final_combinedPay)
            
        
    def cash_dividend(self):
        """Calculation of PAF for Cash Dividend"""
        try:#CHANGE Country CODE to currency HERE"""
        #Handling of AuSCRalian stock security
            if self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"EventType"].count() > 0:
#                print("testing",self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"]))].count())
                logA.info("Cash Dividend for exceptional cases Calculation is INITIATED")
                """CHANGE Country CODE to currency HERE"""
                au_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"PriceExT1"]
                print("Inside Cash divi_AU")
                au_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"FxExT1"]
                au_TaxRate=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"TaxRate"]
                Frank_Percent=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"FrankingPercent"]
                Income_dis_percent=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"IncomePercent"]
                Ef_TaxRate=(au_TaxRate/100)*(1 - Frank_Percent - Income_dis_percent)
                au_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"EventAmount"]
                Ef_EventAmount=au_EventAmount(1-Ef_TaxRate)
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"Div_GTR(PAF)"]=(au_PriceExT1-(au_EventAmount*au_Fx_1))/au_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"Div_NTR(PAF)"]=(au_PriceExT1-(Ef_EventAmount*au_Fx_1))/au_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"Div_PR(PAF)"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"Div_(NSAF)"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"SCR_GTR(UAF)"]=(au_PriceExT1)/(au_PriceExT1-(au_EventAmount*au_Fx_1))
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"SCR_NTR(UAF)"]=(au_PriceExT1)/(au_PriceExT1-(Ef_EventAmount*au_Fx_1))
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"SCR_NTR(PAF)"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"AddC_GTR(CASH)"]=au_EventAmount*au_Fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"AddC_NTR(CASH)"]=Ef_EventAmount*au_Fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"])),"AddC_PR(CASH)"]=0
                                             
#            elif self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])),"EventType"].count()>0:
#                logA.info("Cash Dividend for JP and KR is INITIATED")
#                if self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"EventType"].count()>0:
#                    if self.kr_user_choice =="pay" and self.jp_user_choice =="pay":
#                        k_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"PayDatePrice_1"]
#                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"PayDateFx_1"]
#                        k_TaxRate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"TaxRate"]
#                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"EventAmount"]
#                        #calculate PR,NTR,GTR
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"PR(PAF)"]=1
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"NTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))/k_PriceExT1                 
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"GTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*k_Fx_1))/k_PriceExT1
#                    elif self.kr_user_choice =="ex" and self.jp_user_choice=='ex':
#                        k_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"PriceExT1"]
#                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"FxExT1"]
#                        k_TaxRate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"TaxRate"]
#                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"EventAmount"]
#                        #calculate PR,NTR,GTR
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"PR(PAF)"]=1
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"NTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))/k_PriceExT1                 
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR","JP"])),"GTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*k_Fx_1))/k_PriceExT1
#                    elif self.kr_user_choice=="ex":
#                        k_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"PriceExT1"]
#                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"FxExT1"]
#                        k_TaxRate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"TaxRate"]
#                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"EventAmount"]
#                        #calculate PR,NTR,GTR
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"PR(PAF)"]=1
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"NTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))/k_PriceExT1                 
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["KR"])),"GTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*k_Fx_1))/k_PriceExT1
#                    elif self.jp_user_choice=="pay":
#                        k_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"PayDatePrice_1"]
#                        k_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"PayDateFx_1"]
#                        k_TaxRate=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"TaxRate"]
#                        k_EventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"EventAmount"]
#                        #calculate PR,NTR,GTR
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"PR(PAF)"]=1
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"NTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))/k_PriceExT1                 
#                        self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["JP"])),"GTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*k_Fx_1))/k_PriceExT1
#                        
                                
                    
            else:
                logA.info("Cash Dividend for exceptional cases Calculation: NO RECORDS FOUND")
        except Exception as e:
            logA.error(e)
            logA.error("Cash Dividend for exceptional cases Calculation has FAILED")
        try:
            if self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]),"EventType"].count() > 0:
                
                logA.info("Cash Dividend for non exceptional cases Calculation is INITIATED")
                self.sql_cash_div_ex_excep["GTR(PAF)"]=(self.sql_cash_div_ex_excep["PriceExT1"] - ( self.sql_cash_div_ex_excep["EventAmount"] * self.sql_cash_div_ex_excep["FxExT1"]))/self.sql_cash_div_ex_excep["PriceExT1"]
                EventAmount=self.fn_cash_ex_excep("Cash Dividend","EventAmount")
                logA.debug("NTR_EventAmount_{0}".format(EventAmount))
                PriceExT1=self.fn_cash_ex_excep("Cash Dividend","PriceExT1")
                logA.debug("NTR_PriceExT1_{0}".format(PriceExT1))
                FxExT1=self.fn_cash_ex_excep("Cash Dividend","FxExT1")
                logA.debug("NTR_FxExT1_{0}".format(FxExT1))
                TaxRate=self.fn_cash_ex_excep("Cash Dividend","TaxRate")
                logA.debug("NTR_TaxRate_{0}".format(TaxRate))
                self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]),"NTR(PAF)"]=(PriceExT1 - ( EventAmount * FxExT1 * (TaxRate/100) )) / PriceExT1
                logA.info("NTR Calculation is SUCCESFULLY done")
                self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventType"]=="Cash Dividend","PR"]=1
                logA.info("Cash Dividend Calculation is SUCCESFULLY done")
                self.sql_cash_div_ex_excep.to_csv("sql_cash_div_ex_excep.csv")
        except Exception as e:
            logA.error(e)
            logA.error("Cash Dividend for non exceptional cases Calculation has FAILED")
        try:
            if self.nd_final_combinedPay.loc[self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]),"EventType"].count() > 0:
                
                k_PriceExT1=self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"PayDatePrice_1"]
                k_Fx_1=self.nd_final_combinedPay.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"PayDateFx_1"]
                k_TaxRate=self.nd_final_combinedPay.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"TaxRate"]
                k_EventAmount=self.nd_final_combinedPay.loc[(self.sql_cash_div_except["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"EventAmount"]
                #calculate PR,NTR,GTR
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"Div_PR(PAF)"]=1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"Div_NTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))/k_PriceExT1                 
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"Div_GTR(PAF)"]=(k_PriceExT1 - (k_EventAmount*k_Fx_1))/k_PriceExT1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"Div_(NSAF)"]=1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"SCR_GTR(UAF)"]=k_PriceExT1/(k_PriceExT1 - (k_EventAmount*k_Fx_1))
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"SCR_NTR(UAF)"]=k_PriceExT1/(k_PriceExT1 - (k_EventAmount*(k_TaxRate/100)*k_Fx_1))
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"SCR_PR(UAF)"]=1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"SCR_(PAF)"]=1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"AdC_GTR(CASH)"]=k_EventAmount*k_Fx_1
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"AdC_NTR(CASH)"]=k_EventAmount*k_Fx_1*(k_TaxRate/100)
                self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["KR","JP"])),"AdC_PR(CASH)"]=0
                self.nd_final_combinedPay.to_csv("nd_f_combinedpay.csv")
        except Exception as e:
            logA.error(e)
            logA.error("Cash Dividend on Paydate handling has FAILED")
            
    def special_cash_dividend(self):
        """Calculation for special cash Dividend and similar kind of treatment"""
        try:
            #Exceptional cases will be handled the same as below
            logA.info("Special Cash Dividend for non exceptional cases Calculation is INITIATED")
            if (not self.sql_cash_div_ex_excep.empty) & (self.sql_cash_div_ex_excep.loc[(self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"])),"EventType"].count()>0):
                s_EventAmount=self.fn_s_cash_ex_excep("Special Cash Dividend","EventAmount")
                s_PriceExT1=self.fn_s_cash_ex_excep("Special Cash Dividend","PriceExT1")
                s_FxExT1=self.fn_s_cash_ex_excep("Special Cash Dividend","FxExT1")
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"Div_GTR(PAF)"]=(s_PriceExT1 - ( s_EventAmount *s_FxExT1 )) / s_PriceExT1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"Div_NTR(PAF)"]=(s_PriceExT1 - ( s_EventAmount *s_FxExT1 )) / s_PriceExT1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"Div_PR(PAF)"]=(s_PriceExT1 -( s_EventAmount * FxExT1)) / s_PriceExT1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"Div_NSAF"]=1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"SCR_GTR(UAF)"]=(s_PriceExT1)/(s_EventAmount * FxExT1)
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"SCR_NTR(UAF)"]=(s_PriceExT1)/(s_EventAmount * FxExT1)
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"SCR_PR(UAF)"]=(s_PriceExT1)/(s_EventAmount * FxExT1)
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"SCR_PAF"]=1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"AdC_GTR(CASH)"]=s_EventAmount*FxExT1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"AdC_NTR(CASH)"]=s_EventAmount*FxExT1
                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Special Cash Dividend"]),"AdC_PR(CASH)"]=0
                logA.debug("Total No of special cash dividend(non exceptional cases): {0}".format(s_PriceExT1.count()))
                logA.debug("PriceExT1_{0}".format(s_PriceExT1))
                logA.debug("FxExT1_{0}".format(s_FxExT1))
                logA.debug("DividendAmount_{0}".format(s_EventAmount))
#                self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventType"].isin(["Cash Dividend"]),"PR"]=(PriceExT1 -( DividendAmount * FxExT1))/PriceExT1
                logA.info("Special Cash Dividend for non exceptional cases Calculation is SUCCESFULLY done")
            else:
                logA.info("Special Cash Dividend for non exceptional cases : NO RECORDS FOUND")
                pass
            #handling depends 
            if (not self.sql_cash_div_except.empty) &(self.sql_cash_div_except.loc[self.sql_cash_div_except['EventType'].isin(["AU"]),'Special Cash Dividend'].count() > 0):
                au_eventAmount=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"EventAmount"]
                au_PriceExT1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"PriceExT1"]
                au_Fx_1=self.sql_cash_div_except.loc[(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"]))&(self.sql_cash_div_except["LocalCurrency"].isin(["AU"])),"FxExT1"]
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_GTR(PAF)"]=(au_PriceExT1-(au_eventAmount*au_Fx_1))/au_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_NTR(PAF)"]=(au_PriceExT1-(au_eventAmount*au_Fx_1))/au_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_PR(PAF)"]=(au_PriceExT1-(au_eventAmount*au_Fx_1))/au_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_NSAF"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_GTR(UAF)"]=(au_PriceExT1)/(au_eventAmount*au_Fx_1)
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_NTR(UAF)"]=(au_PriceExT1)/(au_eventAmount*au_Fx_1)
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_PR(UAF)"]=(au_PriceExT1)/(au_eventAmount*au_Fx_1)
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_PAF"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"AdC_GTR(CASH)"]=au_eventAmount*au_Fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Adc_NTR(CASH)"]=au_eventAmount*au_Fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["AU"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Adc_PR(CASH)"]=au_eventAmount*au_Fx_1
                                             
            if (not self.nd_final_combinedPay.empty) or (self.nd_final_combinedPay.loc[(self.nd_final_combinedPay['EventType'].isin(["JP","KR"])),'Special Cash Dividend'].count() > 0):
                jp_kr_PriceExT1=self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Special Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["JP","KR"])),"PayDatePrice_1"]
                jp_kr_eventAmount=self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Special Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["JP","KR"])),"EventAmount"]
                jp_kr_fx_1=au_Fx_1=self.nd_final_combinedPay.loc[(self.nd_final_combinedPay["EventType"].isin(["Special Cash Dividend"]))&(self.nd_final_combinedPay["LocalCurrency"].isin(["JP","KR"])),"PayDateFx_1"]
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_GTR(PAF)"]=(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))/jp_kr_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_NTR(PAF)"]=(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))/jp_kr_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_PR(PAF)"]=(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))/jp_kr_PriceExT1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"Div_NSAF)"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_GTR(UAF)"]=jp_kr_PriceExT1/(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_NTR(UAF)"]=jp_kr_PriceExT1/(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_PR(UAF)"]=jp_kr_PriceExT1/(jp_kr_PriceExT1-(jp_kr_eventAmount*jp_kr_fx_1))
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"SCR_PAF"]=1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"AdC_GTR(CASH)"]=jp_kr_eventAmount*jp_kr_fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"AdC_NTR(CASH)"]=jp_kr_eventAmount*jp_kr_fx_1
                self.sql_cash_div_except.loc[self.sql_cash_div_except["LocalCurrency"].isin(["JP","KR"])&(self.sql_cash_div_except["EventType"].isin(["Special Cash Dividend"])),"AdC_PR(CASH)"]=jp_kr_eventAmount*jp_kr_fx_1
                
                
        except Exception as e:
            logA.error(e)
            logA.error("Special Cash Dividend Calculation has FAILED")
            pass
    def stock_dividend(self):
        try:
            logA.info("Stock Dividend Calculation is INITIATED")
            new_shares=self.sql_others.loc[self.sql_data["EventType"].isin(["Stock Dividend"]),"TermNewShares"]
            old_shares=self.sql_others.loc[self.sql_data["EventType"].isin(["Stock Dividend"]),"TermOldShares"]
            self.sql_others.loc[self.sql_data["EventType"].isin(["Stock Dividend"]),"m"]=new_shares/old_shares
            self.sql_others.loc[self.sql_others["EventType"].isin(['Stock Dividend']),"Div_PAF"]=1/(1+m)
            self.sql_others.loc[self.sql_others["EventType"].isin(['Stock Dividend']),"Div_NSAF"]=1+m
            self.sql_others.loc[self.sql_others["EventType"].isin(['Stock Dividend']),"SCR_UAF"]=1+m
#            self.sql_others
            
        except Exception as e:
            logA.error(e)
            logA.error("Stock Dividend Calculation has FAILED")
            pass
    def stock_split(self):
        try:
            logA.info("Stock Dividend Calculation is INITIATED")
            new_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Stock Split"]),"TermNewShares"]
            old_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Stock Split"]),"TermOldShares"]
            m=new_shares/old_shares
            self.sql_others.loc[self.sql_others["EventType"].isin(["Stock Split"]),"Div_PAF"]=1/m
            self.sql_others.loc[self.sql_others["EventType"].isin(["Stock Split"]),"Div_NSAF"]=m
            self.sql_others.loc[self.sql_others["EventType"].isin(["Stock Split"]),"SCR_UAF"]=m
        except Exception as e:
            logA.error(e)
            logA.error("Special Cash Dividend Calculation has FAILED")
            pass
    def rights_issue(self):
        try:
            logA.info("Rights Issue Calculation is INITIATED")
            subs_price=self.sql_others.loc[self.sql_others["EventType"].isin(["Rights Issue"]),"OfferPrice"]
            price_ex_1=self.sql_others.loc[self.sql_others["EventType"].isin(["Rights Issue"]),"PriceExT1"]
            new_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Rights Issue"]),"TermNewShares"]
            old_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Right Issue"]),"TermOldShare"]
            m=new_shares/old_shares
            self.sql_others.loc[self.sql_others["EventType"].isin(["Right Issue"]),"Div_PAF"]=(price_ex_1-(m*subs_price))/price_ex_1
            self.sql_others.loc[self.sql_others["EventType"].isin(["Right Issue"]),"Div_NSAF"]=1-m
            self.sql_others.loc[self.sql_others["EventType"].isin(["Right Issue"]),"SCR_UAF"]=(price_ex_1(1+m))/(price_ex_1+subs_price*m)
            
        except Exception as e:
            logA.error(e)
            logA.error("Rights Issue Calculation has FAILED")
            pass
    def buy_back(self):
        try:
            logA.info("Rights Issue Calculation is INITIATED")
            buybk_price=self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"OfferPrice"]
            price_ex_1=self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"PriceExT1"]
            new_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"TermNewShares"]
            old_shares=self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"TermOldShare"]
            m=new_shares/old_shares
            self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"PAF"]=(price_ex_1-(m*subs_price))/price_ex_1
            self.sql_others.loc[self.sql_others["EventType"].isin(["Buyback"]),"NSAF"]=1-m
            
        except Exception as e:
            logA.error(e)
            logA.error("Rights Issue Calculation has FAILED")
            pass

#cash_dividend()
if __name__=="__main__":
    check=rule_divisor()
    #check.duplicate_handler()
    check.cash_dividend()
#check.special_cash_dividend()
