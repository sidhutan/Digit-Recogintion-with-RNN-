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
#        self.df.loc[(self.df[col1].isin([i for i in col1_event]))&(self.sql_cash_div_except["CountryCode"].isin(["AU"])),"PriceEx_1"]
        
    def fn_cash_ex_excep(self,Cash_Dividend,Eventvalue):
        """Just a func to have a clean code structure"""
        return self.sql_cash_div_ex_excep.loc[self.sql_cash_div_ex_excep["EventName"].isin([Cash_Dividend]),Eventvalue]

    def fn_s_cash_ex_excep(self,Cash_Dividend,Eventvalue):
         """Just a func to have a clean code structure"""
         return self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin([Cash_Dividend]),Eventvalue]
    def fn_s_cash_excep(self,eventtype,Eventvalue):
         return self.sql_cash_div_except.loc[self.sql_cash_div_except["EventName"].isin([eventtype]),Eventvalue]
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
                if len(i.loc[i["EventName"].isin(["Cash Dividend"])])>0:
                    counter=counter+1
                    if i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"]).isin(["JP","KR"]),"EventName"].count() > 0:
                        jp_kr_priceEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PriceEx_1"]
                        jp_kr_FxEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"FxEx_1"]
                        jp_kr_Taxrate=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"Taxrate"]
                        jp_kr_event_amount=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"EventAmount"].sum()
                        GTR(PAF)=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"GTR(PAF)"]=(jp_kr_priceEx_1-(jp_kr_event_amount)*(jp_kr_FxEx_1))/jp_kr_priceEx_1
                        NTR(PAF)=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NTR(PAF)"]=(jp_kr_priceEx_1-(jp_kr_event_amount)*(jp_kr_FxEx_1)*(jp_kr_Taxrate/100))/jp_kr_priceEx_1
                        PR(PAF)=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PR(PAF)"]=1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NSAF"]=1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"GTR_PriceEx_1"]=jp_kr_priceEx_1*GTR(PAF)
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NTR_PriceEx_1"]=jp_kr_priceEx_1*NTR(PAF)
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PR_PriceEx_1"]=jp_kr_priceEx_1*PR(PAF)
                    if i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"]).isin(["JP","KR"]),"EventName"].count() > 0:
                        if counter>0:
                            jp_kr_priceEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PriceEx_1"]
                        else:
                            jp_kr_priceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PriceEx_1"]
                        jp_kr_FxEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"FxEx_1"]
                        jp_kr_Taxrate=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"Taxrate"]
                        jp_kr_event_amount=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"EventAmount"].sum()
                        GTR(PAF)=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"GTR(PAF)"]=(jp_kr_priceEx_1-(jp_kr_event_amount)*(jp_kr_FxEx_1))/jp_kr_priceEx_1
                        NTR(PAF)=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NTR(PAF)"]=(jp_kr_priceEx_1-(jp_kr_event_amount)*(jp_kr_FxEx_1))/jp_kr_priceEx_1
                        PR(PAF)=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PR(PAF)"]=1
                        i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NSAF"]=1
                        i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"GTR_PriceEx_1"]=jp_kr_priceEx_1*GTR(PAF)
                        i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"NTR_PriceEx_1"]=jp_kr_priceEx_1*NTR(PAF)
                        i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["JP","KR"])),"PR_PriceEx_1"]=jp_kr_priceEx_1*PR(PAF)
                        
        if df1==self.d_final_combinedEx:
            counter=0
            df_ex=self.d_final_combinedEx.groupby('CombinedEx')
            for i in list(df_ex.groups):
                grp_name=i
                i=df_ex.get_group(i)
                
                if len(i.loc[i["EventName"].isin(['Cash Dividend'])]) > 0:
                    counter=counter+1
                    i.loc[i["EventName"].isin(['Cash Dividend']),"Counter"]=counter
                    if i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventName"].count() > 0:
                        au_PriceEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"PriceEx_1"]
                        au_Fx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"FxEx_1"]
                        au_Taxrate=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"Taxrate"]
                        Frank_Percent=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"Franking_percent"]
                        Income_dis_percent=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"Income_percent"]
                        Ef_Taxrate=au_Taxrate*(1 - Frank_Percent - Income_dis_percent)
                        au_EventAmount=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventAmount"].sum()
                        Ef_EventAmount=au_EventAmount(1-Ef_Taxrate)
                        i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Cash Dividend"])),"GTR(PAF)"]=(au_PriceEx_1-(au_EventAmount*au_Fx_1))/au_PriceEx_1
                        i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Cash Dividend"])),"NTR(PAF)"]=(au_PriceEx_1-(Ef_EventAmount*au_Fx_1))/au_PriceEx_1
                        aus_adj_price_NTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Cash Dividend"])),"NTR(PAF)"].values[0]*au_PriceEx_1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"NTR_PriceEx_1"]=aus_adj_price_NTR
                        aus_adj_price_GTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Cash Dividend"])),"GTR(PAF)"].values[0]*au_PriceEx_1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"GTR_PriceEx_1"]=aus_adj_price_GTR
                        i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Cash Dividend"])),"PR(PAF)"]=1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"PR_PriceEx_1"]=au_PriceEx_1*1
                        i.loc[(i["EventName"].isin(["Cash Dividend"]))&(i["CountryCode"].isin(["AU"])),"NSAF"]=1
                    if len(i.loc[(i["EventName"].isin('Cash Dividend'))&(~i['CountryCode'].isin(['AU']))]) > 0:
                        PriceEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(~i["CountryCode"].isin(["AU"])),"PriceEx_1"]
                        EventAmount=i.loc[(i['EventName'].isin('Cash Dividend'))&(~i["CountryCode"].isin(['AU'])),"EventAmount"].sum()
                        FxEx_1=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(~i["CountryCode"].isin(["AU"])),"FxEx_1"]
                        Taxrate=i.loc[(i["EventName"].isin(["Cash Dividend"]))&(~i["CountryCode"].isin(["AU"])),"Taxrate"]
                        i.loc[i["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"]=(PriceEx_1 - ( EventAmount * FxEx_1 * (Taxrate/100) )) / PriceEx_1
                        adj_price_NTR=i.loc[i["EventName"].isin(["Cash Dividend"]),"NTR(PAF)"].values[0]*PriceEx_1
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"NTR_PriceEx_1"]=adj_price_NTR
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"GTR(PAF)"]=(PriceEx_1 - ( EventAmount * FxEx_1) / PriceEx_1)
                        adj_price_GTR=i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"GTR(PAF)"].values[0]*PriceEx_1
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"GTR_PriceEx_1"]=adj_price_GTR
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"PR(PAF)"]=1
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"PR_PriceEx_1"]=PriceEx_1*1
                        i.loc[(i['EventName'].isin(['Cash Dividend']))&(~i['CountryCode'].isin(['AU'])),"NSAF"]=1
                        
                    if len(i.loc[(i['EventName'].isin(['Special Dividend']))]) > 0:
                        counter=counter+1
                        i.loc[(i['EventName'].isin(['Special Dividend'])),"Counter"]=counter
                        if i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventName"].count() > 0:
                            GTR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"GTR_PriceEx_1"].values[0]    
                            NTR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"NTR_PriceEx_1"].values[0]
                            PR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"PR_PriceEx_1"].values[0]    
    #                        eventamount=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventAmount"].values[0]
                            au_Fx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"FxEx_1"]
                            au_EventAmount=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventAmount"].sum()
                            i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"GTR(PAF)"]=(GTR_PriceEx_1-(au_EventAmount*au_Fx_1))/GTR_PriceEx_1
                            i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"NTR(PAF)"]=(NTR_PriceEx_1-(au_EventAmount*au_Fx_1))/NTR_PriceEx_1
                            aus_adj_price_NTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"NTR(PAF)"].values[0]*NTR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"NTR_PriceEx_1"]=aus_adj_price_NTR
                            aus_adj_price_GTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"GTR(PAF)"].values[0]*GTR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"GTR_PriceEx_1"]=aus_adj_price_GTR
                            i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"PR(PAF)"]=(au_PriceEx_1-(au_EventAmount*au_Fx_1))/GTR_PriceEx_1
                            aus_adj_price_PR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"PR(PAF)"].values[0]*PR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"PR_PriceEx_1"]=aus_adj_price_PR
                        if len(i.loc[(i["EventName"].isin('Special Dividend'))&(~i['CountryCode'].isin(['AU']))]) > 0:
                            GTR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(~i["CountryCode"].isin(["AU"])),"GTR_PriceEx_1"].values[0]    
                            NTR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(~i["CountryCode"].isin(["AU"])),"NTR_PriceEx_1"].values[0]
                            PR_PriceEx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(~i["CountryCode"].isin(["AU"])),"PR_PriceEx_1"].values[0]    
    #                        eventamount=i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"EventAmount"].values[0]
                            au_Fx_1=i.loc[(i["EventName"].isin(["Special Dividend"]))&(~i["CountryCode"].isin(["AU"])),"FxEx_1"]
                            au_EventAmount=i.loc[(i["EventName"].isin(["Special Dividend"]))&(~i["CountryCode"].isin(["AU"])),"EventAmount"].sum()
                            i.loc[(~i["CountryCode"].isin(["AU"]))&(i["EventName"].isin(["Special Dividend"])),"GTR(PAF)"]=(GTR_PriceEx_1-(au_EventAmount*au_Fx_1))/GTR_PriceEx_1
                            i.loc[(~i["CountryCode"].isin(["AU"]))&(i["EventName"].isin(["Special Dividend"])),"NTR(PAF)"]=(NTR_PriceEx_1-(au_EventAmount*au_Fx_1))/NTR_PriceEx_1
                            aus_adj_price_NTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"NTR(PAF)"].values[0]*NTR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"NTR_PriceEx_1"]=aus_adj_price_NTR
                            aus_adj_price_GTR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"GTR(PAF)"].values[0]*GTR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"GTR_PriceEx_1"]=aus_adj_price_GTR
                            i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"PR(PAF)"]=(au_PriceEx_1-(au_EventAmount*au_Fx_1))/GTR_PriceEx_1
                            aus_adj_price_PR=i.loc[i["CountryCode"].isin(["AU"])&(i["EventName"].isin(["Special Dividend"])),"PR(PAF)"].values[0]*PR_PriceEx_1
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"PR_PriceEx_1"]=aus_adj_price_PR
                            i.loc[(i["EventName"].isin(["Special Dividend"]))&(i["CountryCode"].isin(["AU"])),"NSAF"]=1
                    if len(i.loc[i["EventName"].isin(["Stock Dividend"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventName"].isin(["Stock Dividend"]),"Counter"]=counter
                        i.loc[i["EventName"].isin(['Cash Dividend'])]
                        TermNewShares=i.loc[(i["EventName"].isin(["Stock Dividend"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventName"].isin(["Stock Dividend"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        PAF=1/1+m
                        GTR_PriceEx_1=i["GTR_PriceEx_1"]*PAF
                        NTR_PriceEx_1=i["NTR_PriceEx_1"]*PAF
                        PR_PriceEx_1=i["PR_PriceEx_1"]*PAF
                        i.loc[i["EventName"].isin(["Stock Dividend"]),"GTR_PriceEx_1"]=GTR_PriceEx_1
                        i.loc[i["EventName"].isin(["Stock Dividend"]),"NTR_PriceEx_1"]=NTR_PriceEx_1
                        i.loc[i["EventName"].isin(["Stock Dividend"]),"PR_PriceEx_1"]=PR_PriceEx_1
                        i.loc[i["EventName"].isin(["Stock Dividend"]),"NSAF"]=1-m
                    if len(i.loc[i["EventName"].isin(["Stock Split"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventName"].isin(["Stock Split"]),"Counter"]=counter
                        TermNewShares=i.loc[(i["EventName"].isin(["Stock Split"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventName"].isin(["Stock Split"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        i.loc[i["EventName"].isin(["Stock Split"]),"NSAF"]=m
                        PAF=1/m
                        GTR_PriceEx_1=i["GTR_PriceEx_1"]*PAF
                        NTR_PriceEx_1=i["NTR_PriceEx_1"]*PAF
                        PR_PriceEx_1=i["PR_PriceEx_1"]*PAF
                    if len(i.loc[i["EventName"].isin(["Rights Issue"])]) > 0:
                        counter=counter+1
                        i.loc[i["EventName"].isin(["Rights Issue"]),"Counter"]=counter
                        GTR_PriceEx_1=i["GTR_PriceEx_1"]
                        NTR_PriceEx_1=i["NTR_PriceEx_1"]
                        PR_PriceEx_1=i["PR_PriceEx_1"]
                        TermNewShares=i.loc[(i["EventName"].isin(["Rights Issue"],"TermNewShares"))].values[0]
                        TermoldShares=i.loc[(i["EventName"].isin(["Rights Issue"],"TermOldShares"))].values[0]
                        m=TermNewShares/TermoldShares
                        offerprice=i.loc[(i["EventName"].isin(["Rights Issue"])),"OfferPrice"].values[0]
                        PAF_G=(GTR_PriceEx_1+(m*offerprice))/GTR_PriceEx_1*(1-m)
                        PAF_N=(NTR_PriceEx_1+(m*offerprice))/NTR_PriceEx_1*(1-m)
                        PAF_P=(PR_PriceEx_1+(m*offerprice))/PR_PriceEx_1*(1-m)
                        GTR_PriceEx_1=i["GTR_PriceEx_1"]*PAF_G
                        NTR_PriceEx_1=i["NTR_PriceEx_1"]*PAF_N
                        PR_PriceEx_1=i["PR_PriceEx_1"]*PAF_P
                        i.loc[i["EventName"].isin(["Rights Issue"]),"NSAF"]=1+m
                    
                            #no spin off cases
    #                if len(i["EventName"].isin(["Spin off"])) >0:
    #                    GTR_PriceEx_1=i["GTR_PriceEx_1"]
    #                    NTR_PriceEx_1=i["NTR_PriceEx_1"]
    #                    PR_PriceEx_1=i["PR_PriceEx_1"]
    #                    TermsNewShares=i.loc[(i["EventName"].isin(["Spin off"],"TermNewShares"))].values[0]
    #                    TermsOldShares=i.loc[(i["EventName"].isin(["Spin off"],"TermOldShares"))].values[0]
                closeprice=i.loc[i["Counter"]==max(i["Counter"]),"PriceEx_1"].values[0]
                adj_GTR_price=i.loc[i["Counter"]==max(i["Counter"]),"GTR_PriceEx_1"].values[0]
                adj_NTR_price=i.loc[i["Counter"]==max(i["Counter"]),"NTR_PriceEx_1"].values[0]
                adj_PR_price=i.loc[i["Counter"]==max(i["Counter"]),"PR_PriceEx_1"].values[0]
    #            types = {"TYPE":["PR","GTR","NTR"]}
                df = pd.DataFrame(columns=["PR_PAF","GTR_PAF","NTR_PAF","PR_SAF","GTR_SAF","NTR_SAF"])
                if closeprice==adj_GTR_price:
                    df.loc[grp_name,"GTR_PAF"] = 1
    #                df.loc[df[i]=="GTR","PAF"] = 1
                else:
                    df.loc[grp_name,"GTR_PAF"]=adj_GTR_price/closeprice
#                    df.loc[grp_name,"SAF"]=
    #                df.loc[df["TYPE"]=="GTR","PAF"] = adj_GTR_price/closeprice
                if closeprice== adj_NTR_price:
                    df.loc[grp_name,"NTR_PAF"]=1
    #                df.loc[df["TYPE"]=="NTR","PAF"] = 1
                else:
                    df.loc[grp_name,"NTR_PAF"]=adj_NTR_price/closeprice
    #                df.loc[df["TYPE"]=="NTR","PAF"] = adj_NTR_price/closeprice
                if closeprice== adj_NTR_price:
                    df.loc[grp_name,"PR_PAF"]=1
    #                df.loc[df["TYPE"]=="PR","PAF"] = 1
                else:
                    df.loc[grp_name,"PR_PAF"]=adj_PR_price/closeprice
    #                df.loc[df["TYPE"]=="PR","PAF"] = adj_PR_price/closeprice                   
                                            
                                        
                            
                        
                    
                                   
                        
                    
                        
                        
                        
                                       
                        
                        
                        
    
        

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
            #handling depends 
            if self.sql_cash_div_except.loc[self.sql_cash_div_except['EventName'].isin(['AU']),'EventName'].count()>0:
                s_eventAmount=
                
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
