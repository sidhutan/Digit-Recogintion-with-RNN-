# -*- coding: utf-8 -*-
"""
Created on Mon May 13 10:34:08 2019

@author: tanmay.sidhu

"""
import pandas as pd
import DB_connect

class conf:
    def __init__(self):
        """DF sql_data is a universe of Input client uploading"""
        self.sql_data=DB_connect.DB_connect('select A.UserName,B.EventName,C.CountryCode,D.Taxrate,X.* from cainput as X join causer as A on A.UserId = X.UserId join eventtypemaster as B on B.eventtypeid = x.eventtypeid join countrymaster as C on C.CountryId = x.countryid join countrytaxrate_wht as D on D.CountryID = X.CountryId')
        self.sql_preference_data=DB_connect.DB_connect("select * from preferences")
        self.jp_user_choice=self.sql_preference_data["Japan"].values[0]
        self.kr_user_choice=self.sql_preference_data["Korea"].values[0]
#        self.sql_data.to_csv("sql_data.csv")
        if self.jp_user_choice=='ex' and self.kr_user_choice=='ex':
            self.sql_data["CombinedEx"]=self.sql_data["Identifier"]+"_"+self.sql_data["ExDate"]
            self.d_final_combinedEx=self.sql_data[self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.nd_final_combinedEx=self.sql_data[~self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.d_final_combinedPay = None
            self.nd_final_combinedPay = None
        elif self.jp_user_choice=='ex' and self.kr_user_choice=='pay':
            self.sql_data.loc[~(self.sql_data["CountryCode"]=="KR"),"CombinedEx"]=self.sql_data.loc[~(self.sql_data["CountryCode"]=="KR"),"Identifier"]+"_"+self.sql_data.loc[~(self.sql_data["CountryCode"]=="KR"),"ExDate"]
            self.sql_data.loc[(self.sql_data["CountryCode"]=="KR"),"CombinedPay"]=self.sql_data.loc[(self.sql_data["CountryCode"]=="KR"),"Identifier"]+"_"+self.sql_data.loc[(self.sql_data["CountryCode"]=="KR"),"PayDate"]
            self.d_final_combinedEx=self.sql_data[self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.d_final_combinedPay=self.sql_data[self.sql_data["CombinedPay"].duplicated(keep=False)]
            self.nd_final_combinedEx=self.sql_data[~self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.nd_final_combinedPay=self.sql_data[~self.sql_data["CombinedPay"].duplicated(keep=False)]
        elif self.jp_user_choice=='pay' and self.kr_user_choice=='ex':
            self.sql_data.loc[(self.sql_data["CountryCode"]=="JP"),"CombinedPay"]=self.sql_data.loc[(self.sql_data["CountryCode"]=="JP"),"Identifier"]+"_"+self.sql_data.loc[(self.sql_data["CountryCode"]=="JP"),"ExDate"]
            self.sql_data.loc[~(self.sql_data["CountryCode"]=="JP"),"CombinedEx"]=self.sql_data.loc[~(self.sql_data["CountryCode"]=="JP"),"Identifier"]+"_"+self.sql_data.loc[~(self.sql_data["CountryCode"]=="JP"),"PayDate"]
            self.d_final_combinedEx=self.sql_data[self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.d_final_combinedPay=self.sql_data[self.sql_data["CombinedPay"].duplicated(keep=False)]
            self.nd_final_combinedEx=self.sql_data[~self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.nd_final_combinedPay=self.sql_data[~self.sql_data["CombinedEx"].duplicated(keep=False)]
        elif self.kr_user_choice=="pay" and self.jp_user_choice=="pay":
            self.sql_data.loc[(self.sql_data["CountryCode"].isin(["KR","JP"])),"CombinedPay"]=self.sql_data.loc[(self.sql_data["CountryCode"].isin(["KR","JP"])),"Identifier"]+"_"+self.sql_data.loc[(self.sql_data["CountryCode"].isin(["KR","JP"])),"PayDate"]
            self.sql_data.loc[~(self.sql_data["CountryCode"].isin(["KR","JP"])),"CombinedEx"]=self.sql_data.loc[~(self.sql_data["CountryCode"].isin(["KR","JP"])),"Identifier"]+"_"+self.sql_data.loc[~(self.sql_data["CountryCode"].isin(["KR","JP"])),"ExDate"]
            self.d_final_combinedPay=self.sql_data[self.sql_data["CombinedPay"].duplicated(keep=False)]
            self.d_final_combinedEx=self.sql_data[self.sql_data["CombinedEx"].duplicated(keep=False)]
            self.nd_final_combinedPay=self.sql_data[~self.sql_data["CombinedPay"].duplicated(keep=False)]
            self.nd_final_combinedEx=self.sql_data[~self.sql_data["CombinedEx"].duplicated(keep=False)]
        #logged in user details_Need to be mentioned in the filter
#    def segreation(self,DF1,*DF2):
    
        """DF sql_cash_div  has all the cash and special cash entries"""
        """DF sql_cash_div_except has only exceptions""" 
        """DF sql_cash_div_ex_excep has excluded all the exceptions"""
        self.sql_cash_div=self.nd_final_combinedEx[self.nd_final_combinedEx["EventName"].isin(["Special Cash Dividend","Cash Dividend"])]
        #using country code instead of currency
        self.sql_cash_div_except=self.sql_cash_div[self.sql_cash_div["CountryCode"].isin(["AU","JP","KR"])]
#        self.sql_cash_div_except.to_csv("cash_div_except.csv")
        self.sql_cash_div_ex_excep=self.sql_cash_div[~self.sql_cash_div["CountryCode"].isin(["AU","JP","KR"])]
        if self.kr_user_choice=="pay" or self.jp_user_choice=='pay':
            self.sql_cash_div_pay=self.nd_final_combinedPay[self.nd_final_combinedPay["EventName"].isin(["Special Cash Dividend","Cash Dividend"])]
        """Others"""
        """DF sql_others other CA other than cash Dividend and Special cash"""
        """DF sql_other_except has only Exceptions"""
        """DF sql_other_ex_except has excluted all Exceptions"""
        self.sql_others=self.nd_final_combinedEx[~self.nd_final_combinedEx["EventName"].isin(["Special Cash Dividend","Cash Dividend"])]
        #using country code instead of currency
        self.sql_other_except=self.sql_others[self.sql_others["CountryCode"].isin(["AU","JP","KR"])]
        self.sql_other_ex_except=self.sql_others[~self.sql_others["CountryCode"].isin(["AU","JP","KR"])]
#        PriceEx_1=self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"PriceEx_1"]
#        FxEx_1=self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"FxEx_1"]
#        DividendAmount=self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"EventAmount"]
#        self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"PR"]=(PriceEx_1 - DividendAmount * FxEx_1)/PriceEx_1
#        self.sql_cash_div_ex_excep.to_csv("test.csv")                            
#            self.sql_cash_div_ex_excep.loc[~self.sql_cash_div_ex_excep["EventName"].isin(["Cash Dividend"]),"PR"]=(PriceEx_1 - DividendAmount * FxEx_1)/PriceEx_1
#        self.sql_cash_div_ex_excep["GTR(PAF)"]=(self.sql_cash_div_ex_excep["PriceEx_1"] - ( self.sql_cash_div_ex_excep["EventAmount"] * self.sql_cash_div_ex_excep["FxEx_1"] * (1-self.sql_cash_div_ex_excep["Taxrate"]/100)))/self.sql_cash_div_ex_excep["PriceEx_1"]
#        self.sql_cash_div_ex_excep["NTR(PAF)"]=(self.sql_cash_div_ex_excep["PriceEx_1"] - ( self.sql_cash_div_ex_excep["EventAmount"] * self.sql_cash_div_ex_excep["FxEx_1"] * (1-self.sql_cash_div_ex_excep["Taxrate"]/100)))/self.sql_cash_div_ex_excep["PriceEx_1"]
#        self.sql_cash_div_ex_excep["PR(PAF)"]=(self.sql_cash_div_ex_excep["PriceEx_1"] - ( self.sql_cash_div_ex_excep["EventAmount"] * self.sql_cash_div_ex_excep["FxEx_1"] * (1-self.sql_cash_div_ex_excep["Taxrate"]/100)))/self.sql_cash_div_ex_excep["PriceEx_1"]
#sql_cash_div["PAF"]=(sql_cash_div["PriceEx_1"] - ( sql_cash_div["EventAmount"] * sql_cash_div["FxEx_1"] * (1-sql_cash_div["Taxrate"]/100)))/sql_cash_div["PriceEx_1"]

#sql_cash["NAF"]=1

a=conf()
sql_data=a.sql_data
sql_data["ExDate"]=pd.to_datetime(sql_data["ExDate"])

print(type(sql_data["ExDate"].values[0]))
#print(a.sql_others)
#print(a.sql_s_cash_div)
