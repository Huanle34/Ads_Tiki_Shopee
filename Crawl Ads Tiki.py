from ctypes import pointer
from pickle import TRUE
from this import d
from tkinter import N
from turtle import st
from selenium import webdriver
import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
import numpy as np
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from random import randint
from time import sleep
import os
import shutil
import re
from datetime import date
from selenium.webdriver.chrome.options import Options
options = Options()

options.add_argument("--incognito")
# options.add_argument("--headless")
driver = webdriver.Chrome("D:/File Setup/Chrome Webdriver/chromedriver.exe",chrome_options=options)


def open_web():
    driver.delete_all_cookies()
    driver.maximize_window()
    driver.implicitly_wait(30)
    driver.get("https://sellercenter.tiki.vn/new#/native-ads/")
    
    #Đăng nhập
    import commontikianhsao
    driver.find_element(By.XPATH,"//button[@class = 'ant-btn ant-btn-primary ant-btn-lg ant-btn-block loginBtn___3pXbP']").click()

    username = driver.find_element(By.XPATH,"//input[@name = 'email']")
    username.clear()
    username.send_keys(commontikianhsao.email)
    password = driver.find_element(By.XPATH,"//input[@name = 'password']")
    password.clear()
    password.send_keys(commontikianhsao.password)

    driver.find_element(By.XPATH,"//button[@type='submit']").click()
    sleep(5)

#Automation click on button "Download report" in seller center
def click_download():
    driver.find_elements(By.XPATH,"//input[@class = 'tka-checkbox-input']")[0].click()
    driver.find_elements(By.XPATH,"//button[@class = 'tka-btn tka-btn-default']")[0].click()

# Move file download from folder Download to Folder month
def rename_remove(file_name, path_old, path_new):
    path1 = path_old + file_name + ".xlsx"
    path2 = path_new + file_name + ".xlsx"
    shutil.move(path1, path2)

#Find name, budget, link campaing in sellercenter
def find_link_campaign():
    driver.get("https://sellercenter.tiki.vn/new#/native-ads/")
    campaign_names = driver.find_elements(By.XPATH,"//td[@class = 'tka-table-cell tka-table-cell-fix-left tka-table-cell-fix-left-last']//a")
    campaign_hrefs = driver.find_elements(By.XPATH,"//td[@class = 'tka-table-cell tka-table-cell-fix-left tka-table-cell-fix-left-last']//a")
    campaign_daily_budget= driver.find_elements(By.XPATH,"//td[@class = 'tka-table-cell']/div[@class =  'd-flex']/div[@class =  'd-flex']")
    print(len(campaign_names))
    link_campaign = []
    for name, href, budget in zip(campaign_names, campaign_hrefs, campaign_daily_budget):
        a = {}
        a = {
            'name':name.text,
            'link': str(href.get_attribute("href")),
            'budget': budget.text
        }
        link_campaign.append(a)
    pd.DataFrame(link_campaign).to_excel("C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Tiki/Budget_campaign_daily/Budget_campaign.xlsx")
    return link_campaign


# Fill day report in sellercenter
def day_report(startday, endday):
    try:
        pointer = driver.find_element(By.XPATH,"//input[@placeholder='Ngày bắt đầu']")
        pointer.send_keys(u'\ue009' +'A'+ u'\ue003')
        pointer.send_keys(startday)
        pointer.send_keys(u'\ue007') #ENTER
        pointer = driver.find_element(By.XPATH,"//input[@placeholder='Ngày kết thúc']")
        pointer.send_keys(u'\ue009' +'A'+ u'\ue003')
        pointer.send_keys(endday)
        pointer.send_keys(u'\ue007') #ENTER
    except Exception as e: print("Error")


################################################################################  
#Load file from folder data by month, merger -> file data by month
def download_file_Tiki(startday,endday):
    df = pd.DataFrame(find_link_campaign())
    day_report(startday,endday)
    names_campaign = list(df["name"])
    links_campaign = list(df["link"])
    total_sp  = pd.DataFrame()
    total_kw = pd.DataFrame()
    for name, link in zip(names_campaign,links_campaign):
        print(link)
        url = link.encode('ascii', 'ignore').decode('unicode_escape')
        print(url)
        driver.get(url)
        sleep(2)
        click_download()
        sleep(25)

        path_new =  "C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Tiki/{period}/".format(period =startday.replace("/","_")+"-"+endday.replace("/","_"))
        # create_folder
        df_sp = pd.DataFrame()
        df_kw = pd.DataFrame()
        os.makedirs(path_new,exist_ok=True)
        rename_remove(name,"C:/Users/ADMIN/Downloads/",path_new)

        df_sp = pd.read_excel(path_new +name+".xlsx", sheet_name = "Sản phẩm",skiprows = 2)
        df_sp = df_sp[df_sp["Trạng thái"]=="Chạy"]
        df_sp["Time"] = path_new[-22:-1]
        df_sp["Campaign"] = name
        total_sp = pd.concat([total_sp,df_sp], axis=0, ignore_index = True)

        df_kw = pd.read_excel(path_new+name+".xlsx", sheet_name = "Mục tiêu", skiprows = 2)
        df_kw = df_kw[df_kw["Lượt nhấp chuột"]>0]
        df_kw["Time"] = path_new[-22:-1]
        df_kw["Campaign"]= name
        total_kw = pd.concat([total_kw,df_kw], axis=0, ignore_index = True)

    with pd.ExcelWriter("C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Tiki/{period}.xlsx".format(period = startday.replace("/","_")+"-"+endday.replace("/","_"))) as writer:
        total_sp.to_excel(writer, sheet_name='Sản phẩm')
        total_kw.to_excel(writer, sheet_name='Mục tiêu')
    
################################################################################
# Load file by month -> rename -> caculation -> return 2 dataframe (data_nhom_tiki, data_tiki_ads)
def read_file_Tiki(path):
    
    global data_tiki_ads
    global data_nhom_tiki
    global data_keyword_tiki

    dir_list = os.listdir(path)
    for file_name in dir_list:
        if file_name.endswith(".xlsx"):
            data_keyword_tiki_lc = pd.read_excel(path + file_name, sheet_name = "Mục tiêu")
            data_keyword_tiki = pd.concat([data_keyword_tiki,data_keyword_tiki_lc], axis=0, ignore_index = True)
            data_tiki_ads_lc = pd.read_excel(path + file_name, sheet_name = "Sản phẩm")
            data_tiki_ads = pd.concat([data_tiki_ads,data_tiki_ads_lc], axis=0, ignore_index = True)
    data_tiki_ads.drop('Unnamed: 0',axis = 1, inplace = True)
    cols = ['Trang_thai','Nhom_quang_cao','Link_san_pham','SKU',"San_pham",'Luot_hien_thi','Luot_nhap_chuot','Chi_phi_quang_cao','CTR','Avg. CPC','Cho_vao_gio_hang','So_san_pham_ban_ra','GMV','ACoS',"CR",'Time','name']
    data_tiki_ads.columns = cols
    try:
        data_tiki_ads["Chi_phi_per_san_pham"] = data_tiki_ads["Chi_phi_quang_cao"]/data_tiki_ads["So_san_pham_ban_ra"]
        data_tiki_ads = data_tiki_ads.replace([np.inf, -np.inf,np.nan], 0)
        data_tiki_ads = data_tiki_ads[data_tiki_ads["Luot_nhap_chuot"]>0]
        df  =pd.read_excel('C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Tiki/Budget_campaign_daily/Budget_campaign.xlsx',usecols = ["name","budget"])
        df['budget'] = df['budget'].str.replace(" đ",'', regex=True).str.replace('.','', regex = True)
        df['budget']= df['budget'].astype(int)
        data_tiki_ads = pd.merge(data_tiki_ads,df, on = "name", how = "inner")
        data_nhom_tiki= data_tiki_ads.groupby(["Time","Nhom_quang_cao"], as_index=False).sum()

        data_nhom_tiki.drop('SKU', axis=1, inplace=True)
        data_nhom_tiki["CTR"] = data_nhom_tiki["Luot_nhap_chuot"]/data_nhom_tiki["Luot_hien_thi"]
        data_nhom_tiki["Avg. CPC"] = data_nhom_tiki["Chi_phi_quang_cao"]/data_nhom_tiki["Luot_nhap_chuot"]
        data_nhom_tiki["ACoS"]= data_nhom_tiki["Chi_phi_quang_cao"]/data_nhom_tiki["GMV"]
        data_nhom_tiki["CR"] = data_nhom_tiki["So_san_pham_ban_ra"]/data_nhom_tiki["Luot_nhap_chuot"]
        data_nhom_tiki["Chi_phi_per_san_pham"] = data_nhom_tiki["Chi_phi_quang_cao"]/data_nhom_tiki["So_san_pham_ban_ra"]
        data_nhom_tiki=data_nhom_tiki.replace([np.inf, -np.inf,np.nan], 0)

        data_tiki_ads = data_tiki_ads.sort_values(by = ["Time","Chi_phi_quang_cao"],ascending=False).reset_index(drop = True)
        data_nhom_tiki = data_nhom_tiki.sort_values(by = ["Time","Chi_phi_quang_cao"],ascending=False).reset_index(drop = True)

    except Exception as e: print("Error_groupby_tiki")

    try:
        data_keyword_tiki = data_keyword_tiki[data_keyword_tiki["GMV"]>0]
    except Exception as e: print("Error_Keywork_Tiki")
################################################################################
# Load file -> rename file by day -> export file total (final)
def match_file_shopee(path):
    #Đổi tên file -> ngày tháng xuất file
    dir_list = os.listdir(path)
    csv_list = []
    for file in dir_list:
        if file.endswith(".csv"):
            os.rename(path+ file,path + file[-25:])
            csv_list.append(file[-25:])
    #Concat nhiều file _> 1 file + insert columns time
    total = pd.DataFrame()
    for file_name in csv_list:
        df = pd.read_csv(path + file_name, skiprows=6)
        df["Time"] = file_name[:-4]
        total = pd.concat([total,df], axis=0, ignore_index = True)
    total.to_excel(path + "Total/total.xlsx")
    
################################################################################

# Read file Shopee
    # Load file final, rename cols, caculation, merger file Group -> return 2 dataframe (data_nhom_shopee, data_shopee_ads)
def read_file_Shopee(file_name):

    global data_shopee_ads
    global data_nhom_sp
    global data_keyword_shopee

    match_file_shopee("C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Shopee/")

    data_shopee_ads = pd.read_excel(file_name)
    data_keyword_shopee  = data_shopee_ads
    try:
        cols =                             ["Trang_thai","Nhom_quang_cao","Link_san_pham","SKU","San_pham","Luot_hien_thi","Luot_nhap_chuot","Chi_phi_quang_cao","CTR","Avg. CPC","Cho_vao_gio_hang","So_san_pham_ban_ra","GMV","ACoS","CR","Time"]
        data_shopee_ads = data_shopee_ads[["Trạng thái","Loại quảng cáo","Tên Quảng cáo","Mã sản phẩm","Tên Quảng cáo","Số lượt xem","Số lượt click","Chi phí","Tỷ Lệ Click","Chi phí cho mỗi lượt chuyển đổi","Sản phẩm đã bán","Sản phẩm đã bán","GMV","Doanh Thu/Chi Phí","CIR","Time"]]
        data_shopee_ads.columns = cols
        data_shopee_ads = data_shopee_ads[data_shopee_ads["Luot_nhap_chuot"]>0]
        data_shopee_ads["CTR"] = data_shopee_ads["Luot_nhap_chuot"]/data_shopee_ads["Luot_hien_thi"]
        data_shopee_ads["Avg. CPC"]= data_shopee_ads["Chi_phi_quang_cao"]/data_shopee_ads["Luot_nhap_chuot"]
        data_shopee_ads["ACoS"]= data_shopee_ads["Chi_phi_quang_cao"]/data_shopee_ads["GMV"]
        data_shopee_ads["CR"] = data_shopee_ads["So_san_pham_ban_ra"]/data_shopee_ads["Luot_nhap_chuot"]
        data_shopee_ads["Chi_phi_per_san_pham"] = data_shopee_ads["Chi_phi_quang_cao"]/data_shopee_ads["So_san_pham_ban_ra"]
        data_shopee_ads=data_shopee_ads.replace([np.inf, -np.inf,np.nan], 0)
        

    except Exception as e: print("Error_read_file_shopee")

    try:
        data_nhom_sp = data_shopee_ads.groupby(["Time","San_pham"], as_index=False).sum()#.swaplevel(0)
        data_nhom_sp = pd.DataFrame(data_nhom_sp)
        data_nhom_sp.drop('SKU', axis = 1, inplace = True)
        data_nhom_sp["CTR"] = data_nhom_sp["Luot_nhap_chuot"]/data_nhom_sp["Luot_hien_thi"]
        data_nhom_sp["Avg. CPC"] = data_nhom_sp["Chi_phi_quang_cao"]/data_nhom_sp["Luot_nhap_chuot"]
        data_nhom_sp["ACoS"]= data_nhom_sp["Chi_phi_quang_cao"]/data_nhom_sp["GMV"]
        data_nhom_sp["CR"] = data_nhom_sp["So_san_pham_ban_ra"]/data_nhom_sp["Luot_nhap_chuot"]
        data_nhom_sp["Chi_phi_per_san_pham"] = data_nhom_sp["Chi_phi_quang_cao"]/data_nhom_sp["So_san_pham_ban_ra"]
        merge1  = pd.read_excel("Merge Shopee.xlsx")
        data_nhom_sp = pd.merge(data_nhom_sp,
                                merge1,
                                on = "San_pham",
                                how = 'inner'
                                )
        data_nhom_sp = data_nhom_sp.replace([np.inf, -np.inf,np.nan], 0)
    except Exception as e: print("Error_groupby_shopee")

    try:
        data_keyword_shopee = data_keyword_shopee[["Tên Quảng cáo","Trạng thái","Mã sản phẩm","Loại quảng cáo","Dữ liệu cấp độ từ khóa/vị trí hiển thị",
        "Số lượt xem","Số lượt click","Tỷ Lệ Click","Lượt chuyển đổi","Tỷ lệ chuyển đổi","Chi phí cho mỗi lượt chuyển đổi","Sản phẩm đã bán",
        "GMV","Chi phí","Doanh Thu/Chi Phí","CIR","Time"]]
        data_keyword_shopee = data_keyword_shopee[data_keyword_shopee["GMV"]>0]
        data_keyword_shopee["Tỷ Lệ Click"] = data_keyword_shopee["Tỷ Lệ Click"].str.replace("%",'', regex=True)
        data_keyword_shopee["Tỷ Lệ Click"] = data_keyword_shopee["Tỷ Lệ Click"].astype(float)
        data_keyword_shopee = data_keyword_shopee[data_keyword_shopee["Tỷ Lệ Click"]>5.00].reset_index(drop = True)
    except Exception as e : print("Error_Keyword_Shopee")

    data_shopee_ads = data_shopee_ads.sort_values(by = ["Time","Chi_phi_quang_cao"],ascending=False).reset_index(drop=True)
    data_nhom_sp = data_nhom_sp.sort_values(["Time","Chi_phi_quang_cao"],ascending=False).reset_index(drop = True)



#Save dataframe -> file excel Muti sheet
def save_file():
    with pd.ExcelWriter('/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data_Ads_Tiki_Shopee.xlsx') as writer:  
        data_tiki_ads.to_excel(writer, sheet_name='Sản phẩm Tiki')
        data_nhom_tiki.to_excel(writer, sheet_name='Nhóm sản phẩm Tiki')
        data_shopee_ads.to_excel(writer, sheet_name='Sản phẩm Shopee')
        data_nhom_sp.to_excel(writer, sheet_name='Nhóm sản phẩm Shopee')   
        data_keyword_shopee.to_excel(writer, sheet_name='Keyword_Shopee')
        data_keyword_tiki.to_excel(writer, sheet_name='Keyword_Tiki')


data_tiki_ads = pd.DataFrame()
data_nhom_tiki = pd.DataFrame() 
data_shopee_ads = pd.DataFrame()
data_nhom_sp = pd.DataFrame()  
data_keyword_shopee = pd.DataFrame()
data_keyword_tiki =pd.DataFrame()

def main():
    
    read_file_Shopee("C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Shopee/Total/total.xlsx")
    open_web()
    starts  =["01/05/2022"]#["01/01/2022","01/02/2022","01/03/2022","01/04/2022",
    ends  =["26/05/2022"]#["31/01/2022","28/02/2022","31/03/2022","30/04/2022",
    for start, end in zip(starts, ends):
        download_file_Tiki(start,end)
    read_file_Tiki("C:/Users/ADMIN/OneDrive/Máy tính/Project Crawl Ads Tiki/Data Ads Tiki/")
    save_file()
    driver.close() 

if __name__ == '__main__':
    main()






