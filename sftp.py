import pandas as pd
import glob
import os
import shutil
from datetime import date
import schedule  
import time
import logging

def CreateLog(fileName, logPath):
    if not os.path.exists(logPath):
        os.mkdir(logPath)
        logging.basicConfig(
            filename= f'./log/{fileName}', 
            filemode= 'w', 
            format= '%(asctime)s - %(message)s', 
            encoding= 'utf-8'
            )
    else:
        logging.basicConfig(
            filename= f'./log/{fileName}', 
            filemode='a', 
            format='%(asctime)s - %(message)s',
            encoding='utf-8'
            )   
        
def job():

    CreateLog(fileName='running.log', logPath="./log/")
    
    list_of_files = glob.glob("/AMF/Source_at/*.xlsx")
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)
    df = pd.read_excel(latest_file)

    chip_id_idx_lst = df[df.iloc[:, 0]=="Chip ID"].index
    new_folder_dict = {}
    for chip_id_idx in chip_id_idx_lst:
        for i in range(20):       
            if isinstance(df.iat[chip_id_idx,i+1], str):
                chip_id_dict = {}
                chip_id_dict["adr_path"] = "/AMF/Source_at/" + str(df.iat[chip_id_idx+5,0].split("\n")[2].split(" Folder")[0]) + "/ADR/" + str(df.iat[chip_id_idx+6,i+1]) + "/" + str(df.iat[chip_id_idx+6,i+2]) + ".Adr"
                chip_id_dict["point"] = df.iat[chip_id_idx+6,i+2]
                chip_id_dict["charge_map_path"] = "/AMF/Source_at/" + str(df.iat[chip_id_idx+5,0].split("\n")[2].split(" Folder")[0]) + "/CHARGE_MAP/" + str(df.iat[chip_id_idx+6,i+1]) + "/"
                if "\n" in df.iat[chip_id_idx,i+1]:
                    chipid = df.iat[chip_id_idx,i+1].split("\n")[0]
                else:
                    chipid = df.iat[chip_id_idx,i+1]            
                new_folder_dict[chipid] = chip_id_dict

    all_lst = list(new_folder_dict.keys())
    all_lst.remove("Chip ID")
    all_lst.remove("Taco exp.")
    success_ac_lst = []
    success_a_lst = []
    success_c_lst = []
    fail_lst = []   

    print("start downloading adr files")
    for x, y in new_folder_dict.items():
        filename = y["adr_path"].split("/")[-1]    
        os.makedirs("/AMF/Temp/" + x, exist_ok=True)
        try:
            shutil.copy(y["adr_path"], "/AMF/Temp/" + x + "/" + filename)
            success_a_lst.append(x)
        except:
            continue
    success_a_lst = list(set(success_a_lst))    

    print("start downloading chargemap files")
    for x, y in new_folder_dict.items():
        try:
            if len(os.listdir(y["charge_map_path"])) > 0:            
                directory_list = os.listdir(y["charge_map_path"])
                filteredList = [y["charge_map_path"] + file for file in directory_list if file.startswith(y["point"])]            
                for chargepath in filteredList:
                    filename = chargepath.split("/")[-1]
                    shutil.copy(y["charge_map_path"]+filename, "/AMF/Temp/" + x + "/" + filename)                
                success_c_lst.append(x)
        except:
            continue       
    success_c_lst = list(set(success_c_lst))

    success_ac_lst = list(set(success_a_lst) & set(success_c_lst))
    success_ac_union_lst = list(set(success_a_lst) | set(success_c_lst))        
    fail_lst = list(set(all_lst) - set(success_ac_union_lst))
    fail_a_lst = list(set(all_lst) - set(success_a_lst))

    # Source path 
    src = '/AMF/Temp/'
    # Destination path 
    dst = '/AMF/Target/SW_AT/'

    print("start moving files to target folder")
    for path in success_a_lst:
        shutil.copytree(src+path, dst+path, dirs_exist_ok=True)

    print("deleting Temp folder")
    shutil.rmtree("/AMF/Temp/")

    print("generating adr missing report")
    df_report = pd.DataFrame({'adr file missing chip id':fail_a_lst})
    today = date.today()
    df_report.to_excel("/AMF/Target/adr_file_missing_"+today.strftime("%m_%d")+".xlsx")
    
    logging.warning('finished!')

if __name__ == '__main__':
    
    schedule.every().day.at("06:30").do(job)  
    schedule.every().day.at("17:30").do(job)  

    while True:  
        schedule.run_pending()     
