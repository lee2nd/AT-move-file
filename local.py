import pandas as pd
import os
import paramiko
import fnmatch
import glob
import shutil
from datetime import date

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

hostname = "10.88.19.29"
port = 22
username = "wma"
password = "wma"

ssh.connect(hostname=hostname, port=port, username=username, password=password)
sftp_client = ssh.open_sftp()

last_modified = 0

for filename in sftp_client.listdir('/app_1/wma/AMF/data/Source_at'):
    if fnmatch.fnmatch(filename, "*.xlsx"):
        temp_modified = sftp_client.stat("/app_1/wma/AMF/data/Source_at/" + filename).st_mtime
        if temp_modified > last_modified:
            last_modified = temp_modified
            last_filename = filename

df = pd.read_excel(sftp_client.open("/app_1/wma/AMF/data/Source_at/" + last_filename))

chip_id_idx_lst = df[df.iloc[:, 0]=="Chip ID"].index
new_folder_dict = {}

for chip_id_idx in chip_id_idx_lst:
    for i in range(20):       
        if isinstance(df.iat[chip_id_idx,i+1], str):
            chip_id_dict = {}
            chip_id_dict["adr_path"] = "/app_1/wma/AMF/data/Source_at/" + str(df.iat[chip_id_idx+5,0].split("\n")[2].split(" Folder")[0]) + "/ADR/" + str(df.iat[chip_id_idx+6,i+1]) + "/" + str(df.iat[chip_id_idx+6,i+2]) + ".Adr"
            chip_id_dict["point"] = df.iat[chip_id_idx+6,i+2]
            chip_id_dict["charge_map_path"] = "/app_1/wma/AMF/data/Source_at/" + str(df.iat[chip_id_idx+5,0].split("\n")[2].split(" Folder")[0]) + "/CHARGE_MAP/" + str(df.iat[chip_id_idx+6,i+1]) + "/"
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

print("start download adr file")
for x, y in new_folder_dict.items():

    filename = y["adr_path"].split("/")[-1]  
    os.makedirs("U:/22. 龍潭/Temp/" + x , exist_ok=True)
    
    try:
        sftp_client.get(y["adr_path"], "U:/22. 龍潭/Temp/" + x + "/" + filename)
        success_a_lst.append(x)
    except:
        continue

success_a_lst = list(set(success_a_lst))

print("start download chaegemap file")
for x, y in new_folder_dict.items():
    
    try:
        if len(sftp_client.listdir(y["charge_map_path"])) > 0:
                        
            directory_list = sftp_client.listdir(y["charge_map_path"])
            filteredList = [y["charge_map_path"] + file for file in directory_list if file.startswith(y["point"])] 
            
            for chargepath in filteredList:
                filename = chargepath.split("/")[-1]

                try:
                    sftp_client.get(y["charge_map_path"]+filename, "U:/22. 龍潭/Temp/" + x + "/" + filename)
                    success_c_lst.append(x)
                except:
                    continue                
    except:
        continue     
    
success_c_lst = list(set(success_c_lst))
success_ac_lst = list(set(success_a_lst) & set(success_c_lst))
success_ac_union_lst = list(set(success_a_lst) | set(success_c_lst))     
fail_lst = list(set(all_lst) - set(success_ac_union_lst)) 
fail_a_lst = list(set(all_lst) - set(success_a_lst))  

print("start removing empty size files")
for adrpath in glob.glob("U:/22. 龍潭/Temp/*/*.Adr"):
    size = os.path.getsize(adrpath)
    if size == 0:
        shutil.rmtree(adrpath.split("\\")[0]+"/"+adrpath.split("\\")[1])

print("start uploading files to target folder")
for path in glob.glob("U:/22. 龍潭/Temp/*/*"):
    remote_dir = "/app_1/wma/AMF/data/Target/SW_AT/"+path.split("\\")[1]
    remote_path = "/app_1/wma/AMF/data/Target/SW_AT/"+path.split("\\")[1]+"/"+path.split("\\")[2]
    try:
        sftp_client.chdir(remote_dir)
        sftp_client.put(path, remote_path) 
    except IOError:
        sftp_client.mkdir(remote_dir)
        sftp_client.chdir(remote_dir)
        sftp_client.put(path, remote_path) 
    
print("start deleting local temp all files")        
shutil.rmtree("U:/22. 龍潭/Temp")

df_report = pd.DataFrame({'adr file missing chip id':fail_a_lst})
today = date.today()
df_report.to_excel("U:/22. 龍潭/adr_file_missing_"+today.strftime("%m_%d")+".xlsx")
sftp_client.put("U:/22. 龍潭/adr_file_missing_"+today.strftime("%m_%d")+".xlsx", "/app_1/wma/AMF/data/Target/adr_file_missing/adr_file_missing_"+today.strftime("%m_%d")+".xlsx") 

sftp_client.close()
ssh.close()
