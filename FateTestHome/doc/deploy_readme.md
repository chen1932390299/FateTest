# FateTest
…or create a new repository on the command line
echo "# FateTest" >> README.md
git init
git add README.md
git commit -m "first commit"
git remote add origin git@github.com:chen1932390299/FateTest.git
git push -u origin master
                
…or push an existing repository from the command line
git remote add origin git@github.com:chen1932390299/FateTest.git
git push -u origin master

#
###前言：
code build on free hand login between guest and host part with No PassWord.
first sudo su - app  login guest part_ty
detail can see this blog : https://www.cnblogs.com/SunshineKimi/p/10847462.html

#how to run task 
python  run_task.py 

# about logs 
demo_logs dir can auto create by code self,it's contains
INFO,DEBUG,ERROR,WARNING 4 types _prefix.log config by user self.

#about demo_conf explain:
## about data_conf.json
it's for user to config dsl and conf file that submit_job needed,
in the future will add assert func failed distinct should failed and timeout failed lead by  kill  

## about ssh_conf.json 
it's for user to config guest and host ip ,ssh port 
guest_party_id,host_party_id ,set each job MAX_TIMEOUT_SECONDS ,
virtualenv environment variable path of python  

## about files_conf.json 
it's for user config it's upload_guest.json and upload_host.json content
according user's own need to upload data_set.

# about upload_hook.py
it can trigger upload all guest or host config data_set in files_conf.json,you can run standalone in guest or host :
python upload_hook.py -role ${role}

# upload_check.py :
return a Map reflect guest and host all table_info, judge each count,namespace,table_name
condition tip user upload status in console or demo_logs
